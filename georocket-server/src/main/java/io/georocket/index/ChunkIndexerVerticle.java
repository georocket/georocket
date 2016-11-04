package io.georocket.index;

import static io.georocket.util.ThrowableHelper.throwableToCode;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.jooq.lambda.tuple.Tuple;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.ImmutableList;

import io.georocket.constants.AddressConstants;
import io.georocket.index.xml.XMLChunkMapper;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.RxStore;
import io.georocket.storage.StoreFactory;
import io.georocket.util.MapUtils;
import io.georocket.util.MimeTypeUtils;
import io.vertx.core.Future;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.eventbus.Message;
import rx.Observable;

/**
 * Background indexing of chunks added to the store
 * @author Benedikt Hiemenz
 * @author Michel Kraemer
 */
public class ChunkIndexerVerticle extends IndexerVerticle {
  private static Logger log = LoggerFactory.getLogger(ChunkIndexerVerticle.class);

  /**
   * Type of Elasticsearch
   */
  protected static final String TYPE_NAME = "object";

  /**
   * The GeoRocket store
   */
  protected RxStore store;

  /**
   * A list of {@link IndexerFactory} objects
   */
  protected List<? extends IndexerFactory> indexerFactories;
  
  /**
   * Maps XML chunks to Elasticsearch documents and vice-versa
   */
  protected ChunkMapper xmlChunkMapper;

  /**
   * Constructor. Set addresses according to {@link AddressConstants}
   */
  public ChunkIndexerVerticle() {
    super(AddressConstants.INDEXER_ADD, AddressConstants.INDEXER_QUERY, AddressConstants.INDEXER_DELETE);
  }

  @Override
  public void start(Future<Void> startFuture) {
    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    indexerFactories = ImmutableList.copyOf(ServiceLoader.load(IndexerFactory.class));
    
    xmlChunkMapper = new XMLChunkMapper(indexerFactories);
    
    store = new RxStore(StoreFactory.createStore(getVertx()));
    super.start(startFuture);
  }

  /**
   * Open a chunk and convert to to an Elasticsearch document. Retry
   * operation several times before failing.
   * @param path the path to the chunk to open
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk if it does not specify a CRS itself (may be null if no
   * CRS is available as fallback)
   * @return an observable that emits the document
   */
  protected Observable<Map<String, Object>> openChunkToDocument(String path,
      String fallbackCRSString) {
    return Observable.defer(() -> store.getOneObservable(path)
      .flatMap(chunk -> {
        // convert chunk to document and close it
        return xmlChunkMapper.chunkToDocument(chunk, fallbackCRSString)
          .doAfterTerminate(chunk::close);
      }))
      .retryWhen(makeRetry());
  }

  /**
   * Add chunk metadata (as received via the event bus) to a Elasticsearch
   * document. Insert all properties from the given JsonObject into the
   * document but prepend the string "chunk" to all property names and
   * convert the first character to upper case (or insert an underscore
   * character if it already is upper case)
   * @param doc the document
   * @param meta the metadata to add to the document
   */
  protected void addMeta(Map<String, Object> doc, JsonObject meta) {
    for (String fieldName : meta.fieldNames()) {
      String newFieldName = fieldName;
      if (Character.isTitleCase(newFieldName.charAt(0))) {
        newFieldName = "_" + newFieldName;
      } else {
        newFieldName = StringUtils.capitalize(newFieldName);
      }
      newFieldName = "chunk" + newFieldName;
      doc.put(newFieldName, meta.getValue(fieldName));
    }
  }

  /**
   * Get chunk metadata from Elasticsearch document
   * @param source the document
   * @return the metadata
   */
  protected ChunkMeta getMeta(JsonObject source) {
    JsonObject filteredSource = new JsonObject();
    for (String fieldName : source.fieldNames()) {
      if (fieldName.startsWith("chunk")) {
        String newFieldName = fieldName.substring(5);
        if (newFieldName.charAt(0) == '_') {
          newFieldName = newFieldName.substring(1);
        } else {
          newFieldName = StringUtils.uncapitalize(newFieldName);
        }
        filteredSource.put(newFieldName, source.getValue(fieldName));
      }
    }
    
    String mimeType = filteredSource.getString("mimeType", "application/xml");
    if (MimeTypeUtils.belongsTo(mimeType, "application", "xml") ||
      MimeTypeUtils.belongsTo(mimeType, "text", "xml")) {
      return xmlChunkMapper.makeChunkMeta(filteredSource);
    }
    
    // TODO should actually never happen, but we should return a proper
    // error anyhow instead of throwing an exception
    throw new RuntimeException("Could not convert document to chunk. "
      + "Unknown mime type: " + mimeType);
  }

  @Override
  protected Observable<Void> onAdd(List<Message<JsonObject>> messages) {
    return Observable.from(messages)
      .flatMap(msg -> {
        // get path to chunk from message
        JsonObject body = msg.body();
        String path = body.getString("path");
        if (path == null) {
          msg.fail(400, "Missing path to the chunk to index");
          return Observable.empty();
        }

        // get chunk metadata
        JsonObject meta = body.getJsonObject("meta");
        if (meta == null) {
          msg.fail(400, "Missing metadata for chunk " + path);
          return Observable.empty();
        }

        // get tags
        JsonArray tagsArr = body.getJsonArray("tags");
        List<String> tags = tagsArr != null ? tagsArr.stream().flatMap(o -> o != null ?
                Stream.of(o.toString()) : Stream.of()).collect(Collectors.toList()) : null;

        // get fallback CRS
        String fallbackCRSString = body.getString("fallbackCRSString");

        log.trace("Indexing " + path);

        String importId = body.getString("importId");
        String filename = body.getString("filename");
        Long importTime = body.getLong("importTime");

        // open chunk and create IndexRequest
        return openChunkToDocument(path, fallbackCRSString)
          .doOnNext(doc -> {
            doc.put("path", path);
            doc.put("importId", importId);
            doc.put("filename", filename);
            doc.put("importTime", importTime);
            addMeta(doc, meta);
            if (tags != null) {
              doc.put("tags", tags);
            }
          })
          .map(doc -> Tuple.tuple(path, new JsonObject(doc), msg))
          .onErrorResumeNext(err -> {
            msg.fail(throwableToCode(err), err.getMessage());
            return Observable.empty();
          });
      })
      .toList()
      .flatMap(l -> ensureIndex().map(v -> l))
      .flatMap(l -> ensureMapping(TYPE_NAME).map(v -> l))
      .flatMap(l -> {
        if (!l.isEmpty()) {
          return insertDocuments(TYPE_NAME, l);
        }
        return Observable.empty();
      });
  }

  @Override
  protected Observable<JsonObject> onQuery(JsonObject body) {
    String search = body.getString("search");
    String path = body.getString("path");
    String scrollId = body.getString("scrollId");
    int pageSize = body.getInteger("pageSize", 100);
    String timeout = "1m"; // one minute

    Observable<JsonObject> observable;
    if (scrollId == null) {
      // Execute a new search. Use a post_filter because we only want to get
      // a yes/no answer and no scoring (i.e. we only want to get matching
      // documents and not those that likely match). For the difference between
      // query and post_filter see the Elasticsearch documentation.
      JsonObject postFilter = queryCompiler.compileQuery(search, path);
      observable = client.beginScroll(TYPE_NAME, null, postFilter, pageSize, timeout);
    } else {
      // continue searching
      observable = client.continueScroll(scrollId, timeout);
    }

    return observable.map(sr -> {
      // iterate through all hits and convert them to JSON
      JsonObject hits = sr.getJsonObject("hits");
      long totalHits = hits.getLong("total");
      JsonArray resultHits = new JsonArray();
      JsonArray hitsHits = hits.getJsonArray("hits");
      for (Object o : hitsHits) {
        JsonObject hit = (JsonObject)o;
        String id = hit.getString("_id");
        JsonObject source = hit.getJsonObject("_source");
        ChunkMeta meta = getMeta(source);
        JsonObject obj = meta.toJsonObject()
          .put("id", id);
        resultHits.add(obj);
      }

      // create result and send it to the client
      return new JsonObject()
        .put("totalHits", totalHits)
        .put("hits", resultHits)
        .put("scrollId", sr.getString("_scroll_id"));
    });
  }

  @Override
  protected Observable<Void> onDelete(JsonObject body) {
    JsonArray paths = body.getJsonArray("paths");

    // execute bulk request
    long startTimeStamp = System.currentTimeMillis();
    onDeletingStarted(startTimeStamp, paths.size());

    return client.bulkDelete(TYPE_NAME, paths).flatMap(bres -> {
      long stopTimeStamp = System.currentTimeMillis();
      if (client.bulkResponseHasErrors(bres)) {
        String error = client.bulkResponseGetErrorMessage(bres);
        log.error("One or more chunks could not be deleted");
        log.error(error);
        onDeletingFinished(stopTimeStamp - startTimeStamp, paths.size(), error);
        return Observable.error(new NoStackTraceThrowable(
                "One or more chunks could not be deleted"));
      } else {
        onDeletingFinished(stopTimeStamp - startTimeStamp, paths.size(), null);
        return Observable.just(null);
      }
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Observable<Void> createMapping() {
    // load default mapping
    Yaml yaml = new Yaml();
    Map<String, Object> mappings;
    try (InputStream is = this.getClass().getResourceAsStream("index_defaults.yaml")) {
      mappings = (Map<String, Object>)yaml.load(is);
    } catch (IOException e) {
      return Observable.error(e);
    }

    // remove unnecessary node
    mappings.remove("variables");

    // merge all properties from indexers
    indexerFactories.forEach(factory ->
            MapUtils.deepMerge(mappings, factory.getMapping()));

    return client.putMapping(TYPE_NAME, new JsonObject(mappings)).map(r -> null);
  }

  @Override
  protected DefaultQueryCompiler createQueryCompiler() {
    return new DefaultQueryCompiler(indexerFactories);
  }
}
