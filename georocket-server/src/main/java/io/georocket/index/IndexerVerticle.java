package io.georocket.index;

import static io.georocket.util.ThrowableHelper.throwableToCode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.ImmutableList;

import io.georocket.constants.AddressConstants;
import io.georocket.index.elasticsearch.ElasticsearchClient;
import io.georocket.index.elasticsearch.ElasticsearchClientFactory;
import io.georocket.index.xml.StreamIndexer;
import io.georocket.index.xml.XMLIndexerFactory;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.RxStore;
import io.georocket.storage.StoreFactory;
import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.MapUtils;
import io.georocket.util.MimeTypeUtils;
import io.georocket.util.RxUtils;
import io.georocket.util.StreamEvent;
import io.georocket.util.XMLParserOperator;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import rx.Observable;
import rx.Observable.Operator;
import rx.functions.Func1;

/**
 * Generic methods for background indexing of any messages
 * @author Michel Kraemer
 */
public class IndexerVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(IndexerVerticle.class);
  
  private static final int MAX_ADD_REQUESTS = 200;
  private static final long BUFFER_TIMESPAN = 5000;
  private static final int MAX_INSERT_REQUESTS = 5;
  private static final int MAX_RETRIES = 5;
  private static final int RETRY_INTERVAL = 1000;

  /**
   * Elasticsearch index
   */
  private static final String INDEX_NAME = "georocket";
  
  /**
   * Type of documents stored in the Elasticsearch index
   */
  private static final String TYPE_NAME = "object";

  /**
   * The Elasticsearch client
   */
  private ElasticsearchClient client;
  
  /**
   * The GeoRocket store
   */
  private RxStore store;

  /**
   * Compiles search strings to Elasticsearch documents
   */
  private DefaultQueryCompiler queryCompiler;
  
  /**
   * A list of {@link IndexerFactory} objects
   */
  private List<? extends IndexerFactory> indexerFactories;
  
  /**
   * True if the indexer should report activities to the Vert.x event bus
   */
  private boolean reportActivities;
  
  @Override
  public void start(Future<Void> startFuture) {
    reportActivities = config().getBoolean("georocket.reportActivities", false);
    
    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    indexerFactories = ImmutableList.copyOf(ServiceLoader.load(IndexerFactory.class));
    
    store = new RxStore(StoreFactory.createStore(getVertx()));
    
    new ElasticsearchClientFactory(vertx).createElasticsearchClient(INDEX_NAME)
      .doOnNext(es -> {
        client = es;
      })
      .flatMap(v -> client.ensureIndex())
      .flatMap(v -> ensureMapping())
      .subscribe(es -> {

        queryCompiler = new DefaultQueryCompiler(indexerFactories);

        registerMessageConsumers();

        startFuture.complete();
      }, err -> {
        startFuture.fail(err);
      });
  }
  
  @Override
  public void stop() {
    client.close();
  }

  /**
   * Register all message consumers for this verticle
   */
  private void registerMessageConsumers() {
    registerAdd();
    registerDelete();
    registerQuery();
  }

  /**
   * @return a function that can be passed to {@link Observable#retryWhen(Func1)}
   * @see RxUtils#makeRetry(int, int, Logger)
   */
  private Func1<Observable<? extends Throwable>, Observable<Long>> makeRetry() {
    return RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log);
  }
  
  /**
   * Register consumer for add messages
   */
  private void registerAdd() {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD)
      .toObservable()
      .buffer(BUFFER_TIMESPAN, TimeUnit.MILLISECONDS, MAX_ADD_REQUESTS)
      .onBackpressureBuffer() // unlimited buffer
      .flatMap(messages -> {
        return onAdd(messages)
          .onErrorReturn(err -> {
            // reply with error to all peers
            log.error("Could not index document", err);
            messages.forEach(msg -> msg.fail(throwableToCode(err), err.getMessage()));
            // ignore error
            return null;
          });
      }, MAX_INSERT_REQUESTS)
      .subscribe(v -> {
        // ignore
      }, err -> {
        // This is bad. It will unsubscribe the consumer from the eventbus!
        // Should never happen anyhow. If it does, something else has
        // completely gone wrong.
        log.fatal("Could not index document", err);
      });
  }
  
  /**
   * Register consumer for delete messages
   */
  private void registerDelete() {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE)
      .toObservable()
      .subscribe(msg -> {
        onDelete(msg.body()).subscribe(v -> {
          msg.reply(v);
        }, err -> {
          log.error("Could not delete document", err);
          msg.fail(throwableToCode(err), err.getMessage());
        });
      });
  }
  
  /**
   * Register consumer for queries
   */
  private void registerQuery() {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY)
      .toObservable()
      .subscribe(msg -> {
        onQuery(msg.body()).subscribe(reply -> {
          msg.reply(reply);
        }, err -> {
          log.error("Could not perform query", err);
          msg.fail(throwableToCode(err), err.getMessage());
        });
      });
  }

  /**
   * Will be called before the indexer starts deleting chunks
   * @param timeStamp the time when the indexer has started deleting
   * @param count the number of chunks to delete
   */
  private void onDeletingStarted(long timeStamp, int count) {
    log.info("Deleting " + count + " chunks from index ...");

    if (reportActivities) {
      JsonObject msg = new JsonObject()
          .put("activity", "deleting-index")
          .put("owner", deploymentID())
          .put("action", "start")
          .put("timestamp", timeStamp);
      vertx.eventBus().send(AddressConstants.ACTIVITIES, msg);
    }
  }

  /**
   * Will be called after the indexer has finished deleting chunks
   * @param duration the time it took to delete the chunks
   * @param count the number of deleted chunks
   * @param errorMessage an error message if the process has failed
   * or <code>null</code> if everything was successful
   */
  private void onDeletingFinished(long duration, int count, String errorMessage) {
    if (errorMessage != null) {
      log.error("Deleting chunks failed: " + errorMessage);
    } else {
      log.info("Finished deleting " + count +
          " chunks from index in " + duration + " ms");
    }

    if (reportActivities) {
      JsonObject msg = new JsonObject()
          .put("activity", "deleting-index")
          .put("owner", deploymentID())
          .put("action", "stop")
          .put("chunkCount", count)
          .put("duration", duration);

      if (errorMessage != null) {
        msg.put("error", errorMessage);
      }

      vertx.eventBus().send(AddressConstants.ACTIVITIES, msg);
    }
  }
  
  @SuppressWarnings("unchecked")
  private Observable<Void> ensureMapping() {
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
  
  /**
   * Insert multiple Elasticsearch documents into the index. Perform a
   * bulk request. This method replies to all messages if the bulk request
   * was successful.
   * @param type Elasticsearch type for documents
   * @param documents a list of tuples containing document IDs, documents to
   * index, and the respective messages from which the documents were created
   * @return an observable that completes when the operation has finished
   */
  private Observable<Void> insertDocuments(String type,
      List<Tuple3<String, JsonObject, Message<JsonObject>>> documents) {
    long startTimeStamp = System.currentTimeMillis();
    onIndexingStarted(startTimeStamp, documents.size());

    List<Tuple2<String, JsonObject>> docsToInsert = Seq.seq(documents)
      .map(Tuple3::limit2)
      .toList();
    List<Message<JsonObject>> messages = Seq.seq(documents)
      .map(Tuple3::v3)
      .toList();
    
    return client.bulkInsert(type, docsToInsert).flatMap(bres -> {
      JsonArray items = bres.getJsonArray("items");
      for (int i = 0; i < items.size(); ++i) {
        JsonObject jo = items.getJsonObject(i);
        JsonObject item = jo.getJsonObject("index");
        Message<JsonObject> msg = messages.get(i);
        if (client.bulkResponseItemHasErrors(item)) {
          msg.fail(500, client.bulkResponseItemGetErrorMessage(item));
        } else {
          msg.reply(null);
        }
      }
      
      long stopTimeStamp = System.currentTimeMillis();
      List<String> importIds = Seq.seq(messages)
        .map(Message::body)
        .map(d -> d.getString("importId"))
        .toList();
      onIndexingFinished(stopTimeStamp - startTimeStamp, importIds,
          client.bulkResponseGetErrorMessage(bres));

      return Observable.empty();
    });
  }

  /**
   * Will be called before the indexer starts the indexing process
   * @param timestamp the time when the indexer has started the process
   * @param count the number of chunks to index
   */
  private void onIndexingStarted(long timestamp, int count) {
    log.info("Indexing " + count + " chunks");
    
    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "indexing")
        .put("owner", deploymentID())
        .put("action", "start")
        .put("timestamp", timestamp);
      vertx.eventBus().send(AddressConstants.ACTIVITIES, msg);
    }
  }

  /**
   * Will be called after the indexer has finished the indexing process
   * @param duration the time passed during indexing
   * @param importIds the import IDs of the chunks that were processed by
   * the indexer. This list may include IDs of chunks whose indexing failed.
   * @param errorMessage an error message if the process has failed
   * or <code>null</code> if everything was successful
   */
  private void onIndexingFinished(long duration, List<String> importIds,
      String errorMessage) {
    if (errorMessage != null) {
      log.error("Indexing failed: " + errorMessage);
    } else {
      log.info("Finished indexing " + importIds.size() + " chunks in " +
          duration + " " + "ms");
    }
    
    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "indexing")
        .put("owner", deploymentID())
        .put("action", "stop")
        .put("importIds", new JsonArray(importIds))
        .put("duration", duration);
      vertx.eventBus().send(AddressConstants.ACTIVITIES, msg);
    }
  }
  
  /**
   * Open a chunk and convert it to an Elasticsearch document. Retry operation
   * several times before failing.
   * @param path the path to the chunk to open
   * @param mimeType the chunk's mime type
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk if it does not specify a CRS itself (may be null if no
   * CRS is available as fallback)
   * @return an observable that emits the document
   */
  private Observable<Map<String, Object>> openChunkToDocument(String path,
      String mimeType, String fallbackCRSString) {
    return Observable.defer(() -> store.getOneObservable(path)
      .flatMap(chunk -> {
        // convert chunk to document and close it
        Seq<? extends IndexerFactory> filteredFactories = Seq.seq(indexerFactories)
          .filter(f -> f instanceof XMLIndexerFactory);
        return chunkToDocument(chunk, fallbackCRSString, new XMLParserOperator(),
          filteredFactories).doAfterTerminate(chunk::close);
      }))
      .retryWhen(makeRetry());
  }
  
  /**
   * Convert a chunk to a Elasticsearch document
   * @param chunk the chunk to convert
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk if it does not specify a CRS itself (may be null if no
   * CRS is available as fallback)
   * @param parserOperator the operator used to parse the chunk stream into
   * stream events
   * @param indexerFactories a sequence of indexer factories that should be
   * used to index the chunk
   * @param <T> the type of the stream events created by <code>parserOperator</code>
   * @return an observable that will emit the document
   */
  private <T extends StreamEvent> Observable<Map<String, Object>> chunkToDocument(
      ChunkReadStream chunk, String fallbackCRSString,
      Operator<T, Buffer> parserOperator,
      Seq<? extends IndexerFactory> indexerFactories) {
    List<StreamIndexer<T>> indexers = new ArrayList<>();
    indexerFactories.forEach(factory -> {
      @SuppressWarnings("unchecked")
      StreamIndexer<T> i = (StreamIndexer<T>)factory.createIndexer();
      if (fallbackCRSString != null && i instanceof CRSAware) {
        ((CRSAware)i).setFallbackCRSString(fallbackCRSString);
      }
      indexers.add(i);
    });
    
    return RxHelper.toObservable(chunk)
      .lift(parserOperator)
      .doOnNext(e -> indexers.forEach(i -> i.onEvent(e)))
      .last() // "wait" until the whole chunk has been consumed
      .map(e -> {
        // create the Elasticsearch document
        Map<String, Object> doc = new HashMap<>();
        indexers.forEach(i -> doc.putAll(i.getResult()));
        return doc;
      });
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
  private void addMeta(Map<String, Object> doc, JsonObject meta) {
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
  private ChunkMeta getMeta(JsonObject source) {
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
      return new XMLChunkMeta(filteredSource);
    } else {
      return new ChunkMeta(filteredSource);
    }
  }

  /**
   * Will be called when chunks should be added to the index
   * @param messages the list of add messages that contain the paths to
   * the chunks to be indexed
   * @return an observable that completes when the operation has finished
   */
  private Observable<Void> onAdd(List<Message<JsonObject>> messages) {
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

        String mimeType = meta.getString("mimeType", XMLChunkMeta.MIME_TYPE);

        // open chunk and create IndexRequest
        return openChunkToDocument(path, mimeType, fallbackCRSString)
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
      .flatMap(l -> {
        if (!l.isEmpty()) {
          return insertDocuments(TYPE_NAME, l);
        }
        return Observable.empty();
      });
  }

  /**
   * Write result of a query given the Elasticsearch response
   * @param body the message containing the query
   * @return an observable that emits the results of the query
   */
  private Observable<JsonObject> onQuery(JsonObject body) {
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

  /**
   * Delete chunks from the index
   * @param body the message containing the paths to the chunks to delete
   * @return an observable that emits a single item when the chunks have
   * been deleted successfully
   */
  private Observable<Void> onDelete(JsonObject body) {
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
}
