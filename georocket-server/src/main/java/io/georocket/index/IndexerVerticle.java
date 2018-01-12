package io.georocket.index;

import static io.georocket.util.MimeTypeUtils.belongsTo;
import static io.georocket.util.ThrowableHelper.throwableToCode;
import static io.georocket.util.ThrowableHelper.throwableToMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;

import com.google.common.collect.ImmutableList;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.elasticsearch.ElasticsearchClient;
import io.georocket.index.elasticsearch.ElasticsearchClientFactory;
import io.georocket.index.generic.DefaultMetaIndexerFactory;
import io.georocket.index.xml.JsonIndexerFactory;
import io.georocket.index.xml.MetaIndexer;
import io.georocket.index.xml.MetaIndexerFactory;
import io.georocket.index.xml.StreamIndexer;
import io.georocket.index.xml.XMLIndexerFactory;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.GeoJsonChunkMeta;
import io.georocket.storage.IndexMeta;
import io.georocket.storage.JsonChunkMeta;
import io.georocket.storage.RxStore;
import io.georocket.storage.StoreFactory;
import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.JsonParserOperator;
import io.georocket.util.MapUtils;
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
  
  private static final long BUFFER_TIMESPAN = 5000;
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
   * A view on {@link #indexerFactories} containing only
   * {@link XMLIndexerFactory} objects
   */
  private List<XMLIndexerFactory> xmlIndexerFactories;

  /**
   * A view on {@link #indexerFactories} containing only
   * {@link JsonIndexerFactory} objects
   */
  private List<JsonIndexerFactory> jsonIndexerFactories;

  /**
   * A view on {@link #indexerFactories} containing only
   * {@link MetaIndexerFactory} objects
   */
  private List<MetaIndexerFactory> metaIndexerFactories;
  
  /**
   * True if the indexer should report activities to the Vert.x event bus
   */
  private boolean reportActivities;
  
  /**
   * The maximum number of chunks to index in one bulk
   */
  private int maxBulkSize;
  
  /**
   * The maximum number of bulk processes to run in parallel. Also affects the
   * number of parallel bulk inserts into Elasticsearch.
   */
  private int maxParallelInserts;
  
  /**
   * The number of add message currently queued due to backpressure
   * (see {@link #onAdd(List)})
   */
  private int queuedAddMessages;
  
  @Override
  public void start(Future<Void> startFuture) {
    // True if the indexer and other verticles should report their activities
    // to the Vert.x event bus (mostly useful for GeoRocket plug-ins)
    reportActivities = config().getBoolean(ConfigConstants.REPORT_ACTIVITIES, false);
    
    maxBulkSize = config().getInteger(ConfigConstants.INDEX_MAX_BULK_SIZE,
        ConfigConstants.DEFAULT_INDEX_MAX_BULK_SIZE);
    maxParallelInserts = config().getInteger(ConfigConstants.INDEX_MAX_PARALLEL_INSERTS,
        ConfigConstants.DEFAULT_INDEX_MAX_PARALLEL_INSERTS);
    
    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    indexerFactories = ImmutableList.copyOf(ServiceLoader.load(IndexerFactory.class));
    xmlIndexerFactories = ImmutableList.copyOf(Seq.seq(indexerFactories)
      .filter(f -> f instanceof XMLIndexerFactory)
      .cast(XMLIndexerFactory.class));
    jsonIndexerFactories = ImmutableList.copyOf(Seq.seq(indexerFactories)
      .filter(f -> f instanceof JsonIndexerFactory)
      .cast(JsonIndexerFactory.class));
    metaIndexerFactories = ImmutableList.copyOf(Seq.seq(indexerFactories)
      .filter(f -> f instanceof MetaIndexerFactory)
      .cast(MetaIndexerFactory.class));
    
    store = new RxStore(StoreFactory.createStore(getVertx()));

    queryCompiler = createQueryCompiler();
    queryCompiler.setQueryCompilers(indexerFactories);
    
    new ElasticsearchClientFactory(vertx).createElasticsearchClient(INDEX_NAME)
      .doOnNext(es -> {
        client = es;
      })
      .flatMap(v -> client.ensureIndex())
      .flatMap(v -> ensureMapping())
      .subscribe(es -> {
        registerMessageConsumers();
        startFuture.complete();
      }, err -> {
        startFuture.fail(err);
      });
  }

  private DefaultQueryCompiler createQueryCompiler() {
    JsonObject config = vertx.getOrCreateContext().config();
    String cls = config.getString(ConfigConstants.QUERY_COMPILER_CLASS, DefaultQueryCompiler.class.getName());
    try {
      return (DefaultQueryCompiler)Class.forName(cls).newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Could not create a DefaultQueryCompiler", e);
    }
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
      .doOnNext(v -> {
        queuedAddMessages++;
      })
      .buffer(BUFFER_TIMESPAN, TimeUnit.MILLISECONDS, maxBulkSize)
      .onBackpressureBuffer() // unlimited buffer
      .flatMap(messages -> {
        queuedAddMessages -= messages.size();
        return onAdd(messages)
          .onErrorReturn(err -> {
            // reply with error to all peers
            log.error("Could not index document", err);
            messages.forEach(msg -> msg.fail(throwableToCode(err), err.getMessage()));
            // ignore error
            return null;
          });
      }, maxParallelInserts)
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
          msg.fail(throwableToCode(err), throwableToMessage(err, ""));
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
          msg.fail(throwableToCode(err), throwableToMessage(err, ""));
        });
      });
  }

  /**
   * Will be called before the indexer starts deleting chunks
   * @param timeStamp the time when the indexer has started deleting
   * @param paths the chunks to delete
   * @param totalChunks the total number of chunks to delete in the whole batch
   * operation
   * @param remainingChunks the remaining number of chunks to delete
   */
  private void onDeletingStarted(long timeStamp, JsonArray paths,
      long totalChunks, long remainingChunks) {
    if (paths.size() < remainingChunks) {
      log.info("Deleting " + paths.size() + "/" + remainingChunks +
          " chunks from index ...");
    } else {
      log.info("Deleting " + paths.size() + " chunks from index ...");
    }

    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "delete")
        .put("state", "index")
        .put("owner", deploymentID())
        .put("action", "enter")
        .put("chunkCount", paths.size())
        .put("totalChunkCount", totalChunks)
        .put("remainingChunkCount", remainingChunks)
        .put("paths", paths)
        .put("timestamp", timeStamp);
      vertx.eventBus().publish(AddressConstants.ACTIVITIES, msg);
    }
  }

  /**
   * Will be called after the indexer has finished deleting chunks
   * @param duration the time it took to delete the chunks
   * @param paths the paths of the deleted chunks
   * @param totalChunks the total number of chunks to delete in the whole batch
   * operation
   * @param remainingChunks the remaining number of chunks to delete
   * @param errorMessage an error message if the process has failed
   * or <code>null</code> if everything was successful
   */
  private void onDeletingFinished(long duration, JsonArray paths,
      long totalChunks, long remainingChunks, String errorMessage) {
    if (errorMessage != null) {
      log.error("Deleting chunks failed: " + errorMessage);
    } else {
      log.info("Finished deleting " + paths.size() +
          " chunks from index in " + duration + " ms");
    }

    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "delete")
        .put("state", "index")
        .put("owner", deploymentID())
        .put("action", "leave")
        .put("chunkCount", paths.size())
        .put("totalChunkCount", totalChunks)
        .put("remainingChunkCount", remainingChunks)
        .put("paths", paths)
        .put("duration", duration);

      if (errorMessage != null) {
        msg.put("error", errorMessage);
      }

      vertx.eventBus().publish(AddressConstants.ACTIVITIES, msg);
    }
  }
  
  private Observable<Void> ensureMapping() {
    // merge mappings from all indexers
    Map<String, Object> mappings = new HashMap<>();
    indexerFactories.stream().filter(f -> f instanceof DefaultMetaIndexerFactory)
        .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));
    indexerFactories.stream().filter(f -> !(f instanceof DefaultMetaIndexerFactory))
        .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));

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
    
    List<String> chunkPaths = Seq.seq(documents)
      .map(Tuple3::v1)
      .toList();
    onIndexingStarted(startTimeStamp, chunkPaths);

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
      List<String> correlationIds = Seq.seq(messages)
        .map(Message::body)
        .map(d -> d.getString("correlationId"))
        .toList();
      onIndexingFinished(stopTimeStamp - startTimeStamp, correlationIds,
          chunkPaths, client.bulkResponseGetErrorMessage(bres));

      return Observable.empty();
    });
  }

  /**
   * Will be called before the indexer starts the indexing process
   * @param timestamp the time when the indexer has started the process
   * @param chunkIds A list of chunkIds
   */
  private void onIndexingStarted(long timestamp, List<String> chunkIds) {
    if (queuedAddMessages > 0) {
      int total = chunkIds.size() + queuedAddMessages;
      log.info("Indexing " + chunkIds.size() + "/" + total + " chunks");
    } else {
      log.info("Indexing " + chunkIds.size() + " chunks");
    }
    
    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "import")
        .put("state", "index")
        .put("owner", deploymentID())
        .put("action", "enter")
        .put("chunkIds", new JsonArray(chunkIds))
        .put("timestamp", timestamp);
      vertx.eventBus().publish(AddressConstants.ACTIVITIES, msg);
    }
  }

  /**
   * Will be called after the indexer has finished the indexing process
   * @param duration the time passed during indexing
   * @param correlationIds the correlation IDs of the chunks that were processed by
   * the indexer. This list may include IDs of chunks whose indexing failed.
   * @param chunkIds A list of chunkIds
   * @param errorMessage an error message if the process has failed
   * or <code>null</code> if everything was successful
   */
  private void onIndexingFinished(long duration, List<String> correlationIds,
      List<String> chunkIds, String errorMessage) {
    if (errorMessage != null) {
      log.error("Indexing failed: " + errorMessage);
    } else {
      log.info("Finished indexing " + correlationIds.size() + " chunks in " +
          duration + " " + "ms");
    }
    
    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "import")
        .put("state", "index")
        .put("owner", deploymentID())
        .put("action", "leave")
        .put("correlationIds", new JsonArray(correlationIds))
        .put("chunkIds", new JsonArray(chunkIds))
        .put("duration", duration);

      if (errorMessage != null) {
        msg.put("error", errorMessage);
      }

      vertx.eventBus().publish(AddressConstants.ACTIVITIES, msg);
    }
  }
  
  /**
   * Open a chunk and convert it to an Elasticsearch document. Retry operation
   * several times before failing.
   * @param path the path to the chunk to open
   * @param chunkMeta metadata about the chunk
   * @param indexMeta metadata used to index the chunk
   * @return an observable that emits the document
   */
  private Observable<Map<String, Object>> openChunkToDocument(
      String path, ChunkMeta chunkMeta, IndexMeta indexMeta) {
    return Observable.defer(() -> store.rxGetOne(path)
      .flatMapObservable(chunk -> {
        List<? extends IndexerFactory> factories;
        Operator<? extends StreamEvent, Buffer> parserOperator;
        
        // select indexers and parser depending on the mime type
        String mimeType = chunkMeta.getMimeType();
        if (belongsTo(mimeType, "application", "xml") ||
          belongsTo(mimeType, "text", "xml")) {
          factories = xmlIndexerFactories;
          parserOperator = new XMLParserOperator();
        } else if (belongsTo(mimeType, "application", "json")) {
          factories = jsonIndexerFactories;
          parserOperator = new JsonParserOperator();
        } else {
          return Observable.error(new NoStackTraceThrowable(String.format(
              "Unexpected mime type '%s' while trying to index "
              + "chunk '%s'", mimeType, path)));
        }
        
        // call meta indexers
        Map<String, Object> metaResults = new HashMap<>();
        for (MetaIndexerFactory metaIndexerFactory : metaIndexerFactories) {
          MetaIndexer metaIndexer = metaIndexerFactory.createIndexer();
          metaIndexer.onIndexChunk(path, chunkMeta, indexMeta);
          metaResults.putAll(metaIndexer.getResult());
        }

        // convert chunk to document and close it
        return chunkToDocument(chunk, indexMeta.getFallbackCRSString(),
            parserOperator, factories)
          .doAfterTerminate(chunk::close)
          // add results from meta indexers to converted document
          .doOnNext(doc -> doc.putAll(metaResults));
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
      List<? extends IndexerFactory> indexerFactories) {
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
   * Convert a {@link JsonObject} to a {@link ChunkMeta} object
   * @param source the JSON object to convert
   * @return the converted object
   */
  private ChunkMeta getMeta(JsonObject source) {
    String mimeType = source.getString("mimeType", XMLChunkMeta.MIME_TYPE);
    if (belongsTo(mimeType, "application", "xml") ||
      belongsTo(mimeType, "text", "xml")) {
      return new XMLChunkMeta(source);
    } else if (belongsTo(mimeType, "application", "geo+json")) {
      return new GeoJsonChunkMeta(source);
    } else if (belongsTo(mimeType, "application", "json")) {
      return new JsonChunkMeta(source);
    } else {
      return new ChunkMeta(source);
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

        // get properties
        JsonObject propertiesObj = body.getJsonObject("properties");
        Map<String, Object> properties = propertiesObj != null ? propertiesObj.getMap() : null;

        // get fallback CRS
        String fallbackCRSString = body.getString("fallbackCRSString");

        log.trace("Indexing " + path);

        String correlationId = body.getString("correlationId");
        String filename = body.getString("filename");
        long timestamp = body.getLong("timestamp", System.currentTimeMillis());

        ChunkMeta chunkMeta = getMeta(meta);
        IndexMeta indexMeta = new IndexMeta(correlationId, filename, timestamp,
            tags, properties, fallbackCRSString);

        // open chunk and create IndexRequest
        return openChunkToDocument(path, chunkMeta, indexMeta)
          .map(doc -> Tuple.tuple(path, new JsonObject(doc), msg))
          .onErrorResumeNext(err -> {
            msg.fail(throwableToCode(err), throwableToMessage(err, ""));
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
    int pageSize = body.getInteger("size", 100);
    String timeout = "1m"; // one minute
    
    JsonObject parameters = new JsonObject()
      .put("size", pageSize);

    Observable<JsonObject> observable;
    if (scrollId == null) {
      // Execute a new search. Use a post_filter because we only want to get
      // a yes/no answer and no scoring (i.e. we only want to get matching
      // documents and not those that likely match). For the difference between
      // query and post_filter see the Elasticsearch documentation.
      JsonObject postFilter;
      try {
        postFilter = queryCompiler.compileQuery(search, path);
      } catch (Throwable t) {
        return Observable.error(t);
      }
      observable = client.beginScroll(TYPE_NAME, null, postFilter, parameters, timeout);
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
        JsonObject jsonMeta = source.getJsonObject("chunkMeta");
        ChunkMeta meta = getMeta(jsonMeta);
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
    long totalChunks = body.getLong("totalChunks", (long)paths.size());
    long remainingChunks = body.getLong("remainingChunks", (long)paths.size());

    // execute bulk request
    long startTimeStamp = System.currentTimeMillis();
    onDeletingStarted(startTimeStamp, paths, totalChunks, remainingChunks);

    return client.bulkDelete(TYPE_NAME, paths).flatMap(bres -> {
      long stopTimeStamp = System.currentTimeMillis();
      if (client.bulkResponseHasErrors(bres)) {
        String error = client.bulkResponseGetErrorMessage(bres);
        log.error("One or more chunks could not be deleted");
        log.error(error);
        onDeletingFinished(stopTimeStamp - startTimeStamp, paths, totalChunks,
            remainingChunks, error);
        return Observable.error(new NoStackTraceThrowable(
                "One or more chunks could not be deleted"));
      } else {
        onDeletingFinished(stopTimeStamp - startTimeStamp, paths, totalChunks,
            remainingChunks, null);
        return Observable.just(null);
      }
    });
  }
}
