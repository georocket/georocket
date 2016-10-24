package io.georocket.index;

import static io.georocket.util.ThrowableHelper.throwableToCode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple;
import org.yaml.snakeyaml.Yaml;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.elasticsearch.ElasticsearchClient;
import io.georocket.index.elasticsearch.ElasticsearchInstaller;
import io.georocket.index.elasticsearch.ElasticsearchRunner;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.RxStore;
import io.georocket.storage.StoreFactory;
import io.georocket.util.MapUtils;
import io.georocket.util.RxUtils;
import io.vertx.core.Future;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func1;

/**
 * Background indexing of chunks added to the store
 * @author Michel Kraemer
 */
public abstract class IndexerVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(IndexerVerticle.class);
  
  protected static final int MAX_ADD_REQUESTS = 1000;
  protected static final long BUFFER_TIMESPAN = 5000;
  protected static final int MAX_INSERT_REQUESTS = 5;
  protected static final int MAX_RETRIES = 5;
  protected static final int RETRY_INTERVAL = 1000;

  protected static final String INDEX_NAME = "georocket";
  protected static final String TYPE_NAME = "object";
  
  /**
   * Runs Elasticsearch
   */
  protected ElasticsearchRunner runner;
  
  /**
   * The Elasticsearch client
   */
  protected ElasticsearchClient client;
  
  /**
   * The GeoRocket store
   */
  protected RxStore store;
  
  /**
   * A list of {@link IndexerFactory} objects
   */
  protected List<? extends IndexerFactory> indexerFactories;
  
  /**
   * Compiles search strings to Elasticsearch documents
   */
  protected DefaultQueryCompiler queryCompiler;
  
  /**
   * True if {@link #ensureIndex()} has been called at least once
   */
  private boolean indexEnsured;
  
  @Override
  public void start(Future<Void> startFuture) {
    indexerFactories = createIndexerFactories();

    createElasticsearchClient()
      .subscribe(es -> {
        client = es.getKey();
        runner = es.getValue();
        
        queryCompiler = new DefaultQueryCompiler(indexerFactories);
        store = new RxStore(StoreFactory.createStore(getVertx()));
        
        registerMessageConsumers();
        
        startFuture.complete();
      }, err -> {
        startFuture.fail(err);
      });
  }
  
  @Override
  public void stop() {
    client.close();
    if (runner != null) {
      runner.stop();
    }
  }

  /**
   * Create a list of indexer factories
   * @return the list
   */
  protected abstract List<? extends IndexerFactory> createIndexerFactories();

  /**
   * Create a {@link ChunkMeta} object from the given JSON object
   * @param json the JSON object
   * @return the {@link ChunkMeta} object
   */
  protected abstract ChunkMeta makeChunkMeta(JsonObject json);

  /**
   * Convert a chunk to a Elasticsearch document
   * @param chunk the chunk to convert
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk if it does not specify a CRS itself (may be null if no
   * CRS is available as fallback)
   * @return an observable that will emit the document
   */
  protected abstract Observable<Map<String, Object>> chunkToDocument(
      ChunkReadStream chunk, String fallbackCRSString);

  /**
   * Create an Elasticsearch client. Either start an Elasticsearch instance or
   * connect to an external one - depending on the configuration.
   * @return an observable emitting an Elasticsearch client and runner
   */
  private Observable<Pair<ElasticsearchClient, ElasticsearchRunner>> createElasticsearchClient() {
    boolean embedded = config().getBoolean(ConfigConstants.INDEX_ELASTICSEARCH_EMBEDDED, true);
    String host = config().getString(ConfigConstants.INDEX_ELASTICSEARCH_HOST, "localhost");
    int port = config().getInteger(ConfigConstants.INDEX_ELASTICSEARCH_PORT, 9200);
    
    String home = config().getString(ConfigConstants.HOME);
    
    String defaultElasticsearchDownloadUrl;
    try {
      defaultElasticsearchDownloadUrl = IOUtils.toString(getClass().getResource(
          "/elasticsearch_download_url.txt"), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return Observable.error(e);
    }
    
    String defaultElasticsearchVersion;
    try {
      defaultElasticsearchVersion = new File(new URL(defaultElasticsearchDownloadUrl)
          .getPath()).getParentFile().getName();
    } catch (MalformedURLException e) {
      return Observable.error(e);
    }
    
    String elasticsearchDownloadUrl = config().getString(
        ConfigConstants.INDEX_ELASTICSEARCH_DOWNLOAD_URL, defaultElasticsearchDownloadUrl);
    String elasticsearchInstallPath = config().getString(
        ConfigConstants.INDEX_ELASTICSEARCH_INSTALL_PATH,
        home + "/elasticsearch/" + defaultElasticsearchVersion);
    
    ElasticsearchClient client = new ElasticsearchClient(host, port, INDEX_NAME,
        TYPE_NAME, vertx);
    
    if (!embedded) {
      // just return the client
      return Observable.just(Pair.of(client, null));
    }
    
    return client.isRunning().flatMap(running -> {
      if (running) {
        // we don't have to start Elasticsearch again
        return Observable.just(Pair.of(client, null));
      }
      
      // install Elasticsearch, start it and then create the client
      ElasticsearchInstaller installer = new ElasticsearchInstaller(vertx);
      ElasticsearchRunner runner = new ElasticsearchRunner(getVertx());
      return installer.download(elasticsearchDownloadUrl, elasticsearchInstallPath)
        .flatMap(path -> runner.runElasticsearch(host, port, path))
        .flatMap(v -> runner.waitUntilElasticsearchRunning(client))
        .map(v -> Pair.of(client, runner));
    });
  }
  
  /**
   * Register all message consumers for this verticle
   */
  protected void registerMessageConsumers() {
    registerAdd();
    registerDelete();
    registerQuery();
  }

  /**
   * @return a function that can be passed to {@link Observable#retryWhen(Func1)}
   * @see RxUtils#makeRetry(int, int, Scheduler, Logger)
   */
  protected Func1<Observable<? extends Throwable>, Observable<Long>> makeRetry() {
    Scheduler scheduler = RxHelper.scheduler(getVertx());
    return RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, scheduler, log);
  }
  
  /**
   * Register consumer for add messages
   */
  protected void registerAdd() {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD)
      .toObservable()
      .buffer(BUFFER_TIMESPAN, TimeUnit.MILLISECONDS, MAX_ADD_REQUESTS,
          RxHelper.scheduler(getVertx()))
      .onBackpressureBuffer() // unlimited buffer
      .subscribe(new Subscriber<List<Message<JsonObject>>>() {
        private void doRequest() {
          request(MAX_INSERT_REQUESTS);
        }
        
        @Override
        public void onStart() {
          doRequest();
        }

        @Override
        public void onCompleted() {
          // nothing to do here
        }

        @Override
        public void onError(Throwable e) {
          // this bad. it will unsubscribe the consumer from the eventbus!
          // should never happen anyhow, if it does something else has
          // completely gone wrong
          log.fatal("Could not index chunks", e);
        }
        
        @Override
        public void onNext(List<Message<JsonObject>> messages) {
          onAdd(messages)
            .subscribe(v -> {
              // should never happen
            }, err -> {
              log.error("Could not index chunks", err);
              messages.forEach(msg -> msg.fail(throwableToCode(err), err.getMessage()));
              doRequest();
            }, this::doRequest);
        }
      });
  }
  
  /**
   * Register consumer for delete messages
   */
  protected void registerDelete() {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE)
      .toObservable()
      .subscribe(msg -> {
        onDelete(msg.body()).subscribe(v -> {
          msg.reply(v);
        }, err -> {
          log.error("Could not delete chunks", err);
          msg.fail(throwableToCode(err), err.getMessage());
        });
      });
  }
  
  /**
   * Register consumer for queries
   */
  protected void registerQuery() {
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

        // open chunk and create IndexRequest
        return openChunkToDocument(path, fallbackCRSString)
            .doOnNext(doc -> {
              doc.put("importId", importId);
              doc.put("filename", filename);
              doc.put("importTime", importTime);
              addMeta(doc, meta);
              if (tags != null) {
                doc.put("tags", tags);
              }
            })
            .map(doc -> Tuple.tuple(path, doc, msg))
            .onErrorResumeNext(err -> {
              msg.fail(throwableToCode(err), err.getMessage());
              return Observable.empty();
            });
      })
      // create map containing all documents and list containing all messages
      .reduce(Tuple.tuple(new HashMap<String, JsonObject>(), new ArrayList<Message<JsonObject>>()), (i, t) -> {
        i.v1.put(t.v1, new JsonObject(t.v2));
        i.v2.add(t.v3);
        return i;
      })
      .flatMap(t -> ensureIndex().map(v -> t))
      .flatMap(t -> {
        if (!t.v1.isEmpty()) {
          return insertDocuments(t.v1, t.v2);
        }
        return Observable.empty();
      });
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
          return chunkToDocument(chunk, fallbackCRSString)
              .doAfterTerminate(chunk::close);
        }))
      .retryWhen(makeRetry(), RxHelper.scheduler(getVertx()));
  }
  
  /**
   * Performs a query
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
      observable = client.beginScroll(null, postFilter, pageSize, timeout);
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
   * Deletes chunks from the index
   * @param body the message containing the paths to the chunks to delete
   * @return an observable that emits a single item when the chunks have
   * been deleted successfully
   */
  private Observable<Void> onDelete(JsonObject body) {
    JsonArray paths = body.getJsonArray("paths");
    
    // execute bulk request
    long startTimeStamp = System.currentTimeMillis();
    onDeletingStarted(startTimeStamp, paths.size());

    return client.bulkDelete(paths).flatMap(bres -> {
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


  /**
   * Will be called before the indexer starts deleting chunks
   * @param timeStamp the time when the indexer has started deleting
   * @param chunkCount the number of chunks to delete
   */
  protected void onDeletingStarted(long timeStamp, int chunkCount) {
    log.info("Deleting " + chunkCount + " chunks from index ...");
  }

  /**
   * Will be called after the indexer has finished deleting chunks
   * @param duration the time it took to delete the chunks
   * @param chunkCount the number of deleted chunks
   * @param errorMessage an error message if the process has failed
   * or <code>null</code> if everything was successful
   */
  protected void onDeletingFinished(long duration, int chunkCount, String errorMessage) {
    if (errorMessage != null) {
      log.error("Deleting chunks failed: " + errorMessage);
    } else {
      log.info("Finished deleting " + chunkCount +
          " chunks from index in " + duration + " ms");
    }
  }

  /**
   * Add chunk metadata (as received via the event bus) to a ElasticSearch
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
   * Get chunk metadata from ElasticSearch document
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
    return makeChunkMeta(filteredSource);
  }
  
  /**
   * Ensure the Elasticsearch index exists
   * @return an observable that will emit a single item when the index has
   * been created or if it already exists
   */
  private Observable<Void> ensureIndex() {
    if (indexEnsured) {
      return Observable.just(null);
    }
    indexEnsured = true;
    
    // check if index exists
    return client.typeExists().flatMap(exists -> {
      if (exists) {
        return Observable.just(null);
      } else {
        // index does not exist. create it.
        return createIndexForType();
      }
    });
  }
  
  /**
   * Create the Elasticsearch index for the type. Assumes it does not exist yet.
   * @return an observable that will emit a single item when the index
   * has been created
   */
  @SuppressWarnings("unchecked")
  private Observable<Void> createIndexForType() {
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
    
    return client.createIndex(new JsonObject(mappings)).map(r -> null);
  }
  
  /**
   * Inserts multiple Elasticsearch documents to the index. It performs a
   * bulk request. This method replies to all messages if the bulk request
   * was successful.
   * @param documents the documents to insert
   * @param messages a list of messages from which the index requests were
   * created; items are in the same order as the respective index requests in
   * the given bulk request
   * @return an observable that completes when the operation has finished
   */
  private Observable<Void> insertDocuments(Map<String, JsonObject> documents,
      List<Message<JsonObject>> messages) {
    long startTimeStamp = System.currentTimeMillis();
    onIndexingStarted(startTimeStamp, messages.size());

    return client.bulkInsert(documents).<Void>flatMap(bres -> {
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
   * @param timeStamp the time when the indexer has started the process
   * @param chunkCount the number of chunks to index
   */
  protected void onIndexingStarted(long timeStamp, int chunkCount) {
    log.info("Indexing " + chunkCount + " chunks");
  }

  /**
   * Will be called after the indexer has finished the indexing process
   * @param duration the time passed during indexing
   * @param chunkImportIds the import IDs of the chunks that were processed by
   * the indexer. This list may include IDs of chunks whose indexing failed.
   * @param errorMessage an error message if the process has failed
   * or <code>null</code> if everything was successful
   */
  protected void onIndexingFinished(long duration, List<String> chunkImportIds,
      String errorMessage) {
    if (errorMessage != null) {
      log.error("Indexing failed: " + errorMessage);
    } else {
      log.info("Finished indexing " + chunkImportIds.size() + " chunks in " +
          duration + " " + "ms");
    }
  }
}
