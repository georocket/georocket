package io.georocket.index;

import static io.georocket.util.ThrowableHelper.throwableToCode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.ShutdownHookProcessDestroyer;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SystemUtils;
import org.jooq.lambda.tuple.Tuple;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.ImmutableList;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.xml.XMLIndexer;
import io.georocket.index.xml.XMLIndexerFactory;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.Store;
import io.georocket.storage.StoreFactory;
import io.georocket.util.AsyncXMLParser;
import io.georocket.util.MapUtils;
import io.georocket.util.RxUtils;
import io.georocket.util.XMLStartElement;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
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
public class IndexerVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(IndexerVerticle.class);
  
  protected static final int MAX_ADD_REQUESTS = 1000;
  protected static final long BUFFER_TIMESPAN = 5000;
  protected static final int MAX_INSERT_REQUESTS = 5;
  protected static final int MAX_RETRIES = 5;
  protected static final int RETRY_INTERVAL = 1000;

  protected static final String INDEX_NAME = "georocket";
  protected static final String TYPE_NAME = "object";
  
  /**
   * The Elasticsearch client
   */
  protected ElasticsearchClient client;
  
  /**
   * The GeoRocket store
   */
  protected Store store;
  
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
    log.info("Launching indexer ...");
    
    indexerFactories = createIndexerFactories();

    createElasticsearchClient()
      .subscribe(esclient -> {
        client = esclient;
        
        queryCompiler = new DefaultQueryCompiler(indexerFactories);
        store = StoreFactory.createStore((Vertx)vertx.getDelegate());
        
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
   * Create a list of indexer factories
   * @return the list
   */
  protected List<? extends IndexerFactory> createIndexerFactories() {
    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    return ImmutableList.copyOf(ServiceLoader.load(XMLIndexerFactory.class));
  }

  /**
   * Create an Elasticsearch client. Either start an Elasticsearch instance or
   * connect to an external one - depending on the configuration.
   * @return an observable emitting the Elasticsearch client
   */
  private Observable<ElasticsearchClient> createElasticsearchClient() {
    JsonObject config = vertx.getOrCreateContext().config();
    boolean embedded = config.getBoolean(ConfigConstants.INDEX_ELASTICSEARCH_EMBEDDED, true);
    String host = config.getString(ConfigConstants.INDEX_ELASTICSEARCH_HOST, "localhost");
    int port = config.getInteger(ConfigConstants.INDEX_ELASTICSEARCH_PORT, 9200);
    
    String home = config.getString(ConfigConstants.HOME);
    
    String defaultElasticsearchDownloadUrl;
    try {
      defaultElasticsearchDownloadUrl = IOUtils.toString(getClass().getResource(
          "/elasticsearch_download_url.txt"));
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
    
    String elasticsearchDownloadUrl = config.getString(
        ConfigConstants.INDEX_ELASTICSEARCH_DOWNLOAD_URL, defaultElasticsearchDownloadUrl);
    String elasticsearchInstallPath = config.getString(
        ConfigConstants.INDEX_ELASTICSEARCH_INSTALL_PATH,
        home + "/elasticsearch/" + defaultElasticsearchVersion);
    
    ElasticsearchClient client = new ElasticsearchClient(host, port, INDEX_NAME,
        TYPE_NAME, vertx);
    
    if (!embedded) {
      // just return the client
      return Observable.just(client);
    }
    
    return client.isRunning().flatMap(running -> {
      if (running) {
        // we don't have to start Elasticsearch again
        return Observable.just(client);
      }
      
      // install Elasticsearch, start it and then create the client
      ElasticsearchInstaller installer = new ElasticsearchInstaller(vertx);
      return installer.download(elasticsearchDownloadUrl, elasticsearchInstallPath)
        .flatMap(path -> runElasticsearch(host, port, path))
        .flatMap(v -> waitUntilElasticsearchRunning(client))
        .map(v -> client);
    });
  }
  
  /**
   * Run Elasticsearch
   * @param host the host Elasticsearch should bind to
   * @param port the port Elasticsearch should listen on
   * @param elasticsearchInstallPath the path where Elasticsearch is installed
   * @return an observable that emits exactly one item when Elasticsearch has started
   */
  private Observable<Void> runElasticsearch(String host, int port,
      String elasticsearchInstallPath) {
    JsonObject config = vertx.getOrCreateContext().config();
    String storage = config.getString(ConfigConstants.STORAGE_FILE_PATH);
    String root = storage + "/index";
    
    ObservableFuture<Void> observable = RxHelper.observableFuture();
    Handler<AsyncResult<Void>> handler = observable.toHandler();
    
    ((Vertx)vertx.getDelegate()).<Void>executeBlocking(f -> {
      log.info("Starting Elasticsearch ...");
      
      // get Elasticsearch executable
      String executable = FilenameUtils.separatorsToSystem(
          elasticsearchInstallPath);
      executable = FilenameUtils.concat(executable, "bin");
      if (SystemUtils.IS_OS_WINDOWS) {
        executable = FilenameUtils.concat(executable, "elasticsearch.bat");
      } else {
        executable = FilenameUtils.concat(executable, "elasticsearch");
      }
      
      // start Elasticsearch
      CommandLine cmdl = new CommandLine(executable);
      cmdl.addArgument("--cluster.name");
      cmdl.addArgument("georocket-cluster");
      cmdl.addArgument("--node.name");
      cmdl.addArgument("georocket-node");
      cmdl.addArgument("--network.host");
      cmdl.addArgument(host);
      cmdl.addArgument("--http.port");
      cmdl.addArgument(String.valueOf(port));
      cmdl.addArgument("--path.home");
      cmdl.addArgument(elasticsearchInstallPath);
      cmdl.addArgument("--path.data");
      cmdl.addArgument(root + "/data");
      
      Executor executor = new DefaultExecutor();
      executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
      
      try {
        executor.execute(cmdl, new DefaultExecuteResultHandler() {
          @Override
          public void onProcessComplete(final int exitValue) {
            log.info("Elasticsearch quit with exit code: " + exitValue);
          }
          
          @Override
          public void onProcessFailed(final ExecuteException e) {
            log.error("Elasticsearch execution failed", e);
          }
        });
        f.complete();
      } catch (IOException e) {
        f.fail(e);
      }
    }, handler);
    
    return observable;
  }
  
  /**
   * Wait 60 seconds or until Elasticsearch is up and running, whatever
   * comes first
   * @param client the client to use to check if Elasticsearch is running
   * @return an observable that emits exactly one item when Elasticsearch
   * is running
   */
  private Observable<Void> waitUntilElasticsearchRunning(
      ElasticsearchClient client) {
    Scheduler scheduler = RxHelper.scheduler(getVertx());
    final Throwable repeat = new NoStackTraceThrowable("");
    return Observable.<Boolean>create(subscriber -> {
      client.isRunning().subscribe(subscriber);
    }).flatMap(running -> {
      if (!running) {
        return Observable.error(repeat);
      }
      return Observable.just(running);
    }).retryWhen(errors -> {
      Observable<Throwable> o = errors.flatMap(t -> {
        if (t == repeat) {
          // Elasticsearch is still not up, retry
          return Observable.just(t);
        }
        // forward error
        return Observable.error(t);
      });
      // retry for 60 seconds
      return RxUtils.makeRetry(60, 1000, scheduler, log).call(o);
    }, scheduler).map(r -> null);
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
      .buffer(BUFFER_TIMESPAN, TimeUnit.MILLISECONDS, MAX_ADD_REQUESTS)
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
        JsonObject metaObj = body.getJsonObject("meta");
        if (metaObj == null) {
          msg.fail(400, "Missing metadata for chunk " + path);
          return Observable.empty();
        }

        ChunkMeta meta = createChunkMeta(metaObj);
        
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
   * Create a {@link ChunkMeta} object. Sub-classes may override this
   * method to provide their own {@link ChunkMeta} type.
   * @param hit the chunk meta content used to initialize the object
   * @return the created object
   */
  protected ChunkMeta createChunkMeta(JsonObject hit) {
    return new ChunkMeta(hit);
  }
  
  /**
   * Open a chunk
   * @param path the path to the chunk to open
   * @return an observable that emits a {@link ChunkReadStream}
   */
  private Observable<ChunkReadStream> openChunk(String path) {
    ObservableFuture<ChunkReadStream> observable = RxHelper.observableFuture();
    store.getOne(path, observable.toHandler());
    return observable;
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
    return Observable.<Map<String, Object>>create(subscriber -> {
      openChunk(path)
        .flatMap(chunk -> {
          // convert chunk to document and close it
          return xmlChunkToDocument(chunk, fallbackCRSString).finallyDo(chunk::close);
        })
        .subscribe(subscriber);
    }).retryWhen(makeRetry(), RxHelper.scheduler(getVertx()));
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
      // execute a new search
      JsonObject query = queryCompiler.compileQuery(search, path);
      observable = client.beginScroll(query, pageSize, timeout);
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
   * Convert a chunk to a Elasticsearch document
   * @param chunk the chunk to convert
   * @param fallbackCRSString a string representing the CRS that should be used
   * to index the chunk if it does not specify a CRS itself (may be null if no
   * CRS is available as fallback)
   * @return an observable that will emit the document
   */
  private Observable<Map<String, Object>> xmlChunkToDocument(ChunkReadStream chunk,
      String fallbackCRSString) {
    AsyncXMLParser xmlParser = new AsyncXMLParser();
    
    List<XMLIndexer> indexers = new ArrayList<>();
    indexerFactories.forEach(factory -> {
      XMLIndexer i = (XMLIndexer)factory.createIndexer();
      if (fallbackCRSString != null && i instanceof CRSAware) {
        ((CRSAware)i).setFallbackCRSString(fallbackCRSString);
      }
      indexers.add(i);
    });
    
    return RxHelper.toObservable(chunk)
      .flatMap(xmlParser::feed)
      .doOnNext(e -> indexers.forEach(i -> i.onEvent(e)))
      .last() // "wait" until the whole chunk has been consumed
      .map(e -> {
        // create the Elasticsearch document
        Map<String, Object> doc = new HashMap<>();
        indexers.forEach(i -> doc.putAll(i.getResult()));
        return doc;
      })
      .finallyDo(xmlParser::close);
  }
  
  /**
   * Add chunk metadata to a ElasticSearch document
   * @param doc the document
   * @param meta the metadata to add to the document
   */
  protected void addMeta(Map<String, Object> doc, ChunkMeta meta) {
    doc.put("chunkStart", meta.getStart());
    doc.put("chunkEnd", meta.getEnd());
    doc.put("chunkParents", meta.getParents().stream().map(p ->
        p.toJsonObject().getMap()).collect(Collectors.toList()));
  }
  
  /**
   * Get chunk metadata from ElasticSearch document
   * @param source the document
   * @return the metadata
   */
  protected ChunkMeta getMeta(JsonObject source) {
    int start = source.getInteger("chunkStart");
    int end = source.getInteger("chunkEnd");
    
    JsonArray parentsList = source.getJsonArray("chunkParents");
    List<XMLStartElement> parents = parentsList.stream().map(o -> {
      JsonObject p = (JsonObject)o;
      String prefix = p.getString("prefix");
      String localName = p.getString("localName");
      String[] namespacePrefixes = jsonToArray(p.getJsonArray("namespacePrefixes"));
      String[] namespaceUris = jsonToArray(p.getJsonArray("namespaceUris"));
      String[] attributePrefixes = jsonToArray(p.getJsonArray("attributePrefixes"));
      String[] attributeLocalNames = jsonToArray(p.getJsonArray("attributeLocalNames"));
      String[] attributeValues = jsonToArray(p.getJsonArray("attributeValues"));
      return new XMLStartElement(prefix, localName, namespacePrefixes, namespaceUris,
          attributePrefixes, attributeLocalNames, attributeValues);
    }).collect(Collectors.toList());
    
    return new ChunkMeta(parents, start, end);
  }
  
  /**
   * Convert a JSON array to a String array. If the JSON array is null the
   * return value will also be null.
   * @param json the JSON array to convert
   * @return the array or null if <code>json</code> is null
   */
  private String[] jsonToArray(JsonArray json) {
    if (json == null) {
      return null;
    }
    String[] result = new String[json.size()];
    for (int i = 0; i < json.size(); ++i) {
      result[i] = json.getString(i);
    }
    return result;
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
    return client.indexExists().flatMap(exists -> {
      if (exists) {
        return Observable.just(null);
      } else {
        // index does not exist. create it.
        return createIndex();
      }
    });
  }
  
  /**
   * Create the Elasticsearch index. Assumes it does not exist yet.
   * @return an observable that will emit a single item when the index
   * has been created
   */
  @SuppressWarnings("unchecked")
  private Observable<Void> createIndex() {
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
      onIndexingFinished(stopTimeStamp - startTimeStamp, messages.size(),
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
   * @param chunkCount the number of chunks indexed
   * @param errorMessage an error message if the process has failed
   * or <code>null</code> if everything was successful
   */
  protected void onIndexingFinished(long duration, int chunkCount, String errorMessage) {
    if (errorMessage != null) {
      log.error("Indexing failed: " + errorMessage);
    } else {
      log.info("Finished indexing " + chunkCount + " chunks in " + duration + " ms");
    }
  }
}
