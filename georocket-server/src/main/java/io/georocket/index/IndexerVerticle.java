package io.georocket.index;

import io.georocket.constants.ConfigConstants;
import io.georocket.index.elasticsearch.ElasticsearchClient;
import io.georocket.index.elasticsearch.ElasticsearchInstaller;
import io.georocket.index.elasticsearch.ElasticsearchRunner;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.query.QueryCompiler;
import io.georocket.util.RxUtils;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jooq.lambda.Seq;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func1;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.georocket.util.ThrowableHelper.throwableToCode;

/**
 * Generic methods for background indexing of any messages
 * @author Michel Kraemer
 */
public abstract class IndexerVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(IndexerVerticle.class);
  
  protected static final int MAX_ADD_REQUESTS = 200;
  protected static final long BUFFER_TIMESPAN = 5000;
  protected static final int MAX_INSERT_REQUESTS = 5;
  protected static final int MAX_RETRIES = 5;
  protected static final int RETRY_INTERVAL = 1000;

  /**
   * Elasticsearch index
   */
  protected static final String INDEX_NAME = "georocket";

  /**
   *  Addresses on Vert.x eventbus for requests
   */
  private final String addressAdd;
  private final String addressQuery;
  private final String addressDelete;

  /**
   * Runs Elasticsearch
   */
  protected ElasticsearchRunner runner;
  
  /**
   * The Elasticsearch client
   */
  protected ElasticsearchClient client;

  /**
   * Compiles search strings to Elasticsearch documents
   */
  protected DefaultQueryCompiler queryCompiler;
  
  /**
   * HashSet to store if {@link #ensureMapping(String)}} has been
   * called at least once for particular Elasticsearch type.
   * If type entry exists in HashSet, it was called.
   */
  private HashSet<String> mappingEnsured;
  
  /**
   * True if {@link #ensureIndex()} has been called at least once
   */
  private boolean indexEnsured;

  /**
   * Constructor. Set all addresses on Vert.x eventbus
   * @param addressAdd address on Vert.x eventbus for add operations
   * @param addressQuery address on Vert.x eventbus for query operations
   * @param addressDelete address on Vert.x eventbus for delete operations
   */
  protected IndexerVerticle(String addressAdd, String addressQuery, String addressDelete) {
    this.addressAdd = addressAdd;
    this.addressQuery = addressQuery;
    this.addressDelete = addressDelete;
    mappingEnsured = new HashSet<>();
  }
  
  @Override
  public void start(Future<Void> startFuture) {

    createElasticsearchClient()
      .subscribe(es -> {
        client = es.getKey();
        runner = es.getValue();

        queryCompiler = createQueryCompiler();

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
   * Create an Elasticsearch client. Either start an Elasticsearch instance or
   * connect to an external one - depending on the configuration.
   * @return an observable emitting an Elasticsearch client and runner
   */
  private Observable<Pair<ElasticsearchClient, ElasticsearchRunner>> createElasticsearchClient() {
    boolean embedded = config().getBoolean(ConfigConstants.INDEX_ELASTICSEARCH_EMBEDDED, true);
    String host = config().getString(ConfigConstants.INDEX_ELASTICSEARCH_HOST, "localhost");
    int port = config().getInteger(ConfigConstants.INDEX_ELASTICSEARCH_PORT, 9200);

    ElasticsearchClient client = new ElasticsearchClient(host, port, INDEX_NAME, vertx);
    
    if (!embedded) {
      // just return the client
      return Observable.just(Pair.of(client, null));
    }
    
    return client.isRunning().flatMap(running -> {
      if (running) {
        // we don't have to start Elasticsearch again
        return Observable.just(Pair.of(client, null));
      }

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
    vertx.eventBus().<JsonObject>consumer(addressAdd)
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
          log.fatal("Could not index document", e);
        }
        
        @Override
        public void onNext(List<Message<JsonObject>> messages) {
          onAdd(messages)
            .subscribe(v -> {
              // should never happen
            }, err -> {
              log.error("Could not index document", err);
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
    vertx.eventBus().<JsonObject>consumer(addressDelete)
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
  protected void registerQuery() {
    vertx.eventBus().<JsonObject>consumer(addressQuery)
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
   * Will be called before the indexer starts deleting documents
   * @param timeStamp the time when the indexer has started deleting
   * @param count the number of documents to delete
   */
  protected void onDeletingStarted(long timeStamp, int count) {
    log.info("Deleting " + count + " documents from index ...");
  }

  /**
   * Will be called after the indexer has finished deleting documents
   * @param duration the time it took to delete the documents
   * @param count the number of deleted documents
   * @param errorMessage an error message if the process has failed
   * or <code>null</code> if everything was successful
   */
  protected void onDeletingFinished(long duration, int count, String errorMessage) {
    if (errorMessage != null) {
      log.error("Deleting documents failed: " + errorMessage);
    } else {
      log.info("Finished deleting " + count +
          " documents from index in " + duration + " ms");
    }
  }
  
  /**
   * Ensure the Elasticsearch index exists
   * @return an observable that will emit a single item when the index has
   * been created or if it already exists
   */
  protected Observable<Void> ensureIndex() {
    if (indexEnsured) {
      return Observable.just(null);
    }
    indexEnsured = true;

    // check if the index exists
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
   * Ensure the Elasticsearch mapping exists
   * @param type the target type for the mapping
   * @return an observable that will emit a single item when the mapping has
   * been created or if it already exists
   */
  protected Observable<Void> ensureMapping(String type) {
    if (mappingEnsured.contains(type)) {
      return Observable.just(null);
    }
    mappingEnsured.add(type);

    // check if the target type exists
    return client.typeExists(type).flatMap(exists -> {
      if (exists) {
        return Observable.just(null);
      } else {
        // target type does not exist. create the mapping.
        return createMapping();
      }
    });
  }
  
  /**
   * Create the Elasticsearch index
   * @return an observable that will emit a single item when the index
   * has been created
   */
  protected Observable<Void> createIndex() {
    return client.createIndex().map(r -> null);
  }
  
  /**
   * Inserts multiple Elasticsearch documents to the index. It performs a
   * bulk request. This method replies to all messages if the bulk request
   * was successful.
   * @param type Elasticsearch type for documents
   * @param documents the documents to insert
   * @param messages a list of messages from which the index requests were
   * created; items are in the same order as the respective index requests in
   * the given bulk request
   * @return an observable that completes when the operation has finished
   */
  protected Observable<Void> insertDocuments(String type, Map<String, JsonObject> documents,
      List<Message<JsonObject>> messages) {
    long startTimeStamp = System.currentTimeMillis();
    onIndexingStarted(startTimeStamp, messages.size());

    return client.bulkInsert(type, documents).flatMap(bres -> {
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
   * @param count the number of documents to index
   */
  protected void onIndexingStarted(long timeStamp, int count) {
    log.info("Indexing " + count + " documents");
  }

  /**
   * Will be called after the indexer has finished the indexing process
   * @param duration the time passed during indexing
   * @param importIds the import IDs of the documents that were processed by
   * the indexer. This list may include IDs of documents whose indexing failed.
   * @param errorMessage an error message if the process has failed
   * or <code>null</code> if everything was successful
   */
  protected void onIndexingFinished(long duration, List<String> importIds,
      String errorMessage) {
    if (errorMessage != null) {
      log.error("Indexing failed: " + errorMessage);
    } else {
      log.info("Finished indexing " + importIds.size() + " documents in " +
          duration + " " + "ms");
    }
  }

  /**
   * Will be called when documents should be added to the index
   * @param messages the list of add messages that contain the paths to
   * the documents to be indexed
   * @return an observable that completes when the operation has finished
   */
  protected abstract Observable<Void> onAdd(List<Message<JsonObject>> messages);

  /**
   * Write result of a query given the Elasticsearch response
   * @param body the message containing the query
   * @return an observable that emits the results of the query
   */
  abstract protected Observable<JsonObject> onQuery(JsonObject body);

  /**
   * Deletes documents from the index
   * @param body the message containing the paths to the documents to delete
   * @return an observable that emits a single item when the documents have
   * been deleted successfully
   */
  protected abstract Observable<Void> onDelete(JsonObject body);

  /**
   * Create the Elasticsearch mapping
   * @return an observable that will emit a single item when the mapping
   * has been created
   */
  protected abstract Observable<Void> createMapping();

  /**
   * Create collection of all query compilers for this verticle.
   * @return Collection of {@link QueryCompiler}
   */
  protected abstract DefaultQueryCompiler createQueryCompiler();
}
