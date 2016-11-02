package io.georocket.index;

import static io.georocket.util.ThrowableHelper.throwableToCode;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;

import io.georocket.index.elasticsearch.ElasticsearchClient;
import io.georocket.index.elasticsearch.ElasticsearchClientFactory;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.query.QueryCompiler;
import io.georocket.util.RxUtils;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import rx.Observable;
import rx.functions.Func1;

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
    new ElasticsearchClientFactory(vertx).createElasticsearchClient(INDEX_NAME)
      .subscribe(es -> {
        client = es;

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
   * @see RxUtils#makeRetry(int, int, Logger)
   */
  protected Func1<Observable<? extends Throwable>, Observable<Long>> makeRetry() {
    return RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log);
  }
  
  /**
   * Register consumer for add messages
   */
  protected void registerAdd() {
    vertx.eventBus().<JsonObject>consumer(addressAdd)
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
   * @param documents a list of tuples containing document IDs, documents to
   * index, and the respective messages from which the documents were created
   * @return an observable that completes when the operation has finished
   */
  protected Observable<Void> insertDocuments(String type,
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
