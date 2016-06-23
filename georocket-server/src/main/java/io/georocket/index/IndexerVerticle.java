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

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
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
   * The Elasticsearch node
   */
  private Node node;
  
  /**
   * The Elasticsearch client
   */
  protected Client client;
  
  /**
   * The GeoRocket store
   */
  protected Store store;
  
  /**
   * A list of {@link XMLIndexerFactory} objects
   */
  protected ImmutableList<XMLIndexerFactory> xmlIndexerFactories;
  
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
    
    String home = vertx.getOrCreateContext().config().getString(
        ConfigConstants.STORAGE_FILE_PATH);
    String root = home + "/index";
    
    // start embedded ElasticSearch instance
    Settings settings = Settings.builder()
        .put("node.name", "georocket-node")
        .put("path.home", root)
        .put("path.data", root + "/data")
        .put("http.enabled", true) // TODO enable HTTP for debugging purpose only!
        .build();
    
    // load xml indexer factories now and not lazily to avoid concurrent
    // modifications to the service loader's internal cache
    xmlIndexerFactories = ImmutableList.copyOf(ServiceLoader.load(XMLIndexerFactory.class));
    
    vertx.<Node>executeBlocking(f -> {
      f.complete(NodeBuilder.nodeBuilder()
          .settings(settings)
          .clusterName("georocket-cluster")
          .data(true)
          .local(true)
          .node());
    }, ar -> {
      if (ar.failed()) {
        startFuture.fail(ar.cause());
        return;
      }
      
      node = ar.result();
      client = node.client();
      
      queryCompiler = new DefaultQueryCompiler(xmlIndexerFactories);
      store = StoreFactory.createStore((Vertx)vertx.getDelegate());
      
      registerMessageConsumers();
      
      startFuture.complete();
    });
  }
  
  @Override
  public void stop() {
    client.close();
    node.close();
  }

  /**
   * Register all message consumers for this verticle.
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
  private Func1<Observable<? extends Throwable>, Observable<Long>> makeRetry() {
    return RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log);
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
        ChunkMeta meta = ChunkMeta.fromJsonObject(metaObj);
        
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
              // add metadata and tags to document
              addMeta(doc, meta);
              if (tags != null) {
                doc.put("tags", tags);
              }

              doc.put("importId", importId);
              doc.put("filename", filename);
              doc.put("importTime", importTime);
            })
            .map(doc -> Pair.of(documentToIndexRequest(path, doc), msg))
            .onErrorResumeNext(err -> {
              msg.fail(throwableToCode(err), err.getMessage());
              return Observable.empty();
            });
      })
      // create bulk request
      .reduce(Pair.of(Requests.bulkRequest(), new ArrayList<Message<JsonObject>>()), (i, p) -> {
        i.getLeft().add(p.getLeft());
        i.getRight().add(p.getRight());
        return i;
      })
      .flatMap(p -> ensureIndex().map(v -> p))
      .flatMap(p -> {
        if (!p.getRight().isEmpty()) {
          return insertDocuments(p.getLeft(), p.getRight());
        }
        return Observable.empty();
      });
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
  private Observable<Map<String, Object>> openChunkToDocument(String path,
      String fallbackCRSString) {
    return Observable.<Map<String, Object>>create(subscriber -> {
      openChunk(path)
        .flatMap(chunk -> {
          // convert chunk to document and close it
          return xmlChunkToDocument(chunk, fallbackCRSString).finallyDo(chunk::close);
        })
        .subscribe(subscriber);
    }).retryWhen(makeRetry());
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
    TimeValue timeout = TimeValue.timeValueMinutes(1);
    
    // search result handler
    ObservableFuture<SearchResponse> observable = RxHelper.observableFuture();
    ActionListener<SearchResponse> listener = handlerToListener(observable.toHandler());
    Observable<JsonObject> result = observable.map(sr -> {
      // iterate through all hits and convert them to JSON
      SearchHits hits = sr.getHits();
      long totalHits = hits.getTotalHits();
      JsonArray resultHits = new JsonArray();
      for (SearchHit hit : hits) {
        ChunkMeta meta = getMeta(hit.getSource());
        JsonObject obj = meta.toJsonObject()
            .put("id", hit.getId());
        resultHits.add(obj);
      }
      
      // create result and send it to the client
      return new JsonObject()
          .put("totalHits", totalHits)
          .put("hits", resultHits)
          .put("scrollId", sr.getScrollId());
    });
    
    if (scrollId == null) {
      // execute a new search
      client.prepareSearch(INDEX_NAME)
          .setTypes(TYPE_NAME)
          .setScroll(timeout)
          .setSize(pageSize)
          .addSort("_doc", SortOrder.ASC) // sort by doc (fastest way to scroll)
          .setPostFilter(queryCompiler.compileQuery(search, path))
          .execute(listener);
    } else {
      // continue searching
      client.prepareSearchScroll(scrollId)
          .setScroll(timeout)
          .execute(listener);
    }
    
    return result;
  }
  
  /**
   * Deletes chunks from the index
   * @param body the message containing the paths to the chunks to delete
   * @return an observable that emits a single item when the chunks have
   * been deleted successfully
   */
  private Observable<Void> onDelete(JsonObject body) {
    JsonArray paths = body.getJsonArray("paths");
    
    // prepare bulk request for all chunks to delete
    BulkRequest br = Requests.bulkRequest();
    for (int i = 0; i < paths.size(); ++i) {
      String path = paths.getString(i);
      br.add(Requests.deleteRequest(INDEX_NAME).type(TYPE_NAME).id(path));
    }
    
    // execute bulk request
    long startTimeStamp = System.currentTimeMillis();
    startedDeleting(startTimeStamp, paths.size());

    ObservableFuture<BulkResponse> observable = RxHelper.observableFuture();
    client.bulk(br, handlerToListener(observable.toHandler()));
    return observable.flatMap(bres -> {
      long stopTimeStamp = System.currentTimeMillis();
      if (bres.hasFailures()) {
        finishedDeleting(startTimeStamp - stopTimeStamp, paths.size(), true, bres.buildFailureMessage());
        return Observable.error(new NoStackTraceThrowable(bres.buildFailureMessage()));
      } else {
        finishedDeleting(startTimeStamp - stopTimeStamp, paths.size(), false, null);
        return Observable.just(null);
      }
    });
  }


  /**
   * <p>Will be called, before the indexer has started the deleting process.</p>
   * <p>The deleting process will be called if an index should be removed.</p>
   *
   * @param timeStamp The time when the indexer has started
   * @param chunkCount The number of chunks messages from which the deleting requests were created
   */
  protected void startedDeleting(long timeStamp, int chunkCount) {
    log.info("Deleting " + chunkCount + " chunks from index ...");
  }

  /**
   * <p>Will be called, after the indexer has finished the deleting process.</p>
   * <p>The deleting process will be called if an index should be removed.</p>
   *
   * @param duration The deleting duration - time passing from the start until the end
   * @param chunkCount The number of chunks messages from which the deleting requests were created
   * @param withError true if an error has occurred
   * @param errorMessage The error message. (Will be null of withError == false)
   */
  protected void finishedDeleting(long duration, int chunkCount, boolean withError, String errorMessage) {
    if (withError) {
      log.error("Finished deleting with error: " + errorMessage);
    } else {
      log.info("Finished deleting " + chunkCount + " chunks from index in " + duration + " ms");
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
  private Observable<Map<String, Object>> xmlChunkToDocument(ChunkReadStream chunk, String fallbackCRSString) {
    AsyncXMLParser xmlParser = new AsyncXMLParser();
    
    List<XMLIndexer> indexers = new ArrayList<>();
    xmlIndexerFactories.forEach(factory -> {
      XMLIndexer i = factory.createIndexer();
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
  private void addMeta(Map<String, Object> doc, ChunkMeta meta) {
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
  @SuppressWarnings("unchecked")
  private ChunkMeta getMeta(Map<String, Object> source) {
    int start = ((Number)source.get("chunkStart")).intValue();
    int end = ((Number)source.get("chunkEnd")).intValue();
    
    List<Map<String, Object>> parentsList = (List<Map<String, Object>>)source.get("chunkParents");
    List<XMLStartElement> parents = parentsList.stream().map(p -> {
      String prefix = (String)p.get("prefix");
      String localName = (String)p.get("localName");
      String[] namespacePrefixes = safeListToArray((List<String>)p.get("namespacePrefixes"));
      String[] namespaceUris = safeListToArray((List<String>)p.get("namespaceUris"));
      String[] attributePrefixes = safeListToArray((List<String>)p.get("attributePrefixes"));
      String[] attributeLocalNames = safeListToArray((List<String>)p.get("attributeLocalNames"));
      String[] attributeValues = safeListToArray((List<String>)p.get("attributeValues"));
      return new XMLStartElement(prefix, localName, namespacePrefixes, namespaceUris,
          attributePrefixes, attributeLocalNames, attributeValues);
    }).collect(Collectors.toList());
    
    return new ChunkMeta(parents, start, end);
  }
  
  /**
   * Convert a list to an array. If the list is null the return value
   * will also be null.
   * @param list the list to convert
   * @return the array or null if <code>list</code> is null
   */
  private String[] safeListToArray(List<String> list) {
    if (list == null) {
      return null;
    }
    return list.toArray(new String[list.size()]);
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
    IndicesExistsRequest r = Requests.indicesExistsRequest(INDEX_NAME);
    ObservableFuture<IndicesExistsResponse> observable = RxHelper.observableFuture();
    client.admin().indices().exists(r, handlerToListener(observable.toHandler()));
    return observable.flatMap(eres -> {
      if (eres.isExists()) {
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
    Map<String, Object> source;
    try (InputStream is = this.getClass().getResourceAsStream("index_defaults.yaml")) {
      source = (Map<String, Object>)yaml.load(is);
    } catch (IOException e) {
      return Observable.error(e);
    }
    
    // remove unnecessary node
    source.remove("variables");
    
    // merge all properties from XML indexers
    xmlIndexerFactories.forEach(factory ->
        MapUtils.deepMerge(source, factory.getMapping()));
    
    CreateIndexRequest request = Requests.createIndexRequest(INDEX_NAME)
        .mapping(TYPE_NAME, source);
    ObservableFuture<CreateIndexResponse> observable = RxHelper.observableFuture();
    client.admin().indices().create(request, handlerToListener(observable.toHandler()));
    return observable.map(r -> null);
  }
  
  /**
   * Convert an Elasticsearch document to an {@link IndexRequest}
   * @param path the absolute path to the chunk
   * @param doc the document to convert
   * @return the {@link IndexRequest}
   */
  private IndexRequest documentToIndexRequest(String path, Map<String, Object> doc) {
    return Requests.indexRequest(INDEX_NAME)
        .type(TYPE_NAME)
        .id(path)
        .source(doc);
  }
  
  /**
   * Inserts multiple Elasticsearch documents to the index. It performs a
   * bulk request. This method replies to all messages if the bulk request
   * was successful.
   * @param bulkRequest the {@link BulkRequest} containing the documents
   * @param messages a list of messages from which the index requests were
   * created; items are in the same order as the respective index requests in
   * the given bulk request
   * @return an observable that completes when the operation has finished
   */
  private Observable<Void> insertDocuments(BulkRequest bulkRequest, List<Message<JsonObject>> messages) {
    long startTimeStamp = System.currentTimeMillis();
    startedIndexing(startTimeStamp, messages.size());

    ObservableFuture<BulkResponse> observable = RxHelper.observableFuture();
    client.bulk(bulkRequest, handlerToListener(observable.toHandler()));
    return observable.<Void>flatMap(bres -> {
      for (BulkItemResponse item : bres.getItems()) {
        Message<JsonObject> msg = messages.get(item.getItemId());
        if (item.isFailed()) {
          msg.fail(500, item.getFailureMessage());
        } else {
          msg.reply(null);
        }
      }
      long stopTimeStamp = System.currentTimeMillis();
      if (bres.hasFailures()) {
        finishedIndexing(startTimeStamp - stopTimeStamp, messages.size(), true, bres.buildFailureMessage());
      } else {
        finishedIndexing(startTimeStamp - stopTimeStamp, messages.size(), false, null);
      }
      return Observable.empty();
    });
  }

  /**
   * Will be called, before the indexer has started the indexing process.
   *
   * @param timeStamp The time when the indexer has started
   * @param chunkCount The number of chunks messages from which the index requests were created
   */
  protected void startedIndexing(long timeStamp, int chunkCount) {
    log.info("Indexing " + chunkCount + " chunks");
  }

  /**
   * Will be called, after the indexer has finished the indexing process.
   *
   * @param duration The indexing duration - time passing from the start until the end
   * @param chunkCount The number of chunks messages from which the index requests were created
   * @param withError true if an error has occurred
   * @param errorMessage The error message. (Will be null of withError == false)
   */
  protected void finishedIndexing(long duration, int chunkCount, boolean withError, String errorMessage) {
    if (withError) {
      log.error("Finished indexing with error: " + errorMessage);
    } else {
      log.info("Finished indexing " + chunkCount + " chunks in " + duration + " ms");
    }
  }

  /**
   * Convenience method to convert a Vert.x {@link Handler} to an ElasticSearch
   * {@link ActionListener}
   * @param <T> the type of the {@link ActionListener}'s result
   * @param handler the handler to convert
   * @return the {@link ActionListener}
   */
  private <T> ActionListener<T> handlerToListener(Handler<AsyncResult<T>> handler) {
    return new ActionListener<T>() {
      @Override
      public void onResponse(T response) {
        vertx.runOnContext(v -> {
          handler.handle(Future.succeededFuture(response));
        });
      }

      @Override
      public void onFailure(Throwable e) {
        vertx.runOnContext(v -> {
          handler.handle(Future.failedFuture(e));
        });
      }
    };
  }
}
