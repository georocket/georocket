package io.georocket.index;

import static io.georocket.util.ThrowableHelper.throwableToCode;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import io.georocket.api.index.IndexerFactory;
import io.georocket.api.index.IndexerFactory.MatchPriority;
import io.georocket.api.index.xml.XMLIndexer;
import io.georocket.api.index.xml.XMLIndexerFactory;
import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.Store;
import io.georocket.storage.StoreFactory;
import io.georocket.util.AsyncXMLParser;
import io.georocket.util.QuotedStringSplitter;
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

/**
 * Background indexing of chunks added to the store
 * @author Michel Kraemer
 */
public class IndexerVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(IndexerVerticle.class);
  
  private static final int MAX_ADD_REQUESTS = 1000;
  private static final long BUFFER_TIMESPAN = 5000;
  private static final int MAX_INSERT_REQUESTS = 5;
  
  private static final String INDEX_NAME = "georocket";
  private static final String TYPE_NAME = "object";
  
  /**
   * The Elasticsearch node
   */
  private Node node;
  
  /**
   * The Elasticsearch client
   */
  private Client client;
  
  /**
   * The GeoRocket store
   */
  private Store store;
  
  /**
   * A service loader for {@link XMLIndexerFactory} objects
   */
  private ServiceLoader<XMLIndexerFactory> xmlIndexerFactoryLoader =
      ServiceLoader.load(XMLIndexerFactory.class);
  
  /**
   * True if {@link #ensureIndex()} has been called at least once
   */
  private boolean indexEnsured;
  
  @Override
  public void start() {
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
    node = NodeBuilder.nodeBuilder()
        .settings(settings)
        .clusterName("georocket-cluster")
        .data(true)
        .local(true)
        .node();
    
    client = node.client();
    store = StoreFactory.createStore((Vertx)vertx.getDelegate());
    
    registerAdd();
    registerDelete();
    registerQuery();
  }
  
  @Override
  public void stop() {
    client.close();
    node.close();
  }
  
  /**
   * Register consumer for add messages
   */
  private void registerAdd() {
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
  private void registerDelete() {
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
        
        log.trace("Indexing " + path);
        
        // open chunk and create IndexRequest
        return openChunk(path)
            .flatMap(chunk -> {
              // convert chunk to document and close it
              return chunkToDocument(chunk).finallyDo(chunk::close);
            })
            .doOnNext(doc -> {
              // add metadata and tags to document
              addMeta(doc, meta);
              if (tags != null) {
                doc.put("tags", tags);
              }
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
          .setPostFilter(makeQuery(search, path))
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
    long startDeleting = System.currentTimeMillis();
    log.info("Deleting " + paths.size() + " chunks from index ...");
    ObservableFuture<BulkResponse> observable = RxHelper.observableFuture();
    client.bulk(br, handlerToListener(observable.toHandler()));
    return observable.flatMap(bres -> {
      if (bres.hasFailures()) {
        return Observable.error(new NoStackTraceThrowable(bres.buildFailureMessage()));
      } else {
        log.info("Finished deleting " + paths.size() + " chunks from index in " +
            (System.currentTimeMillis() - startDeleting) + " ms");
        return Observable.just(null);
      }
    });
  }
  
  /**
   * Creates an ElasticSearch query from the given search string
   * @param search the search string
   * @param path the path where to perform the search (may be null if the
   * whole store should be searched)
   * @return the query
   */
  private QueryBuilder makeQuery(String search, String path) {
    QueryBuilder qb = makeQuery(search);
    if (path != null && !path.equals("/")) {
      String prefix = path.endsWith("/") ? path : path + "/";
      return QueryBuilders.boolQuery()
          .should(qb)
          .must(QueryBuilders.boolQuery()
              .should(QueryBuilders.termQuery("_id", path))
              .should(QueryBuilders.prefixQuery("_id", prefix)));
    }
    return qb;
  }
  
  /**
   * Creates an ElasticSearch query from the given search string
   * @param search the search string
   * @return the query
   */
  private QueryBuilder makeQuery(String search) {
    if (search == null || search.isEmpty()) {
      // match everything my default
      return QueryBuilders.matchAllQuery();
    }
    
    // split search query
    List<String> searches = QuotedStringSplitter.split(search);
    if (searches.size() == 1) {
      search = searches.get(0);
      
      BoolQueryBuilder bqb = QueryBuilders.boolQuery();
      bqb.should(QueryBuilders.termQuery("tags", search));
      
      for (IndexerFactory f : xmlIndexerFactoryLoader) {
        MatchPriority mp = f.getQueryPriority(search);
        if (mp == MatchPriority.ONLY) {
          return f.makeQuery(search);
        } else if (mp == MatchPriority.SHOULD) {
          bqb.should(f.makeQuery(search));
        } else if (mp == MatchPriority.MUST) {
          bqb.must(f.makeQuery(search));
        }
      }
      
      return bqb;
    }
    
    // call #makeQuery for every part of the search query recursively
    BoolQueryBuilder bqb = QueryBuilders.boolQuery();
    searches.stream().map(this::makeQuery).forEach(bqb::should);
    return bqb;
  }
  
  /**
   * Convert a chunk to a Elasticsearch document
   * @param chunk the chunk to convert
   * @return an observable that will emit the document
   */
  private Observable<Map<String, Object>> chunkToDocument(ChunkReadStream chunk) {
    AsyncXMLParser xmlParser = new AsyncXMLParser();
    
    List<XMLIndexer> indexers = new ArrayList<>();
    xmlIndexerFactoryLoader.forEach(factory -> indexers.add(factory.createIndexer()));
    
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
  private Observable<Void> createIndex() {
    Map<String, Object> integerNoIndex = ImmutableMap.of(
        "type", "integer",
        "index", "no"
    );
    Map<String, Object> stringNoIndex = ImmutableMap.of(
        "type", "string",
        "index", "no"
    );
    
    Builder<String, Object> propertiesBuilder = ImmutableMap.<String, Object>builder()
        .put("tags", ImmutableMap.of(
            "type", "string", // array of strings actually, auto-supported by Elasticsearch
            "index", "not_analyzed"
        ))
        
        // metadata: don't index it
        .put("chunkStart", integerNoIndex)
        .put("chunkEnd", integerNoIndex)
        .put("chunkParents", ImmutableMap.of(
            "type", "object",
            "properties", ImmutableMap.builder()
                .put("prefix", stringNoIndex)
                .put("localName", stringNoIndex)
                .put("namespacePrefixes", stringNoIndex)
                .put("namespaceUris", stringNoIndex)
                .put("attributePrefixes", stringNoIndex)
                .put("attributeLocalNames", stringNoIndex)
                .put("attributeValues", stringNoIndex)
                .build()
        ));
    
    // add all properties from XML indexers
    xmlIndexerFactoryLoader.forEach(factory ->
        propertiesBuilder.putAll(factory.getMapping()));
    
    Map<String, Object> source = ImmutableMap.of(
        "properties", propertiesBuilder.build(),
        
        // Do not save the original indexed document to save space. only include metadata!
        // See https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-source-field.html
        // for the drawbacks of this approach!
        "_source", ImmutableMap.of(
            "includes", Arrays.asList(
                "chunkStart",
                "chunkEnd",
                "chunkParents"
             )
        )
    );
    
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
    log.info("Indexing " + messages.size() + " chunks");
    long startIndexing = System.currentTimeMillis();
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
      if (bres.hasFailures()) {
        log.error(bres.buildFailureMessage());
      } else {
        log.info("Finished indexing " + messages.size() + " chunks in " +
            (System.currentTimeMillis() - startIndexing) + " ms");
      }
      return Observable.empty();
    });
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
