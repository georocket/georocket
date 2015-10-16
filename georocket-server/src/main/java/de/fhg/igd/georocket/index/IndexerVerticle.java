package de.fhg.igd.georocket.index;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.xml.stream.events.XMLEvent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.google.common.collect.ImmutableMap;

import de.fhg.igd.georocket.constants.AddressConstants;
import de.fhg.igd.georocket.constants.ConfigConstants;
import de.fhg.igd.georocket.storage.file.FileStore;
import de.fhg.igd.georocket.storage.file.Store;
import de.fhg.igd.georocket.util.ChunkMeta;
import de.fhg.igd.georocket.util.XMLPipeStream;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;

/**
 * Background indexing of chunks added to the store
 * @author Michel Kraemer
 */
public class IndexerVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(IndexerVerticle.class);
  
  private static final String NS_GML = "http://www.opengis.net/gml";
  private static final String INDEX_NAME = "georocket";
  private static final String TYPE_NAME = "object";
  
  /**
   * The ElasticSearch client
   */
  private Client client;
  
  /**
   * The GeoRocket store
   */
  private Store store;
  
  /**
   * True if {@link #ensureIndex(Handler)} has been called at least once
   */
  private boolean indexEnsured;
  
  /**
   * True if {@link #insertDocument(IndexRequest)} has just sent a request
   * to ElasticSearch and this request is currently being processed
   */
  private boolean insertInProgress;
  
  /**
   * A queue for {@link IndexRequest}s used by {@link #insertDocument(IndexRequest)}
   * if an insert is already in progress
   */
  private Queue<IndexRequest> docsToInsert = new ArrayDeque<>();
  
  @Override
  public void start() {
    log.info("Launching indexer ...");
    vertx.eventBus().consumer(AddressConstants.INDEXER, this::onMessage);
    
    String home = vertx.getOrCreateContext().config().getString(
        ConfigConstants.HOME, System.getProperty("user.home") + "/.georocket");
    String root = home + "/storage/index";
    
    // start embedded ElasticSearch instance
    Settings settings = Settings.builder()
        .put("node.name", "georocket-node")
        .put("path.home", root)
        .put("path.data", root + "/data")
        .put("http.enabled", true) // TODO enable HTTP for debugging purpose only!
        .build();
    Node node = NodeBuilder.nodeBuilder()
        .settings(settings)
        .clusterName("georocket-cluster")
        .data(true)
        .local(true)
        .node();
    
    client = node.client();
    store = new FileStore(vertx);
  }
  
  /**
   * Receives a name of a chunks to index
   * @param msg the event bus message containing the chunk name 
   */
  private void onMessage(Message<JsonObject> msg) {
    String filename = msg.body().getString("filename");
    ChunkMeta meta = ChunkMeta.fromJsonObject(msg.body().getJsonObject("meta"));
    log.debug("Indexing " + filename);
    
    // get chunk from store and index it
    store.get(filename, ar -> {
      if (ar.failed()) {
        log.error("Could not get chunk from store", ar.cause());
        return;
      }
      ReadStream<Buffer> chunk = ar.result();
      indexChunk(filename, chunk, meta);
    });
  }
  
  /**
   * Adds a chunk to the index
   * @param name the chunk's name
   * @param chunk the chunk to index
   * @param meta the chunk's metadata
   */
  private void indexChunk(String name, ReadStream<Buffer> chunk, ChunkMeta meta) {
    // create XML parser
    XMLPipeStream xmlStream = new XMLPipeStream(vertx);
    Pump.pump(chunk, xmlStream).start();
    chunk.endHandler(v -> xmlStream.close());
    
    // a HashMap retrieving the attributes that will be added to the
    // ElasticSearch index
    Map<String, Object> doc = new HashMap<>();
    
    xmlStream.handler(event -> {
      // index gml:id
      if (event.getEvent() == XMLEvent.START_ELEMENT) {
        String gmlId = event.getXMLReader().getAttributeValue(NS_GML, "id");
        if (gmlId != null) {
          List<String> gmlIds = (List<String>)doc.get("gmlIds");
          if (gmlIds == null) {
            gmlIds = new ArrayList<>();
            doc.put("gmlIds", gmlIds);
          }
          gmlIds.add(gmlId);
        }
      }
      
      // insert document to index at the end of the XML stream
      if (event.getEvent() == XMLEvent.END_DOCUMENT) {
        addMeta(doc, meta);
        insertDocument(name, doc);
      }
    });
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
   * Ensures the ElasticSearch index exists
   * @param handler will be called when the index has been created or if it
   * already exists
   */
  private void ensureIndex(Handler<AsyncResult<Void>> handler) {
    if (indexEnsured) {
      handler.handle(Future.succeededFuture());
    } else {
      // check if index exists
      IndicesExistsRequest r = Requests.indicesExistsRequest(INDEX_NAME);
      client.admin().indices().exists(r, handlerToListener(existsAr -> {
        if (existsAr.failed()) {
          handler.handle(Future.failedFuture(existsAr.cause()));
        } else {
          indexEnsured = true;
          if (existsAr.result().isExists()) {
            handler.handle(Future.succeededFuture());
          } else {
            // index does not exist. create it.
            createIndex(handler);
          }
        }
      }));
    }
  }
  
  /**
   * Creates the ElasticSearch index. Assumes it does not exist yet.
   * @param handler will be called when the index has been created.
   */
  private void createIndex(Handler<AsyncResult<Void>> handler) {
    Map<String, Object> integerNoIndex = ImmutableMap.of(
        "type", "integer",
        "index", "no"
    );
    Map<String, Object> stringNoIndex = ImmutableMap.of(
        "type", "string",
        "index", "no"
    );
    
    Map<String, Object> source = ImmutableMap.of(
        "properties", ImmutableMap.of(
            "gmlIds", ImmutableMap.of(
                "type", "string", // array of strings actually, auto-supported by ElasticSearch
                "index", "not_analyzed" // do not analyze (i.e. tokenize) this field, use the actual value
            ),
            
            // metadata: don't index it
            "chunkStart", integerNoIndex,
            "chunkEnd", integerNoIndex,
            "chunkParents", ImmutableMap.of(
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
            )
        ),
        
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
    client.admin().indices().create(request, handlerToListener(handler, v -> null));
  }
  
  /**
   * Inserts an ElasticSearch document to the index
   * @param id the chunk's ID
   * @param doc the document to add
   */
  private void insertDocument(String id, Map<String, Object> doc) {
    IndexRequest req = Requests.indexRequest(INDEX_NAME)
        .type(TYPE_NAME)
        .id(id)
        .source(doc);
    insertDocument(req);
  }
  
  /**
   * Inserts an ElasticSearch document to the index. Enqueues the document
   * if an insert operation is currently in progress.
   * @param req the IndexRequest containing the document
   * @see #insertDocument(String, Map)
   */
  private void insertDocument(IndexRequest req) {
    // TODO improve performance a lot through bulk insert!
    if (insertInProgress) {
      docsToInsert.offer(req);
    } else {
      insertInProgress = true;
      
      // ensure index exists
      ensureIndex(eiar -> {
        if (eiar.failed()) {
          log.error("Could not create index", eiar.cause());
        } else {
          // add document to index
          client.index(req, handlerToListener(ar -> {
            log.debug("Finished indexing " + req.id());
            if (ar.failed()) {
              log.error("Could not insert document into index", ar.cause());
            }
            insertInProgress = false;
            
            // proceed with the next document
            if (!docsToInsert.isEmpty()) {
              insertDocument(docsToInsert.poll());
            }
          }));
        }
      });
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
    return handlerToListener(handler, v -> v);
  }
  
  /**
   * Convenience method to convert a Vert.x {@link Handler} to an ElasticSearch
   * {@link ActionListener}. Applies a given function to the {@link ActionListener}'s
   * result before calling the handler
   * @param <T> the type of the {@link ActionListener}'s result
   * @param <R> the type of the {@link Handler}'s result
   * @param handler the handler to convert
   * @param f the function to apply to the {@link ActionListener}'s result
   * before calling the handler
   * @return the {@link ActionListener}
   */
  private <T, R> ActionListener<T> handlerToListener(Handler<AsyncResult<R>> handler, Function<T, R> f) {
    return new ActionListener<T>() {
      @Override
      public void onResponse(T response) {
        vertx.runOnContext(v -> {
          handler.handle(Future.succeededFuture(f.apply(response)));
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
