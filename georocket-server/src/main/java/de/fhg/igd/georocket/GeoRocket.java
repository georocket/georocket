package de.fhg.igd.georocket;

import java.io.FileNotFoundException;
import java.util.function.BiConsumer;

import de.fhg.igd.georocket.constants.ConfigConstants;
import de.fhg.igd.georocket.index.IndexerVerticle;
import de.fhg.igd.georocket.input.FirstLevelSplitter;
import de.fhg.igd.georocket.input.Splitter;
import de.fhg.igd.georocket.output.Merger;
import de.fhg.igd.georocket.storage.ChunkReadStream;
import de.fhg.igd.georocket.storage.Store;
import de.fhg.igd.georocket.storage.StoreCursor;
import de.fhg.igd.georocket.storage.file.FileStore;
import de.fhg.igd.georocket.util.ChunkMeta;
import de.fhg.igd.georocket.util.WindowPipeStream;
import de.fhg.igd.georocket.util.XMLPipeStream;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;

/**
 * GeoRocket - A high-performance database for geospatial files
 * @author Michel Kraemer
 */
public class GeoRocket extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(GeoRocket.class);
  
  private Store store;
  
  /**
   * Convert an asynchronous result to an HTTP status code
   * @param ar the result to convert
   * @return the HTTP status code
   */
  private static int resultToCode(AsyncResult<?> ar) {
    if (ar.failed()) {
      if (ar.cause() instanceof ReplyException) {
        return ((ReplyException)ar.cause()).failureCode();
      } else if (ar.cause() instanceof IllegalArgumentException) {
        return 400;
      } else if (ar.cause() instanceof FileNotFoundException) {
        return 404;
      }
      return 500;
    }
    return 200;
  }
  
  /**
   * Imports an XML file from the given input stream into the store
   * @param f the XML file to read
   * @param callback will be called when the operation has finished
   */
  private void importXML(ReadStream<Buffer> f, Handler<AsyncResult<Void>> callback) {
    XMLPipeStream xmlStream = new XMLPipeStream(vertx);
    WindowPipeStream windowPipeStream = new WindowPipeStream();
    Splitter splitter = new FirstLevelSplitter(windowPipeStream.getWindow());
    
    Pump.pump(f, windowPipeStream).start();
    Pump.pump(windowPipeStream, xmlStream).start();
    
    f.endHandler(v -> {
      xmlStream.close();
      callback.handle(Future.succeededFuture());
    });
    
    f.exceptionHandler(e -> callback.handle(Future.failedFuture(e)));
    windowPipeStream.exceptionHandler(e -> callback.handle(Future.failedFuture(e)));
    xmlStream.exceptionHandler(e -> callback.handle(Future.failedFuture(e)));
    
    xmlStream.handler(event -> {
      Splitter.Result splitResult = splitter.onEvent(event);
      if (splitResult != null) {
        // splitter has created a chunk. store it.
        xmlStream.pause(); // pause stream while chunk being written
        store.add(splitResult.getChunk(), splitResult.getMeta(), ar -> {
          if (ar.failed()) {
            callback.handle(Future.failedFuture(ar.cause()));
          } else {
            // go ahead
            xmlStream.resume();
          }
        });
      }
    });
  }
  
  /**
   * Handles the HTTP GET request for a single chunk
   * @param context the routing context
   */
  private void onGetOne(RoutingContext context) {
    HttpServerRequest request = context.request();
    HttpServerResponse response = context.response();
    
    String name = request.getParam("name");
    
    // get chunk from store
    store.getOne(name, getar -> {
      if (getar.failed()) {
        log.error("Could not get chunk", getar.cause());
        response.setStatusCode(resultToCode(getar)).end(getar.cause().getMessage());
        return;
      }
      
      // write Content-Length
      ChunkReadStream crs = getar.result();
      response.putHeader("Content-Length", String.valueOf(crs.getSize()));
      
      // send chunk to client
      Pump.pump(crs, response).start();
      crs.endHandler(v -> {
        response.end();
        crs.close();
      });
    });
  }
  
  /**
   * Handles the HTTP GET request for a bunch of chunks
   * @param context the routing context
   */
  private void onGet(RoutingContext context) {
    HttpServerResponse response = context.response();
    
    // TODO remove
    response.setChunked(true);
    
    // TODO get from request
    String search = "";
    
    // perform two searches: first initialize the merger and then
    // merge all retrieved chunks
    Merger merger = new Merger();
    ObservableFuture<Void> o = RxHelper.observableFuture();
    initializeMerger(merger, search, o.toHandler());
    o.flatMap(v -> {
      ObservableFuture<Void> o2 = RxHelper.observableFuture();
      doMerge(merger, search, response, o2.toHandler());
      return o2;
    }).reduce((v1, v2) -> v1).subscribe(v -> {
      response.end();
    }, err -> {
      log.error("Could not perform query", err);
      response.setStatusCode(500).end(err.getMessage());
    });
  }
  
  /**
   * Initialize the given merger. Perform a search using the given search string
   * and pass all chunk metadata retrieved to the merger.
   * @param merger the merger to initialize
   * @param search the search query
   * @param handler will be called when the merger has been initialized with
   * all results
   */
  private void initializeMerger(Merger merger, String search, Handler<AsyncResult<Void>> handler) {
    store.get(search, getar -> {
      if (getar.failed()) {
        handler.handle(Future.failedFuture(getar.cause()));
      } else {
        iterateCursor(getar.result(), (meta, callback) -> {
          merger.init(meta);
          callback.run();
        }, handler);
      }
    });
  }
  
  /**
   * Performs a search and merges all retrieved chunks using the given merger
   * @param merger the merger
   * @param search the search query
   * @param out a write stream to write the merged chunk to
   * @param handler will be called when all chunks have been merged
   */
  private void doMerge(Merger merger, String search, WriteStream<Buffer> out,
      Handler<AsyncResult<Void>> handler) {
    store.get(search, getar -> {
      if (getar.failed()) {
        handler.handle(Future.failedFuture(getar.cause()));
      } else {
        StoreCursor cursor = getar.result();
        iterateCursor(cursor, (meta, callback) -> {
          cursor.openChunk(openar -> {
            if (openar.failed()) {
              handler.handle(Future.failedFuture(openar.cause()));
            } else {
              ChunkReadStream crs = openar.result();
              merger.merge(crs, meta, out, v -> {
                crs.close();
                callback.run();
              });
            }
          });
        }, handler);
      }
    });
  }
  
  /**
   * Iterate through all items from a {@link StoreCursor}
   * @param cursor the cursor
   * @param consumer consumes all items
   * @param endHandler will be called when all items have been consumed
   */
  private void iterateCursor(StoreCursor cursor, BiConsumer<ChunkMeta, Runnable> consumer,
      Handler<AsyncResult<Void>> endHandler) {
    if (cursor.hasNext()) {
      cursor.next(ar -> {
        if (ar.failed()) {
          endHandler.handle(Future.failedFuture(ar.cause()));
        } else {
          consumer.accept(ar.result(), () -> {
            iterateCursor(cursor, consumer, endHandler);
          });
        }
      });
    } else {
      endHandler.handle(Future.succeededFuture());
    }
  }
  
  /**
   * Handles the HTTP POST request
   * @param context the routing context
   */
  private void onPost(RoutingContext context) {
    HttpServerRequest request = context.request();
    importXML(request, ar -> {
      if (ar.failed()) {
        request.response()
          .setStatusCode(resultToCode(ar))
          .end("Could not parse XML: " + ar.cause().getMessage());
        ar.cause().printStackTrace();
      } else {
        request.response()
          .setStatusCode(202) // Accepted
          .setStatusMessage("Accepted file - indexing in progress")
          .end();
      }
    });
  }
  
  private ObservableFuture<String> deployVerticle(Class<? extends Verticle> cls) {
    ObservableFuture<String> observable = RxHelper.observableFuture();
    DeploymentOptions options = new DeploymentOptions().setConfig(config());
    vertx.deployVerticle(cls.getName(), options, observable.toHandler());
    return observable;
  }
  
  private ObservableFuture<HttpServer> deployHttpServer() {
    int port = config().getInteger(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT);
    
    Router router = Router.router(vertx);
    router.get("/db/:name").handler(this::onGetOne);
    router.get("/db").handler(this::onGet);
    router.post("/db").handler(this::onPost);
    
    HttpServerOptions serverOptions = new HttpServerOptions()
        .setCompressionSupported(true);
    HttpServer server = vertx.createHttpServer(serverOptions);
    
    ObservableFuture<HttpServer> observable = RxHelper.observableFuture();
    server.requestHandler(router::accept).listen(port, observable.toHandler());
    return observable;
  }
  
  @Override
  public void start(Future<Void> startFuture) {
    log.info("Launching GeoRocket ...");
    
    store = new FileStore(vertx);
    
    deployVerticle(IndexerVerticle.class)
      .flatMap(v -> deployHttpServer())
      .subscribe(id -> {
        startFuture.complete();
      }, err -> {
        startFuture.fail(err);
      });
  }
  
  /**
   * Runs the server
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(GeoRocket.class.getName(), ar -> {
      if (ar.failed()) {
        log.error("Could not deploy GeoRocket");
        ar.cause().printStackTrace();
        System.exit(1);
        return;
      }
    });
  }
}
