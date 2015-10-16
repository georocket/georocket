package de.fhg.igd.georocket;

import java.io.FileNotFoundException;

import de.fhg.igd.georocket.constants.ConfigConstants;
import de.fhg.igd.georocket.index.IndexerVerticle;
import de.fhg.igd.georocket.input.FirstLevelSplitter;
import de.fhg.igd.georocket.input.Splitter;
import de.fhg.igd.georocket.storage.file.ChunkReadStream;
import de.fhg.igd.georocket.storage.file.FileStore;
import de.fhg.igd.georocket.storage.file.Store;
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
   * Handles the HTTP GET request
   * @param context the routing context
   */
  private void onGet(RoutingContext context) {
    HttpServerRequest request = context.request();
    HttpServerResponse response = context.response();
    
    String name = request.getParam("name");
    
    // get chunk from store
    store.get(name, getar -> {
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
      crs.endHandler(v -> response.end());
    });
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
    router.get("/db/:name").handler(this::onGet);
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
