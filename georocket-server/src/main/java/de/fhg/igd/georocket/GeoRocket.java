package de.fhg.igd.georocket;

import java.io.FileNotFoundException;
import java.util.function.BiConsumer;

import org.bson.types.ObjectId;

import de.fhg.igd.georocket.constants.AddressConstants;
import de.fhg.igd.georocket.constants.ConfigConstants;
import de.fhg.igd.georocket.index.IndexerVerticle;
import de.fhg.igd.georocket.output.Merger;
import de.fhg.igd.georocket.storage.ChunkMeta;
import de.fhg.igd.georocket.storage.ChunkReadStream;
import de.fhg.igd.georocket.storage.Store;
import de.fhg.igd.georocket.storage.StoreCursor;
import de.fhg.igd.georocket.storage.file.FileStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
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
  private String home;
  
  /**
   * Convert a throwable to an HTTP status code
   * @param t the throwable to convert
   * @return the HTTP status code
   */
  private static int throwableToCode(Throwable t) {
    if (t instanceof ReplyException) {
      return ((ReplyException)t).failureCode();
    } else if (t instanceof IllegalArgumentException) {
      return 400;
    } else if (t instanceof FileNotFoundException) {
      return 404;
    }
    return 500;
  }
  
  /**
   * Convert an asynchronous result to an HTTP status code
   * @param ar the result to convert
   * @return the HTTP status code
   */
  private static int resultToCode(AsyncResult<?> ar) {
    if (ar.failed()) {
      return throwableToCode(ar.cause());
    }
    return 200;
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
    HttpServerRequest request = context.request();
    String search = request.getParam("search");
    
    // Our responses must always be chunked because we cannot calculate
    // the exact content-length beforehand. We perform two searches, one to
    // initialize the merger and one to do the actual merge. The problem is
    // that the result set may change between these two searches and so we
    // cannot calculate the content-length just from looking at the result
    // from the first search.
    response.setChunked(true);
    
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
      if (!(err instanceof FileNotFoundException)) {
        log.error("Could not perform query", err);
      }
      response.setStatusCode(throwableToCode(err)).end(err.getMessage());
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
        long[] count = new long[] { 0 };
        long[] notaccepted = new long[] { 0 };
        StoreCursor cursor = getar.result();
        iterateCursor(cursor, (meta, callback) -> {
          ++count[0];
          cursor.openChunk(openar -> {
            if (openar.failed()) {
              handler.handle(Future.failedFuture(openar.cause()));
            } else {
              ChunkReadStream crs = openar.result();
              Handler<Void> mergeHandler = v -> {
                crs.close();
                callback.run();
              };
              try {
                merger.merge(crs, meta, out, mergeHandler);
              } catch (IllegalArgumentException e) {
                // chunk cannot be merged. maybe it's a new one that has
                // been added after the Merger has been initialized.
                // just ignore it, but emit a warning later
                ++notaccepted[0];
                mergeHandler.handle(null);
              }
            }
          });
        }, ar -> {
          if (ar.succeeded()) {
            if (notaccepted[0] > 0) {
              log.warn("Could not merge " + notaccepted[0] + " chunks "
                  + "because the merger did not accept them. Most likely "
                  + "these are new chunks that were added while the "
                  + "merge was in progress. If this worries you, just "
                  + "repeat the request.");
            }
            if (count[0] > 0) {
              merger.finishMerge(out);
              handler.handle(ar);
            } else {
              handler.handle(Future.failedFuture(new FileNotFoundException("Not Found")));
            }
          } else {
            handler.handle(ar);
          }
        });
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
    
    // get temporary filename
    String incoming = home + "/incoming";
    String id = new ObjectId().toString();
    String filename = incoming + "/" + id;
    
    log.info("Receiving file ...");
    
    // create directory for incoming files
    FileSystem fs = vertx.fileSystem();
    ObservableFuture<Void> observable = RxHelper.observableFuture();
    fs.mkdirs(incoming, observable.toHandler());
    observable
      .flatMap(v -> {
        // create temporary file
        ObservableFuture<AsyncFile> openObservable = RxHelper.observableFuture();
        fs.open(filename, new OpenOptions(), openObservable.toHandler());
        return openObservable;
      })
      .flatMap(f -> {
        ObservableFuture<AsyncFile> pumpObservable = RxHelper.observableFuture();
        Handler<AsyncResult<AsyncFile>> pumpHandler = pumpObservable.toHandler();
        Pump.pump(request, f).start();
        Handler<Throwable> errHandler = (Throwable t) -> {
          f.close();
          pumpHandler.handle(Future.failedFuture(t));
        };
        f.exceptionHandler(errHandler);
        request.exceptionHandler(errHandler);
        request.endHandler(v -> {
          pumpHandler.handle(Future.succeededFuture(f));
        });
        return pumpObservable;
      })
      .subscribe(f -> {
        // tell caller that we're now importing the file
        request.response()
          .setStatusCode(202) // Accepted
          .setStatusMessage("Accepted file - importing in progress")
          .end();
        
        // close file before importing
        f.close();
        
        // run importer
        JsonObject msg = new JsonObject()
            .put("action", "import")
            .put("filename", id);
        vertx.eventBus().send(AddressConstants.IMPORTER, msg);
      }, err -> {
        request.response()
          .setStatusCode(throwableToCode(err))
          .end("Could not import file: " + err.getMessage());
        err.printStackTrace();
        fs.delete(filename, ar -> {});
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
    home = vertx.getOrCreateContext().config().getString(
        ConfigConstants.HOME, System.getProperty("user.home") + "/.georocket");
    
    deployVerticle(IndexerVerticle.class)
      .flatMap(v -> deployVerticle(ImporterVerticle.class))
      .flatMap(v -> deployHttpServer())
      .subscribe(id -> {
        log.info("GeoRocket launched successfully.");
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
