package io.georocket;

import static io.georocket.util.ThrowableHelper.throwableToCode;
import static io.georocket.util.ThrowableHelper.throwableToMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.spi.FileTypeDetector;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import io.georocket.util.TikaFileTypeDetector;
import org.apache.commons.io.FileUtils;
import org.bson.types.ObjectId;

import com.google.common.base.Splitter;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.IndexerVerticle;
import io.georocket.output.Merger;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.Store;
import io.georocket.storage.StoreCursor;
import io.georocket.storage.StoreFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
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
  
  protected static File geoRocketHome;
  private Store store;
  private String storagePath;
  FileTypeDetector typeDetector = new TikaFileTypeDetector();

  /**
   * Handles the HTTP GET request for a bunch of chunks
   * @param context the routing context
   */
  private void onGet(RoutingContext context) {
    String path = getStorePath(context);
    
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
    initializeMerger(merger, search, path, o.toHandler());
    o.flatMap(v -> {
      ObservableFuture<Void> o2 = RxHelper.observableFuture();
      doMerge(merger, search, path, response, o2.toHandler());
      return o2;
    }).reduce((v1, v2) -> v1).subscribe(v -> {
      response.end();
    }, err -> {
      if (!(err instanceof FileNotFoundException)) {
        log.error("Could not perform query", err);
      }
      response.setStatusCode(throwableToCode(err)).end(throwableToMessage(err, ""));
    });
  }
  
  /**
   * Initialize the given merger. Perform a search using the given search string
   * and pass all chunk metadata retrieved to the merger.
   * @param merger the merger to initialize
   * @param search the search query
   * @param path the path where to perform the search
   * @param handler will be called when the merger has been initialized with
   * all results
   */
  private void initializeMerger(Merger merger, String search, String path,
      Handler<AsyncResult<Void>> handler) {
    store.get(search, path, getar -> {
      if (getar.failed()) {
        handler.handle(Future.failedFuture(getar.cause()));
      } else {
        iterateCursor(getar.result(), (meta, callback) -> {
          merger.init(meta, initar -> {
            if (initar.failed()) {
              handler.handle(Future.failedFuture(initar.cause()));
            } else {
              callback.run();
            }
          });
        }, handler);
      }
    });
  }
  
  /**
   * Performs a search and merges all retrieved chunks using the given merger
   * @param merger the merger
   * @param search the search query
   * @param path the path where to perform the search
   * @param out a write stream to write the merged chunk to
   * @param handler will be called when all chunks have been merged
   */
  private void doMerge(Merger merger, String search, String path,
      WriteStream<Buffer> out, Handler<AsyncResult<Void>> handler) {
    store.get(search, path, getar -> {
      if (getar.failed()) {
        handler.handle(Future.failedFuture(getar.cause()));
      } else {
        long[] count = new long[] { 0 };
        long[] notaccepted = new long[] { 0 };
        StoreCursor cursor = getar.result();
        iterateCursor(cursor, (meta, callback) -> {
          ++count[0];
          store.getOne(cursor.getChunkPath(), openar -> {
            if (openar.failed()) {
              handler.handle(Future.failedFuture(openar.cause()));
            } else {
              ChunkReadStream crs = openar.result();
              Handler<AsyncResult<Void>> mergeHandler = mergeAr -> {
                if (mergeAr.failed()) {
                  handler.handle(mergeAr);
                } else {
                  crs.close();
                  callback.run();
                }
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
    request.pause();

    String contentType = request.getHeader("Content-Type");
    String layer = getStorePath(context);
    String tagsStr = request.getParam("tags");
    List<String> tags = tagsStr != null ? Splitter.on(',')
        .trimResults().splitToList(tagsStr) : null;
    
    // get temporary filename
    String incoming = storagePath + "/incoming";
    String filename = new ObjectId().toString();
    String filePath = incoming + "/" + filename;
    
    log.info("Receiving file ...");
    
    // create directory for incoming files
    FileSystem fs = vertx.fileSystem();
    ObservableFuture<Void> observable = RxHelper.observableFuture();
    fs.mkdirs(incoming, observable.toHandler());
    observable
      .flatMap(v -> {
        // create temporary file
        ObservableFuture<AsyncFile> openObservable = RxHelper.observableFuture();
        fs.open(filePath, new OpenOptions(), openObservable.toHandler());
        return openObservable;
      })
      .flatMap(f -> {
        ObservableFuture<AsyncFile> pumpObservable = RxHelper.observableFuture();
        Handler<AsyncResult<AsyncFile>> pumpHandler = pumpObservable.toHandler();
        Pump.pump(request, f).start();
        Handler<Throwable> errHandler = (Throwable t) -> {
          request.endHandler(null);
          f.close();
          pumpHandler.handle(Future.failedFuture(t));
        };
        f.exceptionHandler(errHandler);
        request.exceptionHandler(errHandler);
        request.endHandler(v -> {
          pumpHandler.handle(Future.succeededFuture(f));
        });
        request.resume();
        return pumpObservable;
      })
      .subscribe(f -> {
        // close file before importing
        f.close();

        // run importer
        JsonObject msg = new JsonObject()
            .put("action", "import")
            .put("filename", filename)
            .put("layer", layer);

        if (tags != null) {
          msg.put("tags", new JsonArray(tags));
        }

        Runnable respondAccepted = () -> {
          request.response()
              .setStatusCode(202) // Accepted
              .setStatusMessage("Accepted file - importing in progress")
              .end();
        };

        if (contentType == null || contentType.trim().isEmpty() || contentType.equals("application/octet-stream")) {
          // Fallback: If the client have not send the Content-Type or send a generic one,
          // then guess it by the file magic number or file content
          vertx.<String>executeBlocking(blockingHandler -> {
            try {
              String mimeType = typeDetector.probeContentType(Paths.get(filePath));
              blockingHandler.complete(mimeType);
            } catch (IOException ex) {
              blockingHandler.fail(ex);
            }
          }, h -> {
            if (h.failed()) {
              log.error("The client have not send a Content-Type during the import and i could not guess it by " +
                  "the content. (Will drop the file import)", h.cause());

              request.response()
                  .setStatusCode(415) // Unsupported Media Type
                  .setStatusMessage("Unsupported Media Type - Your media type wasn't specified in the request header " +
                      "and could not be guessed.")
                  .end();
            } else {
              respondAccepted.run();

              // run importer
              msg.put("contentType", h.result());

              vertx.eventBus().send(AddressConstants.IMPORTER, msg);
            }
          });
        } else {
          // The client have send a content type
          // tell caller that we're now importing the file
          respondAccepted.run();

          msg.put("contentType", contentType);

          vertx.eventBus().send(AddressConstants.IMPORTER, msg);
        }

      }, err -> {
        request.response()
          .setStatusCode(throwableToCode(err))
          .end("Could not import file: " + err.getMessage());
        err.printStackTrace();
        fs.delete(filePath, ar -> {});
      });
  }

  /**
   * Get absolute data store path from request
   * @param context the current routing context
   * @return the absolute path (never null, default: "/")
   */
  private String getStorePath(RoutingContext context) {
    String path = context.normalisedPath();
    String routePath = context.currentRoute().getPath();
    String result = null;
    if (routePath.length() < path.length()) {
      result = path.substring(routePath.length());
    }
    if (result == null || result.isEmpty()) {
      return "/";
    }
    if (result.charAt(0) != '/') {
      result = "/" + result;
    }
    return result;
  }
  
  /**
   * Handles the HTTP DELETE request
   * @param context the routing context
   */
  private void onDelete(RoutingContext context) {
    String path = getStorePath(context);
    
    HttpServerResponse response = context.response();
    HttpServerRequest request = context.request();
    String search = request.getParam("search");
    
    store.delete(search, path, ar -> {
      if (ar.failed()) {
        Throwable t = ar.cause();
        log.error("Could not delete chunks", t);
        response.setStatusCode(throwableToCode(t)).end(t.getMessage());
      } else {
        response.setStatusCode(204).end();
      }
    });
  }

  /**
   * Deploy a new verticle with the standard configuration of this instance.
   *
   * @param cls The verticle class to deploy.
   *
   * @return An future which will carry the deployment id of this verticle.
   */
  protected ObservableFuture<String> deployVerticle(Class<? extends Verticle> cls) {
    ObservableFuture<String> observable = RxHelper.observableFuture();
    DeploymentOptions options = new DeploymentOptions().setConfig(config());
    vertx.deployVerticle(cls.getName(), options, observable.toHandler());
    return observable;
  }

  /**
   * Deploy an indexer verticle.
   *
   * @return An future which will be completed if the verticle was deployed and will carry his deployment id.
   */
  protected ObservableFuture<String> deployIndexer() {
    return deployVerticle(IndexerVerticle.class);
  }

  /**
   * Deploy an importer verticle.
   *
   * @return An future which will be completed if the verticle was deployed and will carry his deployment id.
   */
  protected ObservableFuture<String> deployImporter() {
    return deployVerticle(ImporterVerticle.class);
  }

  private ObservableFuture<HttpServer> deployHttpServer() {
    int port = config().getInteger(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT);

    Router router = createRouter();
    HttpServerOptions serverOptions = createHttpServerOptions();
    HttpServer server = vertx.createHttpServer(serverOptions);

    ObservableFuture<HttpServer> observable = RxHelper.observableFuture();
    server.requestHandler(router::accept).listen(port, observable.toHandler());
    return observable;
  }

  /**
   * Create a {@link Router} and add routes for <code>/store/</code>
   * to it. Sub-classes may override if they want to add further routes
   * @return the created {@link Router}
   */
  protected Router createRouter() {
    Router router = Router.router(vertx);
    router.get("/store/*").handler(this::onGet);
    router.post("/store/*").handler(this::onPost);
    router.delete("/store/*").handler(this::onDelete);
    return router;
  }

  /**
   * Create a {@link HttpServerOptions} and set <code>Compression
   * Support</code> as option. Sub-classes may override if they want to
   * add further options
   * @return the created {@link HttpServerOptions}
   */
  protected HttpServerOptions createHttpServerOptions() {
    HttpServerOptions serverOptions = new HttpServerOptions()
        .setCompressionSupported(true);
    return serverOptions;
  }
  
  @Override
  public void start(Future<Void> startFuture) {
    log.info("Launching GeoRocket ...");

    store = StoreFactory.createStore(vertx);
    storagePath = vertx.getOrCreateContext().config().getString(
        ConfigConstants.STORAGE_FILE_PATH);

    this.deployIndexer()
        .flatMap(v -> deployImporter())
        .flatMap(v -> deployHttpServer())
        .subscribe(id -> {
          log.info("GeoRocket launched successfully.");
          startFuture.complete();
        }, startFuture::fail);
  }
  
  /**
   * Replace configuration variables in a string
   * @param str the string
   * @return a copy of the given string with configuration variables replaced
   */
  private static String replaceConfVariables(String str) {
    return str.replace("$GEOROCKET_HOME", geoRocketHome.getAbsolutePath());
  }
  
  /**
   * Recursively replace configuration variables in an array
   * @param arr the array
   * @return a copy of the given array with configuration variables replaced
   */
  private static JsonArray replaceConfVariables(JsonArray arr) {
    JsonArray result = new JsonArray();
    for (Object o : arr) {
      if (o instanceof JsonObject) {
        replaceConfVariables((JsonObject)o);
      } else if (o instanceof JsonArray) {
        o = replaceConfVariables((JsonArray)o);
      } else if (o instanceof String) {
        o = replaceConfVariables((String)o);
      }
      result.add(o);
    }
    return result;
  }
  
  /**
   * Recursively replace configuration variables in an object
   * @param obj the object
   */
  protected static void replaceConfVariables(JsonObject obj) {
    Set<String> keys = new HashSet<>(obj.getMap().keySet());
    for (String key : keys) {
      Object value = obj.getValue(key);
      if (value instanceof JsonObject) {
        replaceConfVariables((JsonObject)value);
      } else if (value instanceof JsonArray) {
        JsonArray arr = replaceConfVariables((JsonArray)value);
        obj.put(key, arr);
      } else if (value instanceof String) {
        String newValue = replaceConfVariables((String)value);
        obj.put(key, newValue);
      }
    }
  }
  
  /**
   * Set default configuration values
   * @param conf the current configuration
   */
  protected static void setDefaultConf(JsonObject conf) {
    conf.put(ConfigConstants.HOME, "$GEOROCKET_HOME");
    if (!conf.containsKey(ConfigConstants.STORAGE_FILE_PATH)) {
      conf.put(ConfigConstants.STORAGE_FILE_PATH, "$GEOROCKET_HOME/storage");
    }
  }

  /**
   * Read the georocket home out of the system environment.
   *
   * @return The gerocket home path.
   */
  protected static String getRocketHomeStr() {
    String geoRocketHomeStr = System.getenv("GEOROCKET_HOME");
    if (geoRocketHomeStr == null) {
      log.info("Environment variable GEOROCKET_HOME not set. Using current "
          + "working directory.");
      geoRocketHomeStr = new File(".").getAbsolutePath();
    }
    return geoRocketHomeStr;
  }

  /**
   * Get the gerocket home as file.
   *
   * @param geoRocketHomeStr The path to the georocket home.
   *
   * @return The georocket home file.
   */
  protected static File getRocketHomeFile(String geoRocketHomeStr) {
    try {
      return geoRocketHome = new File(geoRocketHomeStr).getCanonicalFile();
    } catch (IOException e) {
      log.error("Invalid GeoRocket home: " + geoRocketHomeStr);
      System.exit(1);
      return null; // Will never happen
    }
  }

  /**
   * Load the georocket configuration and return it.
   * Will return an empty configuration if something went wrong.
   *
   * @param geoRocketHome The configuration file.
   *
   * @return The configuration
   */
  protected static JsonObject loadConfiguration(File geoRocketHome) {
    File confDir = new File(geoRocketHome, "conf");
    File confFile = new File(confDir, "georocketd.json");

    try {
      String confFileStr = FileUtils.readFileToString(confFile, "UTF-8");
      return new JsonObject(confFileStr);
    } catch (IOException e) {
      log.error("Could not read config file " + confFile, e);
    } catch (DecodeException e) {
      log.error("Invalid config file", e);
    }
    return new JsonObject();
  }

  /**
   * Get and load the georocket configuration.
   *
   * @return The configuration
   */
  protected static JsonObject configureGeoRocket() {
    // get GEOROCKET_HOME
    String geoRocketHomeStr = getRocketHomeStr();
    geoRocketHome = getRocketHomeFile(geoRocketHomeStr);

    log.info("Using GeoRocket home " + geoRocketHome);

    // load configuration file
    JsonObject conf = loadConfiguration(geoRocketHome);

    // set default configuration values
    setDefaultConf(conf);

    // replace variables in config
    replaceConfVariables(conf);

    return conf;
  }

  /**
   * Runs the server
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    JsonObject conf = configureGeoRocket();

    // deploy main verticle
    Vertx vertx = Vertx.vertx();
    DeploymentOptions options = new DeploymentOptions().setConfig(conf);
    vertx.deployVerticle(GeoRocket.class.getName(), options, ar -> {
      if (ar.failed()) {
        log.error("Could not deploy GeoRocket");
        ar.cause().printStackTrace();
        System.exit(1);
        return;
      }
    });
  }
}
