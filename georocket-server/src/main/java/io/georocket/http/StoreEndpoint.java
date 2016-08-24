package io.georocket.http;

import static io.georocket.util.ThrowableHelper.throwableToCode;
import static io.georocket.util.ThrowableHelper.throwableToMessage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.spi.FileTypeDetector;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.bson.types.ObjectId;

import com.google.common.base.Splitter;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.output.Merger;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.Store;
import io.georocket.storage.StoreCursor;
import io.georocket.storage.StoreFactory;
import io.georocket.util.HttpException;
import io.georocket.util.TikaFileTypeDetector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
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
import rx.Observable;

/**
 * An HTTP endpoint handling requests related to the GeoRocket data store
 * @author Michel Kraemer
 */
public class StoreEndpoint implements Endpoint {
  private static Logger log = LoggerFactory.getLogger(StoreEndpoint.class);
  
  private final Vertx vertx;
  private final FileTypeDetector typeDetector = new TikaFileTypeDetector();
  
  private Store store;
  private String storagePath;

  /**
   * Create the endpoint
   * @param vertx the Vert.x instance
   */
  public StoreEndpoint(Vertx vertx) {
    this.vertx = vertx;
    store = StoreFactory.createStore(vertx);
    storagePath = vertx.getOrCreateContext().config()
        .getString(ConfigConstants.STORAGE_FILE_PATH);
  }

  @Override
  public Router createRouter() {
    Router router = Router.router(vertx);
    router.get("/*").handler(this::onGet);
    router.post("/*").handler(this::onPost);
    router.delete("/*").handler(this::onDelete);
    return router;
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
   * Try to detect the content type of a file
   * @param filepath the absolute path to the file to analyse
   * @return an observable emitting either the detected content type or an error
   * if the content type could not be detected or the file could not be read
   */
  private Observable<String> detectContentType(String filepath) {
    ObservableFuture<String> result = RxHelper.observableFuture();
    Handler<AsyncResult<String>> resultHandler = result.toHandler();
    
    vertx.<String>executeBlocking(f -> {
      try {
        String mimeType = typeDetector.probeContentType(Paths.get(filepath));
        f.complete(mimeType);
      } catch (IOException e) {
        f.fail(e);
      }
    }, ar -> {
      if (ar.failed()) {
        resultHandler.handle(Future.failedFuture(ar.cause()));
      } else {
        String ct = ar.result();
        if (ct != null) {
          resultHandler.handle(Future.succeededFuture(ar.result()));
        } else {
          resultHandler.handle(Future.failedFuture(new HttpException(215)));
        }
      }
    });
    
    return result;
  }
  
  /**
   * Handles the HTTP POST request
   * @param context the routing context
   */
  private void onPost(RoutingContext context) {
    HttpServerRequest request = context.request();
    request.pause();

    String layer = getStorePath(context);
    String tagsStr = request.getParam("tags");
    List<String> tags = tagsStr != null ? Splitter.on(',')
        .trimResults().splitToList(tagsStr) : null;
    
    // get temporary filename
    String incoming = storagePath + "/incoming";
    String filename = new ObjectId().toString();
    String filepath = incoming + "/" + filename;
    
    log.info("Receiving file ...");
    
    // create directory for incoming files
    FileSystem fs = vertx.fileSystem();
    ObservableFuture<Void> observable = RxHelper.observableFuture();
    fs.mkdirs(incoming, observable.toHandler());
    observable
      .flatMap(v -> {
        // create temporary file
        ObservableFuture<AsyncFile> openObservable = RxHelper.observableFuture();
        fs.open(filepath, new OpenOptions(), openObservable.toHandler());
        return openObservable;
      })
      .flatMap(f -> {
        // write request body into temporary file
        ObservableFuture<Void> pumpObservable = RxHelper.observableFuture();
        Handler<AsyncResult<Void>> pumpHandler = pumpObservable.toHandler();
        Pump.pump(request, f).start();
        Handler<Throwable> errHandler = (Throwable t) -> {
          request.endHandler(null);
          f.close();
          pumpHandler.handle(Future.failedFuture(t));
        };
        f.exceptionHandler(errHandler);
        request.exceptionHandler(errHandler);
        request.endHandler(v -> {
          f.close();
          pumpHandler.handle(Future.succeededFuture());
        });
        request.resume();
        return pumpObservable;
      })
      .flatMap(v -> {
        String contentTypeHeader = request.getHeader("Content-Type");
        String mimeType = null;

        try {
          ContentType contentType = ContentType.parse(contentTypeHeader);
          mimeType = contentType.getMimeType();
        } catch (ParseException | IllegalArgumentException ex) {
          // mimeType already null
        }

        // detect content type of file to import
        if (mimeType == null || mimeType.trim().isEmpty() ||
            mimeType.equals("application/octet-stream") ||
            mimeType.equals("application/x-www-form-urlencoded")) {
          // fallback: if the client has not sent a Content-Type or if it's
          // a generic one, then try to guess it
          log.warn("Mime type '" + mimeType + "' is invalid or generic. "
              + "Trying to guess the right type.");
          return detectContentType(filepath).doOnNext(guessedType -> {
            log.warn("Guessed mime type '" + guessedType + "'.");
          });
        }

        return Observable.just(mimeType);
      })
      .subscribe(detectedContentType -> {
        // run importer
        JsonObject msg = new JsonObject()
            .put("action", "import")
            .put("filename", filename)
            .put("layer", layer)
            .put("contentType", detectedContentType);

        if (tags != null) {
          msg.put("tags", new JsonArray(tags));
        }

        request.response()
          .setStatusCode(202) // Accepted
          .setStatusMessage("Accepted file - importing in progress")
          .end();

        // run importer
        vertx.eventBus().send(AddressConstants.IMPORTER, msg);
      }, err -> {
        request.response()
          .setStatusCode(throwableToCode(err))
          .end("Could not import file: " + err.getMessage());
        err.printStackTrace();
        fs.delete(filepath, ar -> {});
      });
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
        response.setStatusCode(throwableToCode(t)).end(throwableToMessage(t, ""));
      } else {
        response.setStatusCode(204).end();
      }
    });
  }
}
