package io.georocket.http;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.bson.types.ObjectId;

import com.google.common.base.Splitter;

import io.georocket.ServerAPIException;
import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.output.MultiMerger;
import io.georocket.storage.RxAsyncCursor;
import io.georocket.storage.RxStore;
import io.georocket.storage.RxStoreCursor;
import io.georocket.storage.StoreCursor;
import io.georocket.storage.StoreFactory;
import io.georocket.util.HttpException;
import io.georocket.util.MimeTypeUtils;
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
import rx.Single;

/**
 * An HTTP endpoint handling requests related to the GeoRocket data store
 * @author Michel Kraemer
 */
public class StoreEndpoint extends AbstractEndpoint {
  private static Logger log = LoggerFactory.getLogger(StoreEndpoint.class);
  
  private final Vertx vertx;
  
  private RxStore store;
  private String storagePath;

  /**
   * True if the store should report activities to the Vert.x event bus
   */
  private final boolean reportActivities;

  /**
   * Create the endpoint
   * @param vertx the Vert.x instance
   */
  public StoreEndpoint(Vertx vertx) {
    reportActivities = vertx.getOrCreateContext()
        .config().getBoolean(ConfigConstants.REPORT_ACTIVITIES, false);
    this.vertx = vertx;
    store = new RxStore(StoreFactory.createStore(vertx));
    storagePath = vertx.getOrCreateContext().config()
        .getString(ConfigConstants.STORAGE_FILE_PATH);
  }

  @Override
  public Router createRouter() {
    Router router = Router.router(vertx);
    router.get("/*").handler(this::onGet);
    router.put("/*").handler(this::onPut);
    router.post("/*").handler(this::onPost);
    router.delete("/*").handler(this::onDelete);
    return router;
  }
  
  /**
   * Initialize the given merger. Perform a search using the given search string
   * and pass all chunk metadata retrieved to the merger.
   * @param merger the merger to initialize
   * @param data data to use for the initialization
   * @return an observable that will emit exactly one item when the merger
   * has been initialized with all results
   */
  private Observable<Void> initializeMerger(MultiMerger merger, Single<StoreCursor> data) {
    return data
      .map(RxStoreCursor::new)
      .flatMapObservable(RxStoreCursor::toObservable)
      .map(Pair::getLeft)
      .flatMap(merger::init)
      .defaultIfEmpty(null)
      .last();
  }
  
  /**
   * Perform a search and merge all retrieved chunks using the given merger
   * @param merger the merger
   * @param data Data to merge into the response
   * @param out the response to write the merged chunks to
   * @return a single that will emit one item when all chunks have been merged
   */
  private Single<Void> doMerge(MultiMerger merger, Single<StoreCursor> data, WriteStream<Buffer> out) {
    return data
      .map(RxStoreCursor::new)
      .flatMapObservable(RxStoreCursor::toObservable)
      .flatMap(p -> store.rxGetOne(p.getRight())
        .flatMapObservable(crs -> merger.merge(crs, p.getLeft(), out)
          .map(v -> Pair.of(1L, 0L)) // left: count, right: not_accepted
          .onErrorResumeNext(t -> {
            if (t instanceof IllegalStateException) {
              // Chunk cannot be merged. maybe it's a new one that has
              // been added after the merger was initialized. Just
              // ignore it, but emit a warning later
              return Observable.just(Pair.of(0L, 1L));
            }
            return Observable.error(t);
          })
          .doOnTerminate(() -> {
            // don't forget to close the chunk!
            crs.close();
          })), 1 /* write only one chunk concurrently to the output stream */)
      .defaultIfEmpty(Pair.of(0L, 0L))
      .reduce((p1, p2) -> Pair.of(p1.getLeft() + p2.getLeft(),
          p1.getRight() + p2.getRight()))
      .flatMap(p -> {
        long count = p.getLeft();
        long notaccepted = p.getRight();
        if (notaccepted > 0) {
          log.warn("Could not merge " + notaccepted + " chunks "
              + "because the merger did not accept them. Most likely "
              + "these are new chunks that were added while the "
              + "merge was in progress. If this worries you, just "
              + "repeat the request.");
        }
        if (count > 0) {
          merger.finish(out);
          return Observable.just(null);
        } else {
          return Observable.error(new FileNotFoundException("Not Found"));
        }
      }).toSingle().map(v -> null);
  }

  /**
   * Read the context, select the right StoreCursor and set the response header
   * @param context the routing context
   * @return a Single providing a StoreCursor
   */
  protected Single<StoreCursor> prepareCursor(RoutingContext context) {
    HttpServerRequest request = context.request();
    HttpServerResponse response = context.response();
    
    String scroll = request.getParam("scroll");
    String scrollId = request.getParam("scrollId");
    Boolean scrolling = Boolean.parseBoolean(scroll) || scrollId != null;

    String path = getEndpointPath(context);
    String search = request.getParam("search");
    String strSize = request.getParam("size");
    int size = strSize == null ? 100 : new Integer(strSize);

    return Single.<StoreCursor>defer(() -> {
      if (scrolling) {
        if (scrollId == null) {
          return store.rxScroll(search, path, size);
        } else {
          return store.rxScroll(scrollId);
        }
      } else {
        return store.rxGet(search, path);
      }
    }).doOnSuccess(cursor -> {
      if (scrolling) {
        response
          .putHeader("X-Scroll-Id", cursor.getInfo().getScrollId())
          .putHeader("X-Total-Hits", String.valueOf(cursor.getInfo().getTotalHits()))
          .putHeader("X-Hits", String.valueOf(cursor.getInfo().getCurrentHits()));
      }
    });
  }
  
  /**
   * Handles the HTTP GET request for a bunch of chunks
   * @param context the routing context
   */
  protected void onGet(RoutingContext context) {
    HttpServerRequest request = context.request();
    HttpServerResponse response = context.response();

    String path = getEndpointPath(context);
    String search = request.getParam("search");
    String property = request.getParam("property");
    String attribute = request.getParam("attribute");

    if (property != null && attribute != null) {
      response
        .setStatusCode(400)
        .end("You can only get the values of a property or an attribute, but not both");
    } else if (property != null) {
      getPropertyValues(search, path, property, response);
    } else if (attribute != null) {
      getAttributeValues(search, path, attribute, response);
    } else {
      getChunks(context);
    }
  }

  /**
   * Retrieve all chunks matching the specified query and path
   * @param context the routing context
   */
  private void getChunks(RoutingContext context) {
    HttpServerResponse response = context.response();
    Single<StoreCursor> data = prepareCursor(context);
    
    // Our responses must always be chunked because we cannot calculate
    // the exact content-length beforehand. We perform two searches, one to
    // initialize the merger and one to do the actual merge. The problem is
    // that the result set may change between these two searches and so we
    // cannot calculate the content-length just from looking at the result
    // from the first search.
    response.setChunked(true);
    
    // perform two searches: first initialize the merger and then
    // merge all retrieved chunks
    MultiMerger merger = new MultiMerger();
    initializeMerger(merger, data)
      .flatMapSingle(v -> doMerge(merger, data, response))
      .subscribe(v -> {
        response.end();
      }, err -> {
        if (!(err instanceof FileNotFoundException)) {
          log.error("Could not perform query", err);
        }
        fail(response, err);
      });
  }

  /**
   * Get all values for the specified attribute
   * @param search the search query
   * @param path the path
   * @param attribute the name of the attribute
   * @param response the http response
   */
  private void getAttributeValues(String search, String path, String attribute,
      HttpServerResponse response) {
    final Boolean[] first = {true};
    response.setChunked(true);
    response.write("[");

    store.rxGetAttributeValues(search, path, attribute)
      .flatMapObservable(x -> new RxAsyncCursor<>(x).toObservable())
      .subscribe(
        x -> {
          if (first[0]) {
            first[0] = false;
          } else {
            response.write(",");
          }
          response.write("\"" + StringEscapeUtils.escapeJson(x) + "\"");
        },
        err -> fail(response, err),
        () -> response
          .write("]")
          .setStatusCode(200)
          .end());
  }

  /**
   * Get all values for the specified property
   * @param search the search query
   * @param path the path
   * @param property the name of the property
   * @param response the http response
   */
  private void getPropertyValues(String search, String path, String property,
      HttpServerResponse response) {
    final Boolean[] first = {true};
    response.setChunked(true);
    response.write("[");

    store.rxGetPropertyValues(search, path, property)
      .flatMapObservable(x -> new RxAsyncCursor<>(x).toObservable())
      .subscribe(
        x -> {
          if (first[0]) {
            first[0] = false;
          } else {
            response.write(",");
          }
          response.write("\"" + StringEscapeUtils.escapeJson(x) + "\"");
        },
        err -> fail(response, err),
        () -> response
          .write("]")
          .setStatusCode(200)
          .end());
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
        String mimeType = MimeTypeUtils.detect(new File(filepath));
        if (mimeType == null) {
          log.warn("Could not detect file type for " + filepath + ". Using "
            + "application/octet-stream.");
          mimeType = "application/octet-stream";
        }
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

    String layer = getEndpointPath(context);
    String tagsStr = request.getParam("tags");
    String propertiesStr = request.getParam("props");
    String fallbackCRSString = request.getParam("fallbackCRS");

    List<String> tags = tagsStr != null ? Splitter.on(',')
        .trimResults().splitToList(tagsStr) : null;

    Map<String, Object> properties = new HashMap<>();
    if (propertiesStr != null && !propertiesStr.isEmpty()) {
      String regex = "(?<!" + Pattern.quote("\\") + ")" + Pattern.quote(":");
      String[] parts = propertiesStr.split(",");
      for (String part : parts) {
        part = part.trim();
        String[] property = part.split(regex);
        if (property.length != 2) {
          request.response()
            .setStatusCode(400)
            .end("Invalid property syntax: " + part);
          return;
        }
        String key = StringEscapeUtils.unescapeJava(property[0].trim());
        String value = StringEscapeUtils.unescapeJava(property[1].trim());
        properties.put(key, value);
      }
    }

    // get temporary filename
    String incoming = storagePath + "/incoming";
    String filename = new ObjectId().toString();
    String filepath = incoming + "/" + filename;

    String correlationId = UUID.randomUUID().toString();
    long startTime = System.currentTimeMillis();
    this.onReceivingFileStarted(correlationId, layer, startTime);

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
          log.debug("Mime type '" + mimeType + "' is invalid or generic. "
              + "Trying to guess the right type.");
          return detectContentType(filepath).doOnNext(guessedType -> {
            log.debug("Guessed mime type '" + guessedType + "'.");
          });
        }

        return Observable.just(mimeType);
      })
      .subscribe(detectedContentType -> {
        long duration = System.currentTimeMillis() - startTime;
        this.onReceivingFileFinished(correlationId, duration, layer, null);

        // run importer
        JsonObject msg = new JsonObject()
            .put("filename", filename)
            .put("layer", layer)
            .put("contentType", detectedContentType)
            .put("correlationId", correlationId);

        if (tags != null) {
          msg.put("tags", new JsonArray(tags));
        }

        if (!properties.isEmpty()) {
          msg.put("properties", new JsonObject(properties));
        }

        if (fallbackCRSString != null) {
          msg.put("fallbackCRSString", fallbackCRSString);
        }

        request.response()
          .setStatusCode(202) // Accepted
          .putHeader("X-Correlation-Id", correlationId)
          .setStatusMessage("Accepted file - importing in progress")
          .end();

        // run importer
        vertx.eventBus().send(AddressConstants.IMPORTER_IMPORT, msg);
      }, err -> {
        long duration = System.currentTimeMillis() - startTime;
        this.onReceivingFileFinished(correlationId, duration, layer, err);
        fail(request.response(), err);
        err.printStackTrace();
        fs.delete(filepath, ar -> {});
      });
  }

  private void onReceivingFileStarted(String correlationId, String layer, long startTime) {
    log.info(String.format("Receiving file [%s] to layer '%s'", correlationId, layer));

    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "import")
        .put("state", "receive")
        .put("action", "enter")
        .put("correlationId", correlationId)
        .put("timestamp", startTime);

      vertx.eventBus().publish(AddressConstants.ACTIVITIES, msg);
    }
  }

  private void onReceivingFileFinished(String correlationId, long duration,
      String layer, Throwable error) {
    if (error == null) {
      log.info(String.format("Finished receiving file [%s] to layer '%s' after '%d' ms",
          correlationId, layer, duration));
    } else {
      log.error(String.format("Failed receiving file [%s] to layer '%s' after %d ms",
          correlationId, layer, duration), error);
    }

    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "import")
        .put("state", "receive")
        .put("action", "leave")
        .put("correlationId", correlationId)
        .put("duration", duration);

      if (error != null) {
        msg.put("error", error.getMessage());
      }

      vertx.eventBus().publish(AddressConstants.ACTIVITIES, msg);
    }
  }

  /**
   * Handles the HTTP DELETE request
   * @param context the routing context
   */
  private void onDelete(RoutingContext context) {
    String path = getEndpointPath(context);
    HttpServerResponse response = context.response();
    HttpServerRequest request = context.request();
    String search = request.getParam("search");
    String properties = request.getParam("properties");
    String tags = request.getParam("tags");

    if (properties != null && tags != null) {
      response
        .setStatusCode(400)
        .end("You can only delete properties or tags, but not both");
    } else if (properties != null) {
      removeProperties(search, path, properties, response);
    } else if (tags != null) {
      removeTags(search, path, tags, response);
    } else {
      deleteChunks(search, path, response);
    }
  }

  /**
   * Remove properties
   * @param search the search query
   * @param path the path
   * @param properties the properties to remove
   * @param response the http response
   */
  private void removeProperties(String search, String path, String properties,
    HttpServerResponse response) {
    List<String> list = Arrays.asList(properties.split(","));
    store.removeProperties(search, path, list, ar -> {
      if (ar.succeeded()) {
        response
          .setStatusCode(204)
          .end();
      } else {
        fail(response, ar.cause());
      }
    });
  }

  /**
   * Remove tags
   * @param search the search query
   * @param path the path
   * @param tags the tags to remove
   * @param response the http response
   */
  private void removeTags(String search, String path, String tags,
    HttpServerResponse response) {
    if (tags != null) {
      List<String> list = Arrays.asList(tags.split(","));
      store.removeTags(search, path, list, ar -> {
        if (ar.succeeded()) {
          response
            .setStatusCode(204)
            .end();
        } else {
          fail(response, ar.cause());
        }
      });
    }
  }

  /**
   * Delete chunks
   * @param search the search query
   * @param path the path
   * @param response the http response
   */
  private void deleteChunks(String search, String path, HttpServerResponse response) {
    store.rxDelete(search, path)
      .subscribe(v -> {
        response
          .setStatusCode(204)
          .end();
      }, err -> {
        log.error("Could not delete chunks", err);
        fail(response, err);
      });
  }

  /**
   * Handles the HTTP PUT request
   * @param context the routing context
   */
  private void onPut(RoutingContext context) {
    String path = getEndpointPath(context);
    HttpServerResponse response = context.response();
    HttpServerRequest request = context.request();
    String search = request.getParam("search");
    String properties = request.getParam("properties");
    String tags = request.getParam("tags");

    if (properties != null || tags != null) {
      Single<Void> single = Single.just(null);

      if (properties != null) {
        single = single.flatMap(v -> setProperties(search, path, properties));
      }

      if (tags != null) {
        single = single.flatMap(v -> appendTags(search, path, tags));
      }

      single.subscribe(
        v -> response
          .setStatusCode(204)
          .end(),
        err -> fail(response, err)
      );
    } else {
      response
        .setStatusCode(405)
        .end("Only properties and tags can be modified");
    }
  }

  /**
   * Set properties
   * @param search the search query
   * @param path the path
   * @param properties the properties to set
   * @return a Single with completes when the properties are set
   */
  private Single<Void> setProperties(String search, String path, String properties) {
    return Single.just(properties)
      .map(x -> x.split(","))
      .map(Arrays::asList)
      .flatMap(x -> {
        try {
          return Single.just(parseProperties(x));
        } catch (ServerAPIException e) {
          return Single.error(e);
        }
      })
      .flatMap(map -> store.rxSetProperties(search, path, map));
  }

  /**
   * Append tags
   * @param search the search query
   * @param path the path
   * @param tags the tags to append
   * @return a Single with completes when the tags are appended
   */
  private Single<Void> appendTags(String search, String path, String tags) {
    return Single.just(tags)
      .map(x -> x.split(","))
      .map(Arrays::asList)
      .flatMap(tagList -> store.rxAppendTags(search, path, tagList));
  }

  /**
   * Parse list of properties in the form key:value
   * @param updates the list of properties
   * @return a json object with the property keys as object keys and the property
   * values as corresponding object values
   * @throws ServerAPIException if the syntax is not valid
   */
  private static Map<String, String> parseProperties(List<String> updates)
    throws ServerAPIException {
    Map<String, String> props = new HashMap<>();
    String regex = "(?<!" + Pattern.quote("\\") + ")" + Pattern.quote(":");

    for (String part : updates) {
      part = part.trim();
      String[] property = part.split(regex);
      if (property.length != 2) {
        throw new ServerAPIException(
          ServerAPIException.INVALID_PROPERTY_SYNTAX_ERROR,
          "Invalid property syntax: " + part);
      }
      String key = StringEscapeUtils.unescapeJava(property[0].trim());
      String value = StringEscapeUtils.unescapeJava(property[1].trim());
      props.put(key, value);
    }

    return props;
  }
}
