package io.georocket.http;

import com.google.common.base.Splitter;
import io.georocket.ServerAPIException;
import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.output.Merger;
import io.georocket.output.MultiMerger;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.DeleteMeta;
import io.georocket.storage.RxAsyncCursor;
import io.georocket.storage.RxStore;
import io.georocket.storage.RxStoreCursor;
import io.georocket.storage.StoreCursor;
import io.georocket.storage.StoreFactory;
import io.georocket.tasks.ReceivingTask;
import io.georocket.tasks.TaskError;
import io.georocket.util.HttpException;
import io.georocket.util.MimeTypeUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.bson.types.ObjectId;
import rx.Completable;
import rx.Observable;
import rx.Single;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.georocket.http.Endpoint.fail;
import static io.georocket.http.Endpoint.getEndpointPath;

/**
 * An HTTP endpoint handling requests related to the GeoRocket data store
 * @author Michel Kraemer
 */
public class StoreEndpoint implements Endpoint {
  private static Logger log = LoggerFactory.getLogger(StoreEndpoint.class);

  /**
   * <p>Name of the HTTP trailer that tells the client how many chunks could not
   * be merged. Possible reasons for unmerged chunks are:</p>
   * <ul>
   * <li>New chunks were added to the store while merging was in progress.</li>
   * <li>Optimistic merging was enabled and some chunks could not be merged.</li>
   * </ul>
   * <p>The trailer will contain the number of chunks that could not be
   * merged. The client can decide whether to repeat the request to fetch
   * the missing chunks (e.g. with optimistic merging disabled) or not.</p>
   */
  private static String TRAILER_UNMERGED_CHUNKS = "GeoRocket-Unmerged-Chunks";
  
  private Vertx vertx;
  private RxStore store;
  private String storagePath;

  @Override
  public String getMountPoint() {
    return "/store";
  }

  @Override
  public Router createRouter(Vertx vertx) {
    this.vertx = vertx;

    store = new RxStore(StoreFactory.createStore(vertx));
    storagePath = vertx.getOrCreateContext().config()
      .getString(ConfigConstants.STORAGE_FILE_PATH);

    Router router = Router.router(vertx);
    router.get("/*").handler(this::onGet);
    router.put("/*").handler(this::onPut);
    router.post("/*").handler(this::onPost);
    router.delete("/*").handler(this::onDelete);

    return router;
  }

  /**
   * Create a new merger
   * @param ctx routing context
   * @param optimisticMerging {@code true} if optimistic merging is enabled
   * @return the new merger instance
   */
  protected Merger<ChunkMeta> createMerger(RoutingContext ctx,
      boolean optimisticMerging) {
    return new MultiMerger(optimisticMerging);
  }

  /**
   * Initialize the given merger. Perform a search using the given search string
   * and pass all chunk metadata retrieved to the merger.
   * @param merger the merger to initialize
   * @param data data to use for the initialization
   * @return a Completable that will complete when the merger has been
   * initialized with all results
   */
  private Completable initializeMerger(Merger<ChunkMeta> merger, Single<StoreCursor> data) {
    return data
      .map(RxStoreCursor::new)
      .flatMapObservable(RxStoreCursor::toObservable)
      .map(Pair::getLeft)
      .flatMapCompletable(merger::init)
      .toCompletable();
  }
  
  /**
   * Perform a search and merge all retrieved chunks using the given merger
   * @param merger the merger
   * @param data Data to merge into the response
   * @param out the response to write the merged chunks to
   * @param trailersAllowed {@code true} if the HTTP client accepts trailers
   * @return a single that will emit one item when all chunks have been merged
   */
  private Completable doMerge(Merger<ChunkMeta> merger, Single<StoreCursor> data,
      HttpServerResponse out, boolean trailersAllowed) {
    return data
      .map(RxStoreCursor::new)
      .flatMapObservable(RxStoreCursor::toObservable)
      .flatMapSingle(p -> store.rxGetOne(p.getRight())
        .flatMap(crs -> merger.merge(crs, p.getLeft(), out)
          .toSingleDefault(Pair.of(1L, 0L)) // left: count, right: not_accepted
          .onErrorResumeNext(t -> {
            if (t instanceof IllegalStateException) {
              // Chunk cannot be merged. maybe it's a new one that has
              // been added after the merger was initialized. Just
              // ignore it, but emit a warning later
              return Single.just(Pair.of(0L, 1L));
            }
            return Single.error(t);
          })
          .doAfterTerminate(() -> {
            // don't forget to close the chunk!
            crs.close();
          })), false, 1 /* write only one chunk concurrently to the output stream */)
      .defaultIfEmpty(Pair.of(0L, 0L))
      .reduce((p1, p2) -> Pair.of(p1.getLeft() + p2.getLeft(),
          p1.getRight() + p2.getRight()))
      .flatMapCompletable(p -> {
        long count = p.getLeft();
        long notaccepted = p.getRight();
        if (notaccepted > 0) {
          log.warn("Could not merge " + notaccepted + " chunks "
              + "because the merger did not accept them. Most likely "
              + "these are new chunks that were added while "
              + "merging was in progress or those that were ignored "
              + "during optimistic merging. If this worries you, "
              + "just repeat the request.");
        }
        if (trailersAllowed) {
          out.putTrailer(TRAILER_UNMERGED_CHUNKS, String.valueOf(notaccepted));
        }
        if (count > 0) {
          merger.finish(out);
          return Completable.complete();
        } else {
          return Completable.error(new FileNotFoundException("Not Found"));
        }
      })
      .toCompletable();
  }

  /**
   * Read the context, select the right StoreCursor and set the response header.
   * For the first call to this method within a request the <code>preview</code>
   * parameter must equal <code>true</code> and for the second one (if there
   * is one) the parameter must equal <code>false</code>. This method must not
   * be called more than two times within a request.
   * @param context the routing context
   * @param preview <code>true</code> if the cursor should be used to generate
   * a preview or to initialize the merger
   * @return a Single providing a StoreCursor
   */
  protected Single<StoreCursor> prepareCursor(RoutingContext context, boolean preview) {
    HttpServerRequest request = context.request();
    HttpServerResponse response = context.response();
    
    String scroll = request.getParam("scroll");
    String scrollIdParam = request.getParam("scrollId");
    boolean scrolling = BooleanUtils.toBoolean(scroll) || scrollIdParam != null;

    // if we're generating a preview, split the scrollId param at ':' and
    // use the first part. Otherwise use the second one.
    String scrollId;
    if (scrollIdParam != null) {
      String[] scrollIdParts = scrollIdParam.split(":");
      if (preview) {
        scrollId = scrollIdParts[0];
      } else {
        scrollId = scrollIdParts[1];
      }
    } else {
      scrollId = null;
    }

    String path = getEndpointPath(context);
    String search = request.getParam("search");
    String strSize = request.getParam("size");
    int size = strSize == null ? 100 : Integer.parseInt(strSize);

    return Single.defer(() -> {
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
        // create a new scrollId consisting of the one used for the preview and
        // the other one used for the real query
        String newScrollId = cursor.getInfo().getScrollId();
        if (!preview) {
          String oldScrollId = response.headers().get("X-Scroll-Id");
          if (isOptimisticMerging(request)) {
            oldScrollId = "0";
          } else if (oldScrollId == null) {
            throw new IllegalStateException("A preview must be generated " +
              "before the actual request can be made. This usually happens " +
              "when the merger is initialized.");
          }
          newScrollId = oldScrollId + ":" + newScrollId;
        }
        response
          .putHeader("X-Scroll-Id", newScrollId)
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
   * Checks if optimistic merging is enabled
   * @param request the HTTP request
   * @return {@code true} if optimistic is enabled, {@code false} otherwise
   */
  private boolean isOptimisticMerging(HttpServerRequest request) {
    return BooleanUtils.toBoolean(request.getParam("optimisticMerging"));
  }

  /**
   * Checks if the client accepts an HTTP trailer
   * @param request the HTTP request
   * @return {@code true} if the client accepts a trailer, {@code false} otherwise
   */
  private boolean isTrailerAccepted(HttpServerRequest request) {
    String te = request.getHeader("TE");
    return (te != null && te.toLowerCase().contains("trailers"));
  }

  /**
   * Retrieve all chunks matching the specified query and path
   * @param context the routing context
   */
  private void getChunks(RoutingContext context) {
    HttpServerRequest request = context.request();
    HttpServerResponse response = context.response();

    // Our responses must always be chunked because we cannot calculate
    // the exact content-length beforehand. We perform two searches, one to
    // initialize the merger and one to do the actual merge. The problem is
    // that the result set may change between these two searches and so we
    // cannot calculate the content-length just from looking at the result
    // from the first search.
    response.setChunked(true);

    boolean optimisticMerging = isOptimisticMerging(request);
    boolean isTrailerAccepted = isTrailerAccepted(request);

    if (isTrailerAccepted) {
      response.putHeader("Trailer", TRAILER_UNMERGED_CHUNKS);
    }

    // perform two searches: first initialize the merger and then
    // merge all retrieved chunks
    Merger<ChunkMeta> merger = createMerger(context, optimisticMerging);

    Completable c;
    if (optimisticMerging) {
      // skip initialization if optimistic merging is enabled
      c = Completable.complete();
    } else {
      c = initializeMerger(merger, prepareCursor(context, true));
    }

    c.andThen(Completable.defer(() -> doMerge(merger, prepareCursor(context, false),
        response, isTrailerAccepted)))
      .subscribe(response::end, err -> {
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
   * @param gzip true if the file is compressed with GZIP
   * @return an observable emitting either the detected content type or an error
   * if the content type could not be detected or the file could not be read
   */
  private Observable<String> detectContentType(String filepath, boolean gzip) {
    ObservableFuture<String> result = RxHelper.observableFuture();
    Handler<AsyncResult<String>> resultHandler = result.toHandler();
    
    vertx.<String>executeBlocking(f -> {
      try {
        String mimeType = MimeTypeUtils.detect(new File(filepath), gzip);
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

    List<String> tags = StringUtils.isNotEmpty(tagsStr) ? Splitter.on(',')
        .trimResults().splitToList(tagsStr) : null;

    Map<String, Object> properties = new HashMap<>();
    if (StringUtils.isNotEmpty(propertiesStr)) {
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

    String correlationId = new ObjectId().toString();
    long startTime = System.currentTimeMillis();
    this.onReceivingFileStarted(correlationId, startTime);

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
        request.endHandler(v -> f.close(v2 -> pumpHandler.handle(Future.succeededFuture())));
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

        String contentEncoding = request.getHeader("Content-Encoding");
        boolean gzip = false;
        if ("gzip".equals(contentEncoding)) {
          gzip = true;
        }

        // detect content type of file to import
        if (mimeType == null || mimeType.trim().isEmpty() ||
            mimeType.equals("application/octet-stream") ||
            mimeType.equals("application/x-www-form-urlencoded")) {
          // fallback: if the client has not sent a Content-Type or if it's
          // a generic one, then try to guess it
          log.debug("Mime type '" + mimeType + "' is invalid or generic. "
              + "Trying to guess the right type.");
          return detectContentType(filepath, gzip).doOnNext(guessedType -> {
            log.info("Guessed mime type '" + guessedType + "'.");
          });
        }

        return Observable.just(mimeType);
      })
      .subscribe(detectedContentType -> {
        long duration = System.currentTimeMillis() - startTime;
        this.onReceivingFileFinished(correlationId, duration, null);

        String contentEncoding = request.getHeader("Content-Encoding");

        // run importer
        JsonObject msg = new JsonObject()
            .put("filename", filename)
            .put("layer", layer)
            .put("contentType", detectedContentType)
            .put("correlationId", correlationId)
            .put("contentEncoding", contentEncoding);

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
        this.onReceivingFileFinished(correlationId, duration, err);
        fail(request.response(), err);
        err.printStackTrace();
        fs.delete(filepath, ar -> {});
      });
  }

  private void onReceivingFileStarted(String correlationId, long startTime) {
    log.info("Receiving file [" + correlationId + "]");
    ReceivingTask task = new ReceivingTask(correlationId);
    task.setStartTime(Instant.now());
    vertx.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(task));
  }

  private void onReceivingFileFinished(String correlationId, long duration,
      Throwable error) {
    if (error == null) {
      log.info(String.format("Finished receiving file [%s] after %d ms",
          correlationId, duration));
    } else {
      log.error(String.format("Failed receiving file [%s] after %d ms",
          correlationId, duration), error);
    }
    ReceivingTask task = new ReceivingTask(correlationId);
    task.setEndTime(Instant.now());
    if (error != null) {
      task.addError(new TaskError(error));
    }
    vertx.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(task));
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

    if (StringUtils.isNotEmpty(properties) && StringUtils.isNotEmpty(tags)) {
      response
        .setStatusCode(400)
        .end("You can only delete properties or tags, but not both");
    } else if (StringUtils.isNotEmpty(properties)) {
      removeProperties(search, path, properties, response);
    } else if (StringUtils.isNotEmpty(tags)) {
      removeTags(search, path, tags, response);
    } else {
      String strAsync = request.getParam("async");
      boolean async = BooleanUtils.toBoolean(strAsync);
      deleteChunks(search, path, async, response);
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
   * @param async {@code true} if the operation should be performed asynchronously
   * @param response the http response
   */
  private void deleteChunks(String search, String path, boolean async,
      HttpServerResponse response) {
    String correlationId = new ObjectId().toString();
    DeleteMeta deleteMeta = new DeleteMeta(correlationId);
    if (async) {
      store.rxDelete(search, path, deleteMeta)
        .subscribe(() -> {}, err -> log.error("Could not delete chunks", err));
      response
        .setStatusCode(202) // Accepted
        .putHeader("X-Correlation-Id", correlationId)
        .end();
    } else {
      store.rxDelete(search, path, deleteMeta)
        .subscribe(() -> {
          response
            .setStatusCode(204) // No Content
            .putHeader("X-Correlation-Id", correlationId)
            .end();
        }, err -> {
          log.error("Could not delete chunks", err);
          fail(response, err);
        });
    }
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

    if (StringUtils.isNotEmpty(properties) || StringUtils.isNotEmpty(tags)) {
      Completable single = Completable.complete();

      if (StringUtils.isNotEmpty(properties)) {
        single = setProperties(search, path, properties);
      }

      if (StringUtils.isNotEmpty(tags)) {
        single = appendTags(search, path, tags);
      }

      single.subscribe(
        () -> response
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
   * @return a Completable that completes when the properties have been set
   */
  private Completable setProperties(String search, String path, String properties) {
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
      .flatMapCompletable(map -> store.rxSetProperties(search, path, map));
  }

  /**
   * Append tags
   * @param search the search query
   * @param path the path
   * @param tags the tags to append
   * @return a Completable that completes when the tags have been appended
   */
  private Completable appendTags(String search, String path, String tags) {
    return Single.just(tags)
      .map(x -> x.split(","))
      .map(Arrays::asList)
      .flatMapCompletable(tagList -> store.rxAppendTags(search, path, tagList));
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
