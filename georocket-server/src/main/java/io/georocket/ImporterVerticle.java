package io.georocket;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.xml.XMLCRSIndexer;
import io.georocket.input.Splitter.Result;
import io.georocket.input.geojson.GeoJsonSplitter;
import io.georocket.input.xml.FirstLevelSplitter;
import io.georocket.input.xml.XMLSplitter;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.IndexMeta;
import io.georocket.storage.RxStore;
import io.georocket.storage.StoreFactory;
import io.georocket.tasks.ImportingTask;
import io.georocket.tasks.TaskError;
import io.georocket.util.JsonParserTransformer;
import io.georocket.util.RxUtils;
import io.georocket.util.StringWindow;
import io.georocket.util.UTF8BomFilter;
import io.georocket.util.Window;
import io.georocket.util.XMLParserTransformer;
import io.georocket.util.io.RxGzipReadStream;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.file.AsyncFile;
import io.vertx.rxjava.core.file.FileSystem;
import io.vertx.rxjava.core.streams.ReadStream;
import rx.Completable;
import rx.Observable;
import rx.Single;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.georocket.util.MimeTypeUtils.belongsTo;

/**
 * Imports file in the background
 * @author Michel Kraemer
 */
public class ImporterVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(ImporterVerticle.class);
  
  private static final int MAX_RETRIES = 5;
  private static final int RETRY_INTERVAL = 1000;
  private static final int MAX_PARALLEL_IMPORTS = 1;
  
  protected RxStore store;
  private String incoming;
  private boolean paused;
  private Set<AsyncFile> filesBeingImported = new HashSet<>();

  @Override
  public void start() {
    log.info("Launching importer ...");

    store = new RxStore(StoreFactory.createStore(getVertx()));
    String storagePath = config().getString(ConfigConstants.STORAGE_FILE_PATH);
    incoming = storagePath + "/incoming";
    
    vertx.eventBus().<JsonObject>localConsumer(AddressConstants.IMPORTER_IMPORT)
      .toObservable()
      .onBackpressureBuffer() // unlimited buffer
      .flatMapCompletable(msg -> {
        // call onImport() but ignore errors. onImport() will handle errors for us.
        return onImport(msg).onErrorComplete();
      }, false, MAX_PARALLEL_IMPORTS)
      .subscribe(v -> {
        // ignore
      }, err -> {
        // This is bad. It will unsubscribe the consumer from the eventbus!
        // Should never happen anyhow. If it does, something else has
        // completely gone wrong.
        log.fatal("Could not import file", err);
      });

    vertx.eventBus().localConsumer(AddressConstants.IMPORTER_PAUSE, this::onPause);
  }

  /**
   * Receives a name of a file to import
   * @param msg the event bus message containing the filename
   * @return a Completable that will complete when the file has been imported
   */
  protected Completable onImport(Message<JsonObject> msg) {
    JsonObject body = msg.body();
    String filename = body.getString("filename");
    String filepath = incoming + "/" + filename;
    String layer = body.getString("layer", "/");
    String contentType = body.getString("contentType");
    String correlationId = body.getString("correlationId");
    String fallbackCRSString = body.getString("fallbackCRSString");
    String contentEncoding = body.getString("contentEncoding");

    // get tags
    JsonArray tagsArr = body.getJsonArray("tags");
    List<String> tags = tagsArr != null ? tagsArr.stream().flatMap(o -> o != null ?
        Stream.of(o.toString()) : Stream.of()).collect(Collectors.toList()) : null;

    // get properties
    JsonObject propertiesObj = body.getJsonObject("properties");
    Map<String, Object> properties = propertiesObj != null ? propertiesObj.getMap() : null;

    // generate timestamp for this import
    long timestamp = System.currentTimeMillis();

    log.info("Importing [" + correlationId + "] to layer '" + layer + "'");

    FileSystem fs = vertx.fileSystem();
    OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
    return fs.rxOpen(filepath, openOptions)
      .flatMap(f -> {
        filesBeingImported.add(f);
        return importFile(contentType, f, correlationId, filename, timestamp,
            layer, tags, properties, fallbackCRSString, contentEncoding)
          .doAfterTerminate(() -> {
            // delete file from 'incoming' folder
            log.debug("Deleting " + filepath + " from incoming folder");
            filesBeingImported.remove(f);
            f.rxClose()
              .flatMap(v -> fs.rxDelete(filepath))
              .subscribe(v -> {}, err -> {
                log.error("Could not delete file from 'incoming' folder", err);
              });
          });
      })
      .doOnSuccess(chunkCount -> {
        long duration = System.currentTimeMillis() - timestamp;
        log.info("Finished importing [" + correlationId + "] with " + chunkCount +
            " chunks to layer '" + layer + "' after " + duration + " ms");
      })
      .doOnError(err -> {
        long duration = System.currentTimeMillis() - timestamp;
        log.error("Failed to import [" + correlationId + "] to layer '" +
            layer + "' after " + duration + " ms", err);
      })
      .toCompletable();
  }

  /**
   * Import a file from the given read stream into the store. Inspect the file's
   * content type and forward to the correct import method.
   * @param contentType the file's content type
   * @param f the file to import
   * @param correlationId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param timestamp denotes when the import process has started
   * @param layer the layer where the file should be stored (may be null)
   * @param tags the list of tags to attach to the file (may be null)
   * @param properties the map of properties to attach to the file (may be null)
   * @param fallbackCRSString the CRS which should be used if the imported
   * file does not specify one (may be <code>null</code>)
   * @param contentEncoding the content encoding of the file to be
   * imported (e.g. "gzip"). May be <code>null</code>.
   * @return a single that will emit with the number if chunks imported
   * when the file has been imported
   */
  protected Single<Integer> importFile(String contentType, ReadStream<Buffer> f,
      String correlationId, String filename, long timestamp, String layer,
      List<String> tags, Map<String, Object> properties, String fallbackCRSString,
      String contentEncoding) {
    if ("gzip".equals(contentEncoding)) {
      log.debug("Importing file compressed with GZIP");
      f = new RxGzipReadStream(f);
    } else if (contentEncoding != null && !contentEncoding.isEmpty()) {
      log.warn("Unknown content encoding: `" + contentEncoding + "'. Trying anyway.");
    }

    // let the task verticle know that we're now importing
    ImportingTask startTask = new ImportingTask(correlationId);
    startTask.setStartTime(Instant.now());
    vertx.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(startTask));

    Observable<Integer> result;
    if (belongsTo(contentType, "application", "xml") ||
        belongsTo(contentType, "text", "xml")) {
      result = importXML(f, correlationId, filename, timestamp, layer, tags,
        properties, fallbackCRSString);
    } else if (belongsTo(contentType, "application", "json")) {
      result = importJSON(f, correlationId, filename, timestamp, layer, tags, properties);
    } else {
      result = Observable.error(new NoStackTraceThrowable(String.format(
          "Received an unexpected content type '%s' while trying to import "
          + "file '%s'", contentType, filename)));
    }

    Consumer<Throwable> onFinish = t -> {
      // let the task verticle know that the import process has finished
      ImportingTask endTask = new ImportingTask(correlationId);
      endTask.setEndTime(Instant.now());
      if (t != null) {
        endTask.addError(new TaskError(t));
      }
      vertx.eventBus().publish(AddressConstants.TASK_INC,
          JsonObject.mapFrom(endTask));
    };

    return result.window(100)
      .flatMap(Observable::count)
      .doOnNext(n -> {
        // let the task verticle know that we imported n chunks
        ImportingTask currentTask = new ImportingTask(correlationId);
        currentTask.setImportedChunks(n);
        vertx.eventBus().publish(AddressConstants.TASK_INC,
            JsonObject.mapFrom(currentTask));
      })
      .reduce(0, (a, b) -> a + b)
      .toSingle()
      .doOnError(onFinish::accept)
      .doOnSuccess(i -> onFinish.accept(null));
  }

  /**
   * Imports an XML file from the given input stream into the store
   * @param f the XML file to read
   * @param correlationId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param timestamp denotes when the import process has started
   * @param layer the layer where the file should be stored (may be null)
   * @param tags the list of tags to attach to the file (may be null)
   * @param properties the map of properties to attach to the file (may be null)
   * @param fallbackCRSString the CRS which should be used if the imported
   * file does not specify one (may be <code>null</code>)
   * @return an observable that will emit the number 1 when a chunk has been imported
   */
  protected Observable<Integer> importXML(ReadStream<Buffer> f, String correlationId,
      String filename, long timestamp, String layer, List<String> tags,
      Map<String, Object> properties, String fallbackCRSString) {
    UTF8BomFilter bomFilter = new UTF8BomFilter();
    Window window = new Window();
    XMLSplitter splitter = new FirstLevelSplitter(window);
    AtomicInteger processing = new AtomicInteger(0);
    XMLCRSIndexer crsIndexer = new XMLCRSIndexer();
    return f.toObservable()
        .map(Buffer::getDelegate)
        .map(bomFilter::filter)
        .doOnNext(window::append)
        .compose(new XMLParserTransformer())
        .doOnNext(e -> {
          // save the first CRS found in the file
          if (crsIndexer.getCRS() == null) {
            crsIndexer.onEvent(e);
          }
        })
        .flatMap(splitter::onEventObservable)
        .flatMapSingle(result -> {
          String crsString = fallbackCRSString;
          if (crsIndexer.getCRS() != null) {
            crsString = crsIndexer.getCRS();
          }
          IndexMeta indexMeta = new IndexMeta(correlationId, filename,
              timestamp, tags, properties, crsString);
          return addToStoreWithPause(result, layer, indexMeta, f, processing)
              .toSingleDefault(1);
        });
  }
  
  /**
   * Imports a JSON file from the given input stream into the store
   * @param f the JSON file to read
   * @param correlationId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param timestamp denotes when the import process has started
   * @param layer the layer where the file should be stored (may be null)
   * @param tags the list of tags to attach to the file (may be null)
   * @param properties the map of properties to attach to the file (may be null)
   * @return an observable that will emit the number 1 when a chunk has been imported
   */
  protected Observable<Integer> importJSON(ReadStream<Buffer> f, String correlationId,
      String filename, long timestamp, String layer, List<String> tags, Map<String, Object> properties) {
    UTF8BomFilter bomFilter = new UTF8BomFilter();
    StringWindow window = new StringWindow();
    GeoJsonSplitter splitter = new GeoJsonSplitter(window);
    AtomicInteger processing = new AtomicInteger(0);
    return f.toObservable()
        .map(Buffer::getDelegate)
        .map(bomFilter::filter)
        .doOnNext(window::append)
        .compose(new JsonParserTransformer())
        .flatMap(splitter::onEventObservable)
        .flatMapSingle(result -> {
          IndexMeta indexMeta = new IndexMeta(correlationId, filename,
              timestamp, tags, properties, null);
          return addToStoreWithPause(result, layer, indexMeta, f, processing)
              .toSingleDefault(1);
        });
  }

  /**
   * Handle a pause message
   * @param msg the message
   */
  private void onPause(Message<Boolean> msg) {
    Boolean paused = msg.body();
    if (paused == null || !paused) {
      if (this.paused) {
        log.info("Resuming import");
        this.paused = false;
        for (AsyncFile f : filesBeingImported) {
          f.resume();
        }
      }
    } else {
      if (!this.paused) {
        log.info("Pausing import");
        this.paused = true;
        for (AsyncFile f : filesBeingImported) {
          f.pause();
        }
      }
    }
  }

  /**
   * Add a chunk to the store. Pause the given read stream before adding and
   * increase the given counter. Decrease the counter after the chunk has been
   * written and only resume the read stream if the counter is <code>0</code>.
   * This is necessary because the writing to the store may take longer than
   * reading. We need to pause reading so the store is not overloaded (i.e.
   * we handle back-pressure here).
   * @param chunk the chunk to write
   * @param layer the layer the chunk should be added to (may be null)
   * @param indexMeta metadata specifying how the chunk should be indexed
   * @param f the read stream to pause while writing
   * @param processing an AtomicInteger keeping the number of chunks currently
   * being written (should be initialized to <code>0</code> the first time this
   * method is called)
   * @return a Completable that will complete when the operation has finished
   */
  private Completable addToStoreWithPause(Result<? extends ChunkMeta> chunk,
      String layer, IndexMeta indexMeta, ReadStream<Buffer> f, AtomicInteger processing) {
    // pause stream while chunk is being written
    f.pause();
    
    // count number of chunks being written
    processing.incrementAndGet();

    return addToStore(chunk.getChunk(), chunk.getMeta(), layer, indexMeta)
        .doOnCompleted(() -> {
          // resume stream only after all chunks from the current
          // buffer have been stored
          if (processing.decrementAndGet() == 0 && !paused) {
            f.resume();
          }
        });
  }
  
  /**
   * Add a chunk to the store. Retry operation several times before failing.
   * @param chunk the chunk to add
   * @param meta the chunk's metadata
   * @param layer the layer the chunk should be added to (may be null)
   * @param indexMeta metadata specifying how the chunk should be indexed
   * @return a Completable that will complete when the operation has finished
   */
  protected Completable addToStore(String chunk, ChunkMeta meta,
      String layer, IndexMeta indexMeta) {
    return Completable.defer(() -> store.rxAdd(chunk, meta, layer, indexMeta))
        .retryWhen(RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log));
  }
}
