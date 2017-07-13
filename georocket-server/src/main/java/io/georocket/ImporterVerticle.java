package io.georocket;

import static io.georocket.util.MimeTypeUtils.belongsTo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import io.georocket.util.JsonParserOperator;
import io.georocket.util.RxUtils;
import io.georocket.util.StringWindow;
import io.georocket.util.Window;
import io.georocket.util.XMLParserOperator;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.file.FileSystem;
import io.vertx.rxjava.core.streams.ReadStream;
import rx.Single;

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

  /**
   * True if the importer should report activities to the Vert.x event bus
   */
  private boolean reportActivities;
  
  @Override
  public void start() {
    log.info("Launching importer ...");
    reportActivities = config().getBoolean(ConfigConstants.REPORT_ACTIVITIES, false);
    
    store = new RxStore(StoreFactory.createStore(getVertx()));
    String storagePath = config().getString(ConfigConstants.STORAGE_FILE_PATH);
    incoming = storagePath + "/incoming";
    
    vertx.eventBus().<JsonObject>localConsumer(AddressConstants.IMPORTER_IMPORT)
      .toObservable()
      .onBackpressureBuffer() // unlimited buffer
      .flatMapSingle(msg -> {
        // call onImport() but ignore errors. onImport() will handle errors for us.
        return onImport(msg).onErrorReturn(err -> null);
      }, false, MAX_PARALLEL_IMPORTS)
      .subscribe(v -> {
        // ignore
      }, err -> {
        // This is bad. It will unsubscribe the consumer from the eventbus!
        // Should never happen anyhow. If it does, something else has
        // completely gone wrong.
        log.fatal("Could not import file", err);
      });
  }
  
  /**
   * Receives a name of a file to import
   * @param msg the event bus message containing the filename
   * @return a single that will emit an item when the file has
   * been imported
   */
  protected Single<Void> onImport(Message<JsonObject> msg) {
    JsonObject body = msg.body();
    String filename = body.getString("filename");
    String filepath = incoming + "/" + filename;
    String layer = body.getString("layer", "/");
    String contentType = body.getString("contentType");
    String correlationId = body.getString("correlationId");
    String fallbackCRSString = body.getString("fallbackCRSString");

    // get tags
    JsonArray tagsArr = body.getJsonArray("tags");
    List<String> tags = tagsArr != null ? tagsArr.stream().flatMap(o -> o != null ?
        Stream.of(o.toString()) : Stream.of()).collect(Collectors.toList()) : null;

    // get properties
    JsonObject propertiesObj = body.getJsonObject("properties");
    Map<String, Object> properties = propertiesObj != null ? propertiesObj.getMap() : null;

    // generate timestamp for this import
    long timestamp = System.currentTimeMillis();

    onImportingStarted(correlationId, filepath, contentType, layer, tagsArr, timestamp);

    FileSystem fs = vertx.fileSystem();
    OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
    return fs.rxOpen(filepath, openOptions)
      .flatMap(f -> importFile(contentType, f, correlationId, filename, timestamp,
        layer, tags, properties, fallbackCRSString)
      .doAfterTerminate(() -> {
        // delete file from 'incoming' folder
        log.info("Deleting " + filepath + " from incoming folder");
        f.rxClose()
          .flatMap(v -> fs.rxDelete(filepath))
          .subscribe(v -> {}, err -> {
            log.error("Could not delete file from 'incoming' folder", err);
          });
      }))
      .doOnSuccess(chunkCount -> {
        onImportingFinished(correlationId, filepath, contentType, layer, chunkCount,
           System.currentTimeMillis() - timestamp, null);
      })
      .doOnError(err -> {
        onImportingFinished(correlationId, filepath, contentType, layer, null,
            System.currentTimeMillis() - timestamp, err);
      })
      .map(count -> null);
  }

  /**
   * Will be called before the importer starts importing chunks
   * @param correlationId the id for this import
   * @param filepath the filepath of the file containing the chunks
   * @param mimeType The mimeType of the imported file
   * @param layer the layer where to import the chunks
   * @param tags all tags for the current chunks
   * @param timestamp the time when the importer has started importing
   */
  private void onImportingStarted(String correlationId, String filepath,
      String mimeType, String layer, JsonArray tags, long timestamp) {
    log.info(String.format("Importing [%s] '%s' to layer '%s' started at '%d'",
        correlationId, filepath, layer, timestamp));

    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "import")
        .put("state", "store")
        .put("owner", deploymentID())
        .put("action", "enter")
        .put("correlationId", correlationId)
        .put("timestamp", timestamp)
        .put("mimeType", mimeType)
        .put("tags", tags)
        .put("layer", layer);
      vertx.eventBus().publish(AddressConstants.ACTIVITIES, msg);
    }
  }

  /**
   * Will be called after the importer has finished importing chunks
   * @param correlationId the id for this import
   * @param filepath the filepath of the file containing the chunks
   * @param mimeType The mimeType of the imported file
   * @param layer the layer where to import the chunks
   * @param chunkCount the number of chunks that have been imported
   * @param duration the time it took to import the chunks
   * @param error an error if the process has failed
   */
  private void onImportingFinished(String correlationId, String filepath,
      String mimeType, String layer, Integer chunkCount, long duration, Throwable error) {
    if (error == null) {
      log.info(String.format("Finished importing [%s] %d chunks '%s' "
          + "to layer '%s' after %d ms", correlationId, chunkCount, filepath,
          layer, duration));
    } else {
      log.error(String.format("Failed to import [%s] '%s' "
          + "to layer '%s' after %d ms", correlationId, filepath,
          layer, duration), error);
    }

    if (reportActivities) {
      JsonObject msg = new JsonObject()
        .put("activity", "import")
        .put("state", "store")
        .put("owner", deploymentID())
        .put("action", "leave")
        .put("correlationId", correlationId)
        .put("chunkCount", chunkCount)
        .put("duration", duration)
        .put("mimeType", mimeType)
        .put("layer", layer);

      if (error != null) {
        msg.put("error", error.getMessage());
      }

      vertx.eventBus().publish(AddressConstants.ACTIVITIES, msg);
    }
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
   * @return a single that will emit with the number if chunks imported
   * when the file has been imported
   */
  protected Single<Integer> importFile(String contentType, ReadStream<Buffer> f,
      String correlationId, String filename, long timestamp, String layer,
      List<String> tags, Map<String, Object> properties, String fallbackCRSString) {
    if (belongsTo(contentType, "application", "xml") ||
        belongsTo(contentType, "text", "xml")) {
      return importXML(f, correlationId, filename, timestamp, layer, tags,
        properties, fallbackCRSString);
    } else if (belongsTo(contentType, "application", "json")) {
      return importJSON(f, correlationId, filename, timestamp, layer, tags, properties);
    } else {
      return Single.error(new NoStackTraceThrowable(String.format(
          "Received an unexpected content type '%s' while trying to import "
          + "file '%s'", contentType, filename)));
    }
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
   * @return a single that will emit when the file has been imported
   */
  private Single<Integer> importXML(ReadStream<Buffer> f, String correlationId,
      String filename, long timestamp, String layer, List<String> tags,
      Map<String, Object> properties, String fallbackCRSString) {
    Window window = new Window();
    XMLSplitter splitter = new FirstLevelSplitter(window);
    AtomicInteger processing = new AtomicInteger(0);
    XMLCRSIndexer crsIndexer = new XMLCRSIndexer();
    return f.toObservable()
        .map(buf -> (io.vertx.core.buffer.Buffer)buf.getDelegate())
        .doOnNext(window::append)
        .lift(new XMLParserOperator())
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
          return addToStoreWithPause(result, layer, indexMeta, f, processing);
        })
        .count()
        .toSingle();
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
   * @return a single that will emit when the file has been imported
   */
  private Single<Integer> importJSON(ReadStream<Buffer> f, String correlationId,
      String filename, long timestamp, String layer, List<String> tags, Map<String, Object> properties) {
    StringWindow window = new StringWindow();
    GeoJsonSplitter splitter = new GeoJsonSplitter(window);
    AtomicInteger processing = new AtomicInteger(0);
    return f.toObservable()
        .map(buf -> (io.vertx.core.buffer.Buffer)buf.getDelegate())
        .doOnNext(window::append)
        .lift(new JsonParserOperator())
        .flatMap(splitter::onEventObservable)
        .flatMapSingle(result -> {
          IndexMeta indexMeta = new IndexMeta(correlationId, filename,
              timestamp, tags, properties, null);
          return addToStoreWithPause(result, layer, indexMeta, f, processing);
        })
        .count()
        .toSingle();
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
   * @return a single that will emit exactly one item when the
   * operation has finished
   */
  private Single<Void> addToStoreWithPause(Result<? extends ChunkMeta> chunk,
      String layer, IndexMeta indexMeta, ReadStream<Buffer> f, AtomicInteger processing) {
    // pause stream while chunk is being written
    f.pause();
    
    // count number of chunks being written
    processing.incrementAndGet();

    return addToStore(chunk.getChunk(), chunk.getMeta(), layer, indexMeta)
        .doOnSuccess(v -> {
          // resume stream only after all chunks from the current
          // buffer have been stored
          if (processing.decrementAndGet() == 0) {
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
   * @return a single that will emit exactly one item when the
   * operation has finished
   */
  protected Single<Void> addToStore(String chunk, ChunkMeta meta,
      String layer, IndexMeta indexMeta) {
    return Single.defer(() -> store.rxAdd(chunk, meta, layer, indexMeta))
        .retryWhen(RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log));
  }
}
