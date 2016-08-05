package io.georocket;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.xml.CRSIndexer;
import io.georocket.input.FirstLevelSplitter;
import io.georocket.input.Splitter;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.IndexMeta;
import io.georocket.storage.Store;
import io.georocket.storage.StoreFactory;
import io.georocket.util.AsyncXMLParser;
import io.georocket.util.RxUtils;
import io.georocket.util.Window;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.file.FileSystem;
import io.vertx.rxjava.core.streams.ReadStream;
import rx.Observable;

/**
 * Imports file in the background
 * @author Michel Kraemer
 */
public class ImporterVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(ImporterVerticle.class);
  
  private static final int MAX_RETRIES = 5;
  private static final int RETRY_INTERVAL = 1000;
  
  protected Store store;
  private String incoming;
  
  @Override
  public void start() {
    log.info("Launching importer ...");
    
    store = StoreFactory.createStore((Vertx)vertx.getDelegate());
    String storagePath = vertx.getOrCreateContext().config().getString(
        ConfigConstants.STORAGE_FILE_PATH);
    incoming = storagePath + "/incoming";
    
    vertx.eventBus().consumer(AddressConstants.IMPORTER, this::onMessage);
  }
  
  /**
   * Receives a message
   * @param msg the message 
   */
  protected void onMessage(Message<JsonObject> msg) {
    String action = msg.body().getString("action");
    switch (action) {
    case "import":
      onImport(msg);
      break;
    
    default:
      msg.fail(400, "Invalid action: " + action);
      log.error("Invalid action: " + action);
      break;
    }
  }
  
  /**
   * Receives a name of a file to import
   * @param msg the event bus message containing the filename
   */
  protected void onImport(Message<JsonObject> msg) {
    JsonObject body = msg.body();
    String filename = body.getString("filename");
    String filepath = incoming + "/" + filename;
    String layer = body.getString("layer", "/");
    String contentType = body.getString("contentType");

    // get tags
    JsonArray tagsArr = body.getJsonArray("tags");
    List<String> tags = tagsArr != null ? tagsArr.stream().flatMap(o -> o != null ?
        Stream.of(o.toString()) : Stream.of()).collect(Collectors.toList()) : null;
    
    log.info("Importing " + filepath + " to layer " + layer);

    // generate ID and timestamp for this import
    String importId = UUID.randomUUID().toString();
    Date timeStamp = Calendar.getInstance().getTime();
    
    FileSystem fs = vertx.fileSystem();
    OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
    fs.openObservable(filepath, openOptions)
      .flatMap(f -> importFile(contentType, f, importId, filename, timeStamp, layer, tags).finallyDo(() -> {
        // delete file from 'incoming' folder
        log.info("Deleting " + filepath + " from incoming folder");
        f.closeObservable()
          .flatMap(v -> fs.deleteObservable(filepath))
          .subscribe(v -> {}, err -> {
            log.error("Could not delete file from 'incoming' folder", err);
          });
      }))
      .subscribe(v -> {}, err -> {
        log.error("Failed to import chunk", err);
      });
  }

  /**
   * Import a file from the given read stream into the store. Inspect the file's
   * content type and forward to the correct import method.
   * @param contentType the file's content type
   * @param f the file to import
   * @param importId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param importTimeStamp denotes when the import process has started
   * @param layer the layer where the file should be stored (may be null)
   * @param tags the list of tags to attach to the file (may be null)
   * @return an observable that will emit when the file has been imported
   */
  protected Observable<Void> importFile(String contentType, ReadStream<Buffer> f,
      String importId, String filename, Date importTimeStamp, String layer, List<String> tags) {
    switch (contentType) {
      case "application/xml":
      case "text/xml":
        return importXML(f, importId, filename, importTimeStamp, layer, tags);
      default:
        return Observable.error(new NoStackTraceThrowable(String.format(
            "Received an unexpected content type '%s' while trying to import"
            + "file '%s'", contentType, filename)));
    }
  }

  /**
   * Imports an XML file from the given input stream into the store
   * @param f the XML file to read
   * @param importId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param importTimeStamp denotes when the import process has started
   * @param layer the layer where the file should be stored (may be null)
   * @param tags the list of tags to attach to the file (may be null)
   * @return an observable that will emit when the file has been imported
   */
  private Observable<Void> importXML(ReadStream<Buffer> f, String importId,
      String filename, Date importTimeStamp, String layer, List<String> tags) {
    AsyncXMLParser xmlParser = new AsyncXMLParser();
    Window window = new Window();
    Splitter splitter = new FirstLevelSplitter(window);
    AtomicInteger processing = new AtomicInteger(0);
    CRSIndexer crsIndexer = new CRSIndexer();
    return f.toObservable()
        .map(buf -> (io.vertx.core.buffer.Buffer)buf.getDelegate())
        .doOnNext(window::append)
        .flatMap(xmlParser::feed)
        .doOnNext(e -> {
          // save the first CRS found in the file
          if (crsIndexer.getCRS() == null) {
            crsIndexer.onEvent(e);
          }
        })
        .flatMap(splitter::onEventObservable)
        .flatMap(result -> {
          // pause stream while chunk is being written
          f.pause();
          
          // count number of chunks being written
          processing.incrementAndGet();

          IndexMeta indexMeta = new IndexMeta(importId, filename,
              importTimeStamp, tags, crsIndexer.getCRS());
          Observable<Void> o = addToStore(result.getChunk(), result.getMeta(),
              layer, indexMeta);
          return o.doOnNext(v -> {
            // resume stream only after all chunks from the current
            // buffer have been stored
            if (processing.decrementAndGet() == 0) {
              // go ahead
              f.resume();
            }
          });
        })
        .last() // "wait" for last event (i.e. end of file)
        .finallyDo(xmlParser::close);
  }
  
  /**
   * Add a chunk to the store
   * @param chunk the chunk to add
   * @param meta the chunk's metadata
   * @param layer the layer the chunk should be added to (may be null)
   * @param indexMeta metadata specifying how the chunk should be indexed
   * @return an observable that will emit exactly one item when the
   * operation has finished
   */
  private Observable<Void> addToStoreNoRetry(String chunk, ChunkMeta meta,
      String layer, IndexMeta indexMeta) {
    ObservableFuture<Void> o = RxHelper.observableFuture();
    store.add(chunk, meta, layer, indexMeta, o.toHandler());
    return o;
  }
  
  /**
   * Add a chunk to the store. Retry operation several times before failing.
   * @param chunk the chunk to add
   * @param meta the chunk's metadata
   * @param layer the layer the chunk should be added to (may be null)
   * @param indexMeta metadata specifying how the chunk should be indexed
   * @return an observable that will emit exactly one item when the
   * operation has finished
   */
  protected Observable<Void> addToStore(String chunk, ChunkMeta meta,
      String layer, IndexMeta indexMeta) {
    return Observable.<Void>create(subscriber -> {
      addToStoreNoRetry(chunk, meta, layer, indexMeta).subscribe(subscriber);
    }).retryWhen(RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL,
        RxHelper.scheduler(getVertx()), log));
  }
}
