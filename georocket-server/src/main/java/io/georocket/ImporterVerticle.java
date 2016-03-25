package io.georocket;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
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
  
  private Store store;
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
  private void onMessage(Message<JsonObject> msg) {
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
  private void onImport(Message<JsonObject> msg) {
    JsonObject body = msg.body();
    String filename = incoming + "/" + body.getString("filename");
    String layer = body.getString("layer", "/");
    
    // get tags
    JsonArray tagsArr = body.getJsonArray("tags");
    List<String> tags = tagsArr != null ? tagsArr.stream().flatMap(o -> o != null ?
        Stream.of(o.toString()) : Stream.of()).collect(Collectors.toList()) : null;
    
    log.info("Importing " + filename + " to layer " + layer);
    
    FileSystem fs = vertx.fileSystem();
    OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
    fs.openObservable(filename, openOptions)
      .flatMap(f -> importXML(f, layer, tags).finallyDo(() -> {
        // delete file from 'incoming' folder
        log.info("Deleting " + filename + " from incoming folder");
        f.closeObservable()
          .flatMap(v -> fs.deleteObservable(filename))
          .subscribe(v -> {}, err -> {
            log.error("Could not delete file from 'incoming' folder", err);
          });
      }))
      .subscribe(v -> {}, err -> {
        log.error("Failed to import chunk", err);
      });
  }
  
  /**
   * Imports an XML file from the given input stream into the store
   * @param f the XML file to read
   * @param layer the layer where the file should be stored (may be null)
   * @param tags the list of tags to attach to the file (may be null)
   * @return an observable that will emit when the file has been imported
   */
  private Observable<Void> importXML(ReadStream<Buffer> f, String layer, List<String> tags) {
    AsyncXMLParser xmlParser = new AsyncXMLParser();
    Window window = new Window();
    Splitter splitter = new FirstLevelSplitter(window);
    AtomicInteger processing = new AtomicInteger(0);
    return f.toObservable()
        .map(buf -> (io.vertx.core.buffer.Buffer)buf.getDelegate())
        .doOnNext(window::append)
        .flatMap(xmlParser::feed)
        .flatMap(splitter::onEventObservable)
        .flatMap(result -> {
          // pause stream while chunk is being written
          f.pause();
          
          // count number of chunks being written
          processing.incrementAndGet();
          
          IndexMeta indexMeta = new IndexMeta(tags, null);
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
  private Observable<Void> addToStore(String chunk, ChunkMeta meta,
      String layer, IndexMeta indexMeta) {
    return Observable.<Void>create(subscriber -> {
      addToStoreNoRetry(chunk, meta, layer, indexMeta).subscribe(subscriber);
    }).retryWhen(RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log));
  }
}
