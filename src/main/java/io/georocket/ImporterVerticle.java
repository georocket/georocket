package io.georocket;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.input.FirstLevelSplitter;
import io.georocket.input.Splitter;
import io.georocket.storage.Store;
import io.georocket.storage.file.FileStore;
import io.georocket.util.AsyncXMLParser;
import io.georocket.util.Window;
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
  
  private Store store;
  private String incoming;
  
  @Override
  public void start() {
    log.info("Launching importer ...");
    
    store = new FileStore((io.vertx.core.Vertx)vertx.getDelegate());
    String storagePath = vertx.getOrCreateContext().config().getString(
        ConfigConstants.STORAGE_PATH);
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
    return f.toObservable()
        .map(buf -> (io.vertx.core.buffer.Buffer)buf.getDelegate())
        .doOnNext(window::append)
        .flatMap(xmlParser::feed)
        .flatMap(splitter::onEventObservable)
        .flatMap(result -> {
          // pause stream while chunk is being written
          f.pause();
          
          ObservableFuture<Void> o = RxHelper.observableFuture();
          store.add(result.getChunk(), result.getMeta(), layer, tags, o.toHandler());
          return o.doOnNext(v -> {
            // go ahead
            f.resume();
          });
        })
        .last() // "wait" for last event (i.e. end of file)
        .finallyDo(xmlParser::close);
  }
}
