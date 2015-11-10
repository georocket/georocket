package io.georocket;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.input.FirstLevelSplitter;
import io.georocket.input.Splitter;
import io.georocket.storage.Store;
import io.georocket.storage.file.FileStore;
import io.georocket.util.WindowPipeStream;
import io.georocket.util.XMLPipeStream;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;

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
    
    store = new FileStore(vertx);
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
    String filename = incoming + "/" + msg.body().getString("filename");
    String layer = msg.body().getString("layer", "/");
    log.info("Importing " + filename + " to layer " + layer);
    
    FileSystem fs = vertx.fileSystem();
    OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
    ObservableFuture<AsyncFile> observable = RxHelper.observableFuture();
    fs.open(filename, openOptions, observable.toHandler());
    observable
      .flatMap(f -> {
        ObservableFuture<Void> importObservable = RxHelper.observableFuture();
        importXML(f, layer, ar -> {
          f.close();
          fs.delete(filename, deleteAr -> {
            if (ar.failed()) {
              importObservable.toHandler().handle(ar);
            } else {
              importObservable.toHandler().handle(deleteAr);
            }
          });
        });
        return importObservable;
      })
      .subscribe(v -> {
        // nothing to do here
      }, err -> {
        err.printStackTrace();
      });
  }
  
  /**
   * Imports an XML file from the given input stream into the store
   * @param f the XML file to read
   * @param layer the layer where the file should be stored (may be null)
   * @param callback will be called when the operation has finished
   */
  private void importXML(ReadStream<Buffer> f, String layer,
      Handler<AsyncResult<Void>> callback) {
    XMLPipeStream xmlStream = new XMLPipeStream(vertx);
    WindowPipeStream windowPipeStream = new WindowPipeStream();
    Splitter splitter = new FirstLevelSplitter(windowPipeStream.getWindow());
    
    Pump.pump(f, windowPipeStream).start();
    Pump.pump(windowPipeStream, xmlStream).start();
    
    f.endHandler(v -> {
      xmlStream.close();
      callback.handle(Future.succeededFuture());
    });
    
    f.exceptionHandler(e -> callback.handle(Future.failedFuture(e)));
    windowPipeStream.exceptionHandler(e -> callback.handle(Future.failedFuture(e)));
    xmlStream.exceptionHandler(e -> callback.handle(Future.failedFuture(e)));
    
    xmlStream.handler(event -> {
      Splitter.Result splitResult = splitter.onEvent(event);
      if (splitResult != null) {
        // splitter has created a chunk. store it.
        xmlStream.pause(); // pause stream while chunk being written
        store.add(splitResult.getChunk(), splitResult.getMeta(), layer, ar -> {
          if (ar.failed()) {
            callback.handle(Future.failedFuture(ar.cause()));
          } else {
            // go ahead
            xmlStream.resume();
          }
        });
      }
    });
  }
}
