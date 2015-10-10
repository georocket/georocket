package de.fhg.igd.georocket.storage.file;

import org.bson.types.ObjectId;

import de.fhg.igd.georocket.constants.AddressConstants;
import de.fhg.igd.georocket.constants.ConfigConstants;
import de.fhg.igd.georocket.constants.MessageConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;

/**
 * A verticle storing chunks on the file system
 * @author Michel Kraemer
 */
public class FileStoreVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(FileStoreVerticle.class);
  
  /**
   * The folder where the chunks should be saved
   */
  private String root;
  
  @Override
  public void start(Future<Void> startFuture) {
    log.info("Launching file store ...");
    
    String home = config().getString(ConfigConstants.HOME, System.getProperty("user.home") + "/.georocket");
    root = home + "/storage/file";
    
    // create storage folder
    vertx.fileSystem().mkdirs(root, ar -> {
      if (ar.failed()) {
        startFuture.fail(ar.cause());
      } else {
        vertx.eventBus().consumer(AddressConstants.STORE, this::onMessage);
        startFuture.complete();
      }
    });
  }
  
  private void onMessage(Message<JsonObject> msg) {
    JsonObject obj = msg.body();
    String action = obj.getString(MessageConstants.ACTION);
    switch (action) {
    case MessageConstants.ADD:
      onAdd(msg);
      break;
    
    case MessageConstants.GET:
      onGet(msg);
      break;
    
    default:
      msg.fail(400, "Unknown action: " + action);
      break;
    }
  }
  
  /**
   * Handle ADD message. Adds a chunk to the store.
   * @param msg the message
   */
  private void onAdd(Message<JsonObject> msg) {
    JsonObject obj = msg.body();
    String chunk = obj.getString(MessageConstants.CHUNK);
    if (chunk == null) {
      msg.fail(400, "Missing chunk");
      return;
    }
    
    // generate new file name
    String id = new ObjectId().toString();
    String filename = id;
    
    // open new file
    FileSystem fs = vertx.fileSystem();
    fs.open(root + "/" + filename, new OpenOptions(), openar -> {
      if (openar.failed()) {
        msg.fail(500, "Could not open storage file: " + openar.cause().getMessage());
        return;
      }
      
      // write contents to file
      AsyncFile f = openar.result();
      Buffer buf = Buffer.buffer(chunk);
      f.write(buf, 0, writear -> {
        f.close();
        if (writear.failed()) {
          msg.fail(500, "Could not write to storage file: " + writear.cause().getMessage());
          return;
        }
        
        // start indexing
        vertx.eventBus().publish(AddressConstants.INDEXER, filename);
        
        // tell sender that writing was successful
        msg.reply(null);
      });
    });
  }
  
  /**
   * Handle GET message. Get a chunk from the store.
   * @param msg the message
   */
  private void onGet(Message<JsonObject> msg) {
    JsonObject obj = msg.body();
    String name = obj.getString(MessageConstants.NAME);
    if (name == null) {
      msg.fail(400, "Missing name");
      return;
    }
    
    String path = root + "/" + name;
    
    // check if chunk exists
    FileSystem fs = vertx.fileSystem();
    OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
    fs.exists(path, existsar -> {
      if (existsar.failed()) {
        log.error("Could not check if chunk exists", existsar.cause());
        msg.fail(500, existsar.cause().getMessage());
        return;
      }
      if (!existsar.result()) {
        msg.fail(404, "Unknown chunk: " + name);
        return;
      }
      
      // get chunk's size
      fs.props(path, propsar -> {
        if (propsar.failed()) {
          log.error("Could not get chunk properties", propsar.cause());
          msg.fail(500, propsar.cause().getMessage());
          return;
        }
        
        // open chunk
        fs.open(path, openOptions, openar -> {
          if (openar.failed()) {
            log.error("Could not open chunk", openar.cause());
            msg.fail(500, openar.cause().getMessage());
            return;
          }
          
          // send chunk's size to peer
          msg.<String>reply(propsar.result().size(), replyar -> {
            if (replyar.failed()) {
              // peer aborted process
              return;
            }
            
            // get peer's reply address and send chunk to it
            String replyAddress = replyar.result().body();
            MessageProducer<Buffer> sender = vertx.eventBus().sender(replyAddress);
            AsyncFile f = openar.result();
            Pump.pump(f, sender).start();
            f.endHandler(v -> f.close());
          });
        });
      });
    });
  }
}
