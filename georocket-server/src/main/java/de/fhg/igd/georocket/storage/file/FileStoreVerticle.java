package de.fhg.igd.georocket.storage.file;

import org.bson.types.ObjectId;

import de.fhg.igd.georocket.constants.AddressConstants;
import de.fhg.igd.georocket.constants.ConfigConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;

/**
 * A verticle storing chunks on the file system
 * @author Michel Kraemer
 */
public class FileStoreVerticle extends AbstractVerticle {
  /**
   * The folder where the chunks should be saved
   */
  private String root;
  
  @Override
  public void start(Future<Void> startFuture) {
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
  
  private void onMessage(Message<String> msg) {
    // generate new file name
    String id = new ObjectId().toString();
    
    // open new file
    FileSystem fs = vertx.fileSystem();
    fs.open(root + "/" + id, new OpenOptions(), openar -> {
      if (openar.failed()) {
        msg.fail(500, "Could not open storage file: " + openar.cause().getMessage());
        return;
      }
      
      // write contents to file
      AsyncFile f = openar.result();
      Buffer buf = Buffer.buffer(msg.body());
      f.write(buf, 0, writear -> {
        f.close();
        if (writear.failed()) {
          msg.fail(500, "Could not write to storage file: " + writear.cause().getMessage());
          return;
        }
        msg.reply(null); // tell sender that writing was successful
      });
    });
  }
}
