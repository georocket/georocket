package de.fhg.igd.georocket.storage.file;

import java.io.FileNotFoundException;

import org.bson.types.ObjectId;

import de.fhg.igd.georocket.constants.AddressConstants;
import de.fhg.igd.georocket.constants.ConfigConstants;
import de.fhg.igd.georocket.storage.ChunkMeta;
import de.fhg.igd.georocket.storage.ChunkReadStream;
import de.fhg.igd.georocket.storage.Store;
import de.fhg.igd.georocket.storage.StoreCursor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

/**
 * A verticle storing chunks on the file system
 * @author Michel Kraemer
 */
public class FileStore implements Store {
  /**
   * The folder where the chunks should be saved
   */
  private final String root;
  
  /**
   * The vertx container
   */
  private final Vertx vertx;
  
  /**
   * Default constructor
   * @param vertx the Vertx instance
   */
  public FileStore(Vertx vertx) {
    String home = vertx.getOrCreateContext().config().getString(
        ConfigConstants.HOME, System.getProperty("user.home") + "/.georocket");
    this.root = home + "/storage/file";
    this.vertx = vertx;
  }
  
  @Override
  public void add(String chunk, ChunkMeta meta, String path,
      Handler<AsyncResult<Void>> handler) {
    if (path == null || path.isEmpty()) {
      path = "/";
    }
    String dir = root + path;
    String finalPath = path;
    
    // create storage folder
    vertx.fileSystem().mkdirs(dir, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      
      // generate new file name
      String id = new ObjectId().toString();
      String filename = id;
      
      // open new file
      FileSystem fs = vertx.fileSystem();
      fs.open(dir + "/" + filename, new OpenOptions(), openar -> {
        if (openar.failed()) {
          handler.handle(Future.failedFuture(openar.cause()));
          return;
        }
        
        // write contents to file
        AsyncFile f = openar.result();
        Buffer buf = Buffer.buffer(chunk);
        f.write(buf, 0, writear -> {
          f.close();
          
          if (writear.failed()) {
            handler.handle(Future.failedFuture(writear.cause()));
            return;
          }
          
          // start indexing
          JsonObject indexMsg = new JsonObject()
              .put("action", "add")
              .put("path", finalPath + "/" + filename)
              .put("meta", meta.toJsonObject());
          vertx.eventBus().publish(AddressConstants.INDEXER, indexMsg);
          
          // tell sender that writing was successful
          handler.handle(Future.succeededFuture());
        });
      });
    });
  }
  
  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    String absolutePath = root + "/" + path;
    
    // check if chunk exists
    FileSystem fs = vertx.fileSystem();
    ObservableFuture<Boolean> observable = RxHelper.observableFuture();
    fs.exists(absolutePath, observable.toHandler());
    observable
      .flatMap(exists -> {
        if (!exists) {
          return Observable.error(new FileNotFoundException("Could not find chunk: " + path));
        }
        return Observable.just(exists);
      })
      .flatMap(exists -> {
        // get chunk's size
        ObservableFuture<FileProps> propsObservable = RxHelper.observableFuture();
        fs.props(absolutePath, propsObservable.toHandler());
        return propsObservable;
      })
      .map(props -> props.size())
      .flatMap(size -> {
        // open chunk
        ObservableFuture<AsyncFile> openObservable = RxHelper.observableFuture();
        OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
        fs.open(absolutePath, openOptions, openObservable.toHandler());
        return openObservable.map(f -> new FileChunkReadStream(size, f));
      })
      .subscribe(readStream -> {
        // send chunk to peer
        handler.handle(Future.succeededFuture(readStream));
      }, err -> {
        handler.handle(Future.failedFuture(err));
      });
  }
  
  @Override
  public void get(String search, Handler<AsyncResult<StoreCursor>> handler) {
    new FileStoreCursor(vertx, this, 100, search).start(handler);
  }
}
