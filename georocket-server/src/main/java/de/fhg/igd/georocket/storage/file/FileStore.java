package de.fhg.igd.georocket.storage.file;

import java.io.FileNotFoundException;
import java.util.Queue;

import org.bson.types.ObjectId;

import de.fhg.igd.georocket.constants.AddressConstants;
import de.fhg.igd.georocket.constants.ConfigConstants;
import de.fhg.igd.georocket.storage.ChunkMeta;
import de.fhg.igd.georocket.storage.ChunkReadStream;
import de.fhg.igd.georocket.storage.Store;
import de.fhg.igd.georocket.storage.StoreCursor;
import de.fhg.igd.georocket.util.TimedActionQueue;
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
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

/**
 * A verticle storing chunks on the file system
 * @author Michel Kraemer
 */
public class FileStore implements Store {
  private static Logger log = LoggerFactory.getLogger(FileStore.class);
  
  private static final int MAX_FILES_OPEN_ADD = 1000;
  private static final long CLOSE_FILE_TIMEOUT = 1000;
  private static final long CLOSE_FILE_GRACE = 100;
  
  /**
   * The folder where the chunks should be saved
   */
  private final String root;
  
  /**
   * The vertx container
   */
  private final Vertx vertx;
  
  /**
   * A queue of files to close asynchronously
   */
  private final TimedActionQueue<AsyncFile> filesToClose;
  
  /**
   * Default constructor
   * @param vertx the Vertx instance
   */
  public FileStore(Vertx vertx) {
    String home = vertx.getOrCreateContext().config().getString(
        ConfigConstants.HOME, System.getProperty("user.home") + "/.georocket");
    this.root = home + "/storage/file";
    this.vertx = vertx;
    this.filesToClose = new TimedActionQueue<>(MAX_FILES_OPEN_ADD,
        CLOSE_FILE_TIMEOUT, CLOSE_FILE_GRACE, vertx);
  }
  
  /**
   * The handler that closes files asynchronously
   * @param queue the files to close
   * @param done will be called when all files are closed
   */
  private void doCloseFiles(Queue<AsyncFile> queue, Runnable done) {
    // closing files can take a very long time because it involves
    // flushing and releasing (possibly large) direct memory buffers.
    // assume the code will block the event loop.
    vertx.executeBlocking(f -> {
      long start = System.currentTimeMillis();
      int count = 0;
      while (!queue.isEmpty()) {
        queue.poll().close();
        ++count;
      }
      log.info("Flushed and closed " + count + " files in " +
          (System.currentTimeMillis() - start) + " ms");
      f.complete();
    }, ar -> {
      done.run();
    });
  }
  
  @Override
  public void add(String chunk, ChunkMeta meta, Handler<AsyncResult<Void>> handler) {
    // create storage folder
    vertx.fileSystem().mkdirs(root, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      
      // generate new file name
      String id = new ObjectId().toString();
      String filename = id;
      
      // open new file
      FileSystem fs = vertx.fileSystem();
      fs.open(root + "/" + filename, new OpenOptions(), openar -> {
        if (openar.failed()) {
          handler.handle(Future.failedFuture(openar.cause()));
          return;
        }
        
        // write contents to file
        AsyncFile f = openar.result();
        Buffer buf = Buffer.buffer(chunk);
        f.write(buf, 0, writear -> {
          // close file asynchronously
          filesToClose.offer(f, this::doCloseFiles);
          
          if (writear.failed()) {
            handler.handle(Future.failedFuture(writear.cause()));
            return;
          }
          
          // start indexing
          JsonObject indexMsg = new JsonObject()
              .put("action", "add")
              .put("filename", filename)
              .put("meta", meta.toJsonObject());
          vertx.eventBus().publish(AddressConstants.INDEXER, indexMsg);
          
          // tell sender that writing was successful
          handler.handle(Future.succeededFuture());
        });
      });
    });
  }
  
  @Override
  public void getOne(String name, Handler<AsyncResult<ChunkReadStream>> handler) {
    String path = root + "/" + name;
    
    // check if chunk exists
    FileSystem fs = vertx.fileSystem();
    ObservableFuture<Boolean> observable = RxHelper.observableFuture();
    fs.exists(path, observable.toHandler());
    observable
      .flatMap(exists -> {
        if (!exists) {
          return Observable.error(new FileNotFoundException("Could not find chunk: " + name));
        }
        return Observable.just(exists);
      })
      .flatMap(exists -> {
        // get chunk's size
        ObservableFuture<FileProps> propsObservable = RxHelper.observableFuture();
        fs.props(path, propsObservable.toHandler());
        return propsObservable;
      })
      .map(props -> props.size())
      .flatMap(size -> {
        // open chunk
        ObservableFuture<AsyncFile> openObservable = RxHelper.observableFuture();
        OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
        fs.open(path, openOptions, openObservable.toHandler());
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
    new FileStoreCursor(vertx, this, 100).start(handler);
  }
}
