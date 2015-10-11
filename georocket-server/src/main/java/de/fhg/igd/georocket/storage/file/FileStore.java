package de.fhg.igd.georocket.storage.file;

import java.io.FileNotFoundException;
import java.util.ArrayDeque;
import java.util.Queue;

import org.bson.types.ObjectId;

import de.fhg.igd.georocket.constants.AddressConstants;
import de.fhg.igd.georocket.constants.ConfigConstants;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

/**
 * A verticle storing chunks on the file system
 * @author Michel Kraemer
 */
public class FileStore implements Store {
  private static final long CLOSE_FILE_TIMEOUT = 5000;
  
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
  private final Queue<AsyncFile> filesToClose = new ArrayDeque<>();
  
  /**
   * The time when the last file was added to {@link #filesToClose}
   */
  private long lastCloseFile = 0;
  
  /**
   * The ID of the timer that asynchronously closes files from {@link #filesToClose}
   */
  private long closeFileTimerId = -1;
  
  /**
   * Default constructor
   */
  public FileStore(Vertx vertx) {
    String home = vertx.getOrCreateContext().config().getString(
        ConfigConstants.HOME, System.getProperty("user.home") + "/.georocket");
    this.root = home + "/storage/file";
    this.vertx = vertx;
  }
  
  /**
   * Schedules a file to be closed asynchronously
   * @param f the file to close
   */
  private void scheduleClose(AsyncFile f) {
    filesToClose.offer(f);
    lastCloseFile = System.currentTimeMillis();
    if (closeFileTimerId < 0) {
      closeFileTimerId = vertx.setTimer(CLOSE_FILE_TIMEOUT, this::doCloseFiles);
    }
  }
  
  /**
   * The timer that closes files asynchronously
   * @param timerId the timer's ID
   */
  private void doCloseFiles(Long timerId) {
    long now = System.currentTimeMillis();
    long elapsed = now - lastCloseFile;
    if (elapsed >= CLOSE_FILE_TIMEOUT) {
      closeFileTimerId = -1;
      while (!filesToClose.isEmpty()) {
        if (System.currentTimeMillis() - now > 10) { 
          // do not block the event loop more than 10ms, and
          // then wait a bit before we continue
          closeFileTimerId = vertx.setTimer(10, this::doCloseFiles);
          break;
        }
        filesToClose.poll().close();
      }
    } else {
      closeFileTimerId = vertx.setTimer(CLOSE_FILE_TIMEOUT - elapsed, this::doCloseFiles);
    }
  }
  
  @Override
  public void add(String chunk, Handler<AsyncResult<Void>> handler) {
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
          scheduleClose(f);
          
          if (writear.failed()) {
            handler.handle(Future.failedFuture(writear.cause()));
            return;
          }
          
          // tell sender that writing was successful
          handler.handle(Future.succeededFuture());
        });
      });
    });
  }
  
  @Override
  public void get(String name, Handler<AsyncResult<ChunkReadStream>> handler) {
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
        return openObservable.map(f -> new ChunkReadStream(size, f));
      })
      .subscribe(readStream -> {
        // send chunk to peer
        handler.handle(Future.succeededFuture(readStream));
      }, err -> {
        handler.handle(Future.failedFuture(err));
      });
  }
}
