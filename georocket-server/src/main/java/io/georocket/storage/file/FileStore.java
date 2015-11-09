package io.georocket.storage.file;

import java.io.FileNotFoundException;
import java.util.ArrayDeque;
import java.util.Queue;

import org.bson.types.ObjectId;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.Store;
import io.georocket.storage.StoreCursor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

/**
 * A verticle storing chunks on the file system
 * @author Michel Kraemer
 */
public class FileStore implements Store {
  private static final int PAGE_SIZE = 100;

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
  public void get(String search, String path, Handler<AsyncResult<StoreCursor>> handler) {
    new FileStoreCursor(vertx, PAGE_SIZE, search, path).start(handler);
  }
  
  @Override
  public void delete(String search, String path, Handler<AsyncResult<Void>> handler) {
    new FileStoreCursor(vertx, PAGE_SIZE, search, path).start(ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        StoreCursor cursor = ar.result();
        Queue<String> paths = new ArrayDeque<>();
        doDelete(cursor, paths, handler);
      }
    });
  }
  
  /**
   * Iterate over a cursor and delete all returned chunks from the index
   * and from the store.
   * @param cursor the cursor to iterate over
   * @param paths an empty queue (used internally for recursion)
   * @param handler will be called when all chunks have been deleted
   */
  private void doDelete(StoreCursor cursor, Queue<String> paths,
      Handler<AsyncResult<Void>> handler) {
    // handle response of bulk delete operation
    Handler<AsyncResult<Void>> handleBulk = bulkAr -> {
      if (bulkAr.failed()) {
        handler.handle(Future.failedFuture(bulkAr.cause()));
      } else {
        doDelete(cursor, paths, handler);
      }
    };
    
    // while cursor has items ...
    if (cursor.hasNext()) {
      cursor.next(ar -> {
        if (ar.failed()) {
          handler.handle(Future.failedFuture(ar.cause()));
        } else {
          // add item to queue
          paths.add(cursor.getChunkPath());
          
          if (paths.size() >= PAGE_SIZE) {
            // if there are enough items in the queue, bulk delete them
            doDeleteBulk(paths, handleBulk);
          } else {
            // otherwise proceed with next item from cursor
            doDelete(cursor, paths, handler);
          }
        }
      });
    } else {
      // cursor does not return any more items
      if (!paths.isEmpty()) {
        // bulk delete the remaining ones
        doDeleteBulk(paths, handleBulk);
      } else {
        // end operation
        handler.handle(Future.succeededFuture());
      }
    }
  }
  
  /**
   * Deletes all chunks with the given paths from the index and from the store.
   * Removes all items from the given queue.
   * @param paths the queue of paths of chunks to delete (will be empty when
   * the operation has finished)
   * @param handler will be called when the operation has finished
   */
  private void doDeleteBulk(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    // delete from index first so the chunks cannot be found anymore
    JsonArray jsonPaths = new JsonArray();
    paths.forEach(jsonPaths::add);
    JsonObject indexMsg = new JsonObject()
        .put("action", "delete")
        .put("paths", jsonPaths);
    vertx.eventBus().send(AddressConstants.INDEXER, indexMsg, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        // now delete all chunks from file system and clear the queue
        doDeleteChunks(paths, handler);
      }
    });
  }
  
  /**
   * Deletes all chunks with the given paths from the store. Removes all items
   * from the given queue.
   * @param paths the queue of paths of chunks to delete (will be empty when
   * the operation has finished)
   * @param handler will be called when the operation has finished
   */
  private void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    if (paths.isEmpty()) {
      handler.handle(Future.succeededFuture());
      return;
    }
    
    String path = paths.poll();
    FileSystem fs = vertx.fileSystem();
    String absolutePath = root + "/" + path;
    fs.delete(absolutePath, deleteAr -> {
      if (deleteAr.failed()) {
        handler.handle(Future.failedFuture(deleteAr.cause()));
      } else {
        doDeleteChunks(paths, handler);
      }
    });
  }
}
