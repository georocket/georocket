package io.georocket.storage.file;

import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.Queue;

import com.google.common.base.Preconditions;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.PathUtils;
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
 * Stores chunks on the file system
 * @author Michel Kraemer
 */
public class FileStore extends IndexedStore {
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
   * @param vertx the Vert.x instance
   */
  public FileStore(Vertx vertx) {
    super(vertx);
    
    String storagePath = vertx.getOrCreateContext().config().getString(
        ConfigConstants.STORAGE_FILE_PATH);
    Preconditions.checkNotNull(storagePath, "Missing configuration item \"" +
        ConfigConstants.STORAGE_FILE_PATH + "\"");
    
    this.root = Paths.get(storagePath, "file").toString();
    this.vertx = vertx;
  }

  @Override
  protected void doAddChunk(String chunk, String path,
      String correlationId, Handler<AsyncResult<String>> handler) {
    if (path == null || path.isEmpty()) {
      path = "/";
    }
    String dir = Paths.get(root, path).toString();
    String finalPath = path;

    // create storage folder
    vertx.fileSystem().mkdirs(dir, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }

      // generate new file name
      String filename = generateChunkId(correlationId);

      // open new file
      FileSystem fs = vertx.fileSystem();
      fs.open(Paths.get(dir, filename).toString(), new OpenOptions(), openar -> {
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
          } else {
            String result = PathUtils.join(finalPath, filename);
            handler.handle(Future.succeededFuture(result));
          }
        });
      });
    });
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    String absolutePath = Paths.get(root, path).toString();

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
  protected void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    if (paths.isEmpty()) {
      handler.handle(Future.succeededFuture());
      return;
    }

    String path = paths.poll();
    FileSystem fs = vertx.fileSystem();
    String absolutePath = Paths.get(root, path).toString();

    fs.exists(absolutePath, existAr -> {
      if (existAr.failed()) {
        handler.handle(Future.failedFuture(existAr.cause()));
      } else {
        if (existAr.result()) {
          fs.delete(absolutePath, deleteAr -> {
            if (deleteAr.failed()) {
              handler.handle(Future.failedFuture(deleteAr.cause()));
            } else {
              doDeleteChunks(paths, handler);
            }
          });
        } else {
          doDeleteChunks(paths, handler);
        }
      }
    });

  }
}
