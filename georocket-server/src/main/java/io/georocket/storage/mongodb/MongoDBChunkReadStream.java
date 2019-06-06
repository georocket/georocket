package io.georocket.storage.mongodb;

import java.nio.ByteBuffer;

import com.mongodb.async.client.gridfs.AsyncInputStream;

import io.georocket.storage.ChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;

/**
 * A read stream for chunks stored in MongoDB
 * @author Michel Kraemer
 */
public class MongoDBChunkReadStream implements ChunkReadStream {
  private static Logger log = LoggerFactory.getLogger(MongoDBChunkReadStream.class);

  /**
   * The input stream containing the chunk
   */
  private final AsyncInputStream is;
  
  /**
   * The chunk's size
   */
  private final long size;
  
  /**
   * The size of the buffer used to read from the input stream.
   * This value should match the MongoDB GridFS chunk size. If it
   * doesn't unexpected read errors might occur.
   */
  private final int readBufferSize;
  
  /**
   * The current Vert.x context
   */
  private final Context context;
  
  /**
   * True if the stream is closed
   */
  private boolean closed;
  
  /**
   * True if the stream is paused
   */
  private boolean paused;
  
  /**
   * True if a read operation is currently in progress
   */
  private boolean readInProgress;
  
  /**
   * A handler that will be called when new data has
   * been read from the input stream
   */
  private Handler<Buffer> handler;
  
  /**
   * A handler that will be called when the whole chunk
   * has been read
   */
  private Handler<Void> endHandler;
  
  /**
   * A handler that will be called when an exception has
   * occurred while reading from the input stream
   */
  private Handler<Throwable> exceptionHandler;
  
  /**
   * Constructs a new read stream
   * @param is the input stream containing the chunk
   * @param size the chunk's size
   * @param readBufferSize the size of the buffer used to read
   * from the input stream. This value should match the MongoDB GridFS
   * chunk size. If it doesn't unexpected read errors might occur.
   * @param context the Vert.x context
   */
  public MongoDBChunkReadStream(AsyncInputStream is, long size,
      int readBufferSize, Context context) {
    this.is = is;
    this.size = size;
    this.readBufferSize = readBufferSize;
    this.context = context;
  }
  
  /**
   * Perform sanity checks
   */
  private void check() {
    if (closed) {
      throw new IllegalStateException("Read stream is closed");
    }
  }
  
  /**
   * Perform asynchronous read and call handlers accordingly
   */
  private void doRead() {
    if (!readInProgress) {
      readInProgress = true;
      Buffer buff = Buffer.buffer(readBufferSize);
      ByteBuffer bb = ByteBuffer.allocate(readBufferSize);
      doRead(buff, bb, ar -> {
        if (ar.succeeded()) {
          readInProgress = false;
          Buffer buffer = ar.result();
          if (buffer.length() == 0) {
            // empty buffer represents end of file
            handleEnd();
          } else {
            handleData(buffer);
            if (!paused && handler != null) {
              doRead();
            }
          }
        } else {
          handleException(ar.cause());
        }
      });
    }
  }
  
  private void doRead(Buffer writeBuff, ByteBuffer buff,
      Handler<AsyncResult<Buffer>> handler) {
    is.read(buff, (bytesRead, t) -> {
      if (t != null) {
        context.runOnContext(v -> handler.handle(Future.failedFuture(t)));
      } else {
        if (bytesRead == -1 || !buff.hasRemaining()) {
          // end of file or end of buffer
          context.runOnContext(v -> {
            buff.flip();
            writeBuff.setBytes(0, buff);
            handler.handle(Future.succeededFuture(writeBuff));
          });
        } else {
          // read more bytes
          doRead(writeBuff, buff, handler);
        }
      }
    });
  }
  
  /**
   * Will be called when data has been read from the stream
   * @param buffer the buffer containing the data read
   */
  private void handleData(Buffer buffer) {
    if (handler != null) {
      handler.handle(buffer);
    }
  }
  
  /**
   * Will be called when the end of the stream has been reached
   */
  private void handleEnd() {
    if (endHandler != null) {
      endHandler.handle(null);
    }
  }
  
  /**
   * Will be called when an error has occurred while reading
   * @param t the error
   */
  private void handleException(Throwable t) {
    if (exceptionHandler != null && t instanceof Exception) {
      exceptionHandler.handle(t);
    } else {
      log.error("Unhandled exception", t);
    }
  }
  
  @Override
  public long getSize() {
    return size;
  }
  
  @Override
  public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
    check();
    exceptionHandler = handler;
    return this;
  }

  @Override
  public ReadStream<Buffer> handler(Handler<Buffer> handler) {
    check();
    this.handler = handler;
    if (handler != null && !paused && !closed) {
      doRead();
    }
    return this;
  }

  @Override
  public ReadStream<Buffer> pause() {
    check();
    paused = true;
    return this;
  }

  @Override
  public ReadStream<Buffer> resume() {
    check();
    if (paused && !closed) {
      paused = false;
      if (handler != null) {
        doRead();
      }
    }
    return this;
  }

  @Override
  public ReadStream<Buffer> fetch(long amount) {
    return resume();
  }

  @Override
  public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
    check();
    this.endHandler = endHandler;
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    check();
    closed = true;
    is.close((r, t) -> {
      Future<Void> res = Future.future();
      if (t != null) {
        res.fail(t);
      } else {
        res.complete(r);
      }
      if (handler != null) {
        context.runOnContext(v -> handler.handle(res));
      }
    });
  }
}
