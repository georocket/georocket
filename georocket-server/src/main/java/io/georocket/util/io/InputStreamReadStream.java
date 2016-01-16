package io.georocket.util.io;

import java.io.IOException;
import java.io.InputStream;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;

/**
 * Transforms an {@link InputStream} to a {@link ReadStream}
 * @author Michel Kraemer
 */
public class InputStreamReadStream implements ReadStream<Buffer> {
  private static Logger log = LoggerFactory.getLogger(InputStreamReadStream.class);
  
  private static final int READ_BUFFER_SIZE = 8192;
  
  private final InputStream is;
  private final Vertx vertx;
  private boolean readInProgress;
  private boolean paused;
  private boolean closed;
  private Handler<Throwable> exceptionHandler;
  private Handler<Buffer> dataHandler;
  private Handler<Void> endHandler;
  
  /**
   * Create a new read stream
   * @param is the input stream to transform
   * @param vertx the Vert.x instance
   */
  public InputStreamReadStream(InputStream is, Vertx vertx) {
    this.is = is;
    this.vertx = vertx;
  }

  private void handleException(Throwable t) {
    if (exceptionHandler != null) {
      exceptionHandler.handle(t);
    } else {
      log.error("Unhandled exception", t);
    }
  }
  
  private synchronized void handleData(Buffer buffer) {
    if (dataHandler != null) {
      dataHandler.handle(buffer);
    }
  }

  private synchronized void handleEnd() {
    if (endHandler != null) {
      endHandler.handle(null);
    }
  }
  
  private void doRead() {
    if (!readInProgress) {
      readInProgress = true;
      vertx.<Buffer>executeBlocking(f -> {
        byte[] buf = new byte[READ_BUFFER_SIZE];
        int read;
        try {
          read = is.read(buf, 0, READ_BUFFER_SIZE);
        } catch (IOException e) {
          f.fail(e);
          return;
        }
        Buffer r;
        if (read < 0) {
          r = Buffer.buffer();
        } else if (read < READ_BUFFER_SIZE) {
          r = Buffer.buffer(read);
          r.setBytes(0, buf, 0, read);
        } else {
          r = Buffer.buffer(buf);
        }
        f.complete(r);
      }, ar -> {
        if (ar.failed()) {
          handleException(ar.cause());
        } else {
          readInProgress = false;
          Buffer buffer = ar.result();
          if (buffer.length() == 0) {
            handleEnd();
          } else {
            handleData(buffer);
            if (!paused && dataHandler != null) {
              doRead();
            }
          }
        }
      });
    }
  }
  
  @Override
  public InputStreamReadStream exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public ReadStream<Buffer> handler(Handler<Buffer> handler) {
    dataHandler = handler;
    if (dataHandler != null && !paused && !closed) {
      doRead();
    }
    return this;
  }

  @Override
  public InputStreamReadStream pause() {
    paused = true;
    return this;
  }

  @Override
  public InputStreamReadStream resume() {
    if (paused && !closed) {
      paused = false;
      if (dataHandler != null) {
        doRead();
      }
    }
    return this;
  }

  @Override
  public InputStreamReadStream endHandler(Handler<Void> handler) {
    this.endHandler = handler;
    return this;
  }
  
  /**
   * Close the read stream
   * @param handler will be called when the operation has finished (may be null)
   */
  public void close(Handler<AsyncResult<Void>> handler) {
    if (handler == null) {
      handler = ar -> {
        if (ar.failed()) {
          handleException(ar.cause());
        }
      };
    }
    
    closed = true;
    vertx.<Void>executeBlocking(f -> {
      try {
        is.close();
      } catch (IOException e) {
        f.fail(e);
        return;
      }
      f.complete();
    }, handler);
  }
}
