package io.georocket.util.io;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.WriteStream;

import java.util.zip.CRC32;
import java.util.zip.Deflater;

/**
 * <p>A {@link WriteStream} that delegates to another one but compresses all
 * data with GZIP.</p>
 * <p>The code is loosely based on {@link java.util.zip.GZIPOutputStream}</p>
 * @author Michel Kraemer
 * @since 1.3.0
 */
public class GzipWriteStream implements WriteStream<Buffer> {
  private static final Logger log = LoggerFactory.getLogger(GzipWriteStream.class);

  private final WriteStream<Buffer> delegate;
  private final Deflater deflater;
  private final CRC32 crc;
  private final byte[] buf;
  private boolean headerWritten;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;
  private Handler<Void> closeHandler;
  private boolean closed;
  private int writesOutstanding;
  private int maxWrites = 1024 * 1024;

  /**
   * Creates new stream that wraps around another one
   * @param delegate the stream to wrap around
   */
  public GzipWriteStream(WriteStream<Buffer> delegate) {
    this.delegate = delegate;
    deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
    crc = new CRC32();
    buf = new byte[512];
  }

  @Override
  public GzipWriteStream exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    delegate.exceptionHandler(handler);
    return this;
  }

  private void handleException(Throwable t) {
    if (exceptionHandler != null && t instanceof Exception) {
      exceptionHandler.handle(t);
    } else {
      log.error("Unhandled exception", t);
    }
  }

  @Override
  public GzipWriteStream write(Buffer data) {
    if (!headerWritten) {
      headerWritten = true;
      writeHeader();
    }

    // compress the data in a blocking code
    writesOutstanding += data.length();
    Vertx.currentContext().<Buffer>executeBlocking(f -> {
      byte[] bytes = data.getBytes();
      deflater.setInput(bytes, 0, bytes.length);

      Buffer b = Buffer.buffer();
      while (!deflater.needsInput()) {
        deflate(b);
      }
      crc.update(bytes, 0, bytes.length);

      f.complete(b);
    }, ar -> {
      if (ar.failed()) {
        handleException(ar.cause());
        return;
      }

      // forward compressed data to the delegate
      Buffer b = ar.result();
      if (b.length() > 0) {
        delegate.write(b);
        writesOutstanding -= b.length();
      }
      checkDrained();
    });

    return this;
  }

  private void checkDrained() {
    if (drainHandler != null) {
      Handler<Void> handler = drainHandler;
      drainHandler = null;
      handler.handle(null);
    }
  }

  /**
   * Write GZIP header
   */
  private void writeHeader() {
    delegate.write(Buffer.buffer(new byte[] {
        0x1f,              // Magic number (short)
        (byte)0x8b,        // Magic number (short)
        Deflater.DEFLATED, // Compression method (CM)
        0,                 // Flags (FLG)
        0,                 // Modification time MTIME (int)
        0,                 // Modification time MTIME (int)
        0,                 // Modification time MTIME (int)
        0,                 // Modification time MTIME (int)
        0,                 // Extra flags (XFLG)
        0                  // Operating system (OS)
    }));
  }

  @Override
  public void end() {
    // finish compression in a blocking code
    Vertx.currentContext().<Buffer>executeBlocking(f -> {
      deflater.finish();
      Buffer b = Buffer.buffer();
      while (!deflater.finished()) {
        deflate(b);
      }
      f.complete(b);
    }, ar -> {
      if (ar.failed()) {
        handleException(ar.cause());
        return;
      }

      // write trailer
      Buffer b = ar.result();
      b.appendIntLE((int)crc.getValue());
      b.appendIntLE(deflater.getTotalIn());
      delegate.end(b);

      // close compressor
      deflater.end();

      // close stream
      closed = true;
      if (closeHandler != null) {
        Handler<Void> handler = closeHandler;
        closeHandler = null;
        handler.handle(null);
      }
    });
  }

  /**
   * Compress data to a buffer
   * @param b the buffer to append the compressed data to
   */
  private void deflate(Buffer b) {
    int len = deflater.deflate(buf, 0, buf.length);
    if (len > 0) {
      b.appendBytes(buf, 0, len);
    }
  }

  @Override
  public GzipWriteStream setWriteQueueMaxSize(int maxSize) {
    this.maxWrites = maxSize;
    delegate.setWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    if (delegate.writeQueueFull()) {
      return true;
    }
    return writesOutstanding >= maxWrites;
  }

  @Override
  public GzipWriteStream drainHandler(Handler<Void> handler) {
    if (delegate.writeQueueFull()) {
      delegate.drainHandler(handler);
    } else {
      drainHandler = handler;
      checkDrained();
    }
    return this;
  }

  /**
   * Asynchronously close this stream
   */
  public void close() {
    closeInternal(null);
  }

  /**
   * Asynchronously close this stream
   * @param handler the handler that will be called when the
   * stream has been closed
   */
  public void close(Handler<Void> handler) {
    closeInternal(handler);
  }

  private synchronized void closeInternal(Handler<Void> handler) {
    if (closed) {
      handler.handle(null);
    } else {
      closeHandler = handler;
    }
  }
}
