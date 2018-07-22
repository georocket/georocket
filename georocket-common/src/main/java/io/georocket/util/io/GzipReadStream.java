package io.georocket.util.io;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;

import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

/**
 * <p>A {@link ReadStream} that delegates to another one but decompresses all
 * data with GZIP.</p>
 * <p>The code is loosely based on {@link java.util.zip.GZIPInputStream}</p>
 * @author Michel Kraemer
 */
public class GzipReadStream extends DelegateReadStream<Buffer> {
  private static final Logger log = LoggerFactory.getLogger(GzipReadStream.class);

  private final static int FHCRC      = 2;    // Header CRC
  private final static int FEXTRA     = 4;    // Extra field
  private final static int FNAME      = 8;    // File name
  private final static int FCOMMENT   = 16;   // File comment

  private final Inflater inflater;
  private final CRC32 crc;
  private final byte[] buf;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> endHandler;
  private boolean headerRead;
  private Buffer headerBuffer;
  private Buffer trailerBuffer;

  /**
   * Creates new stream that wraps around another one
   * @param delegate the stream to wrap around
   */
  public GzipReadStream(ReadStream<Buffer> delegate) {
    super(delegate);
    inflater = new Inflater(true);
    crc = new CRC32();
    buf = new byte[512];
  }

  @Override
  public GzipReadStream exceptionHandler(Handler<Throwable> handler) {
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
  public GzipReadStream handler(Handler<Buffer> handler) {
    if (handler == null) {
      delegate.handler(null);
      return this;
    }

    delegate.handler(data -> {
      int start = 0;

      if (!headerRead) {
        // append buf to data from previous calls (if there is any)
        if (headerBuffer != null) {
          headerBuffer.appendBuffer(data);
          data = headerBuffer;
        }

        // try to parse header
        int headerSize = tryParseHeader(data);
        if (headerSize > 0) {
          // header was parsed successfully. skip bytes.
          headerRead = true;
          start = headerSize;
          headerBuffer = null;
        } else {
          // save buf for next call
          if (headerBuffer == null) {
            headerBuffer = Buffer.buffer();
            headerBuffer.appendBuffer(data);
          }
          return; // wait for next call
        }
      }

      if (trailerBuffer != null) {
        // save remaining bytes to parse the trailer
        trailerBuffer.appendBuffer(data);
        tryParseTrailer();
        return;
      }

      final int finalStart = start;
      final Buffer finalData = data;
      Vertx.currentContext().<Buffer>executeBlocking(f -> {
        byte[] currentData = finalData.getBytes(finalStart, finalData.length());
        try {
          Buffer r = Buffer.buffer();
          while (true) {
            int n;
            while ((n = inflater.inflate(buf, 0, buf.length)) == 0) {
              if (inflater.finished() || inflater.needsDictionary()) {
                int remaining = inflater.getRemaining();
                if (remaining > 0) {
                  // save remaining bytes to parse the trailer
                  trailerBuffer = finalData.getBuffer(finalData.length() - remaining,
                    finalData.length());
                }
                break;
              }
              if (inflater.needsInput()) {
                if (currentData != null) {
                  inflater.setInput(currentData, 0, currentData.length);
                  currentData = null;
                } else {
                  // wait for more data
                  break;
                }
              }
            }
            if (n == 0) {
              break;
            }
            crc.update(buf, 0, n);
            r.appendBytes(buf, 0, n);
          }
          f.complete(r);
        } catch (DataFormatException e) {
          f.fail(e);
        }
      }, ar -> {
        if (ar.failed()) {
          handleException(ar.cause());
          return;
        }

        // forward uncompressed data
        Buffer b = ar.result();
        if (b != null && b.length() > 0) {
          handler.handle(b);
        }

        // try to parse trailer if we're already at the end of the file
        if (trailerBuffer != null) {
          tryParseTrailer();
        }
      });
    });

    return this;
  }

  /**
   * Try to parse a GZIP header from the given buffer
   * @param buf the buffer to parse
   * @return the size of the header or 0 if the buffer was not large enough
   * or invalid
   */
  private int tryParseHeader(Buffer buf) {
    // check if the header is large enough for mandatory fields
    if (buf.length() < 10) {
      return 0;
    }

    // Check header magic
    if (buf.getUnsignedShortLE(0) != GZIPInputStream.GZIP_MAGIC) {
      handleException(new ZipException("Not in GZIP format"));
      return 0;
    }

    // Check compression method
    if (buf.getByte(2) != 8) {
      handleException(new ZipException("Unsupported compression method"));
      return 0;
    }

    // Read flags
    int flg = buf.getByte(3);

    int n = 2 + 2 + 6;

    // Skip optional extra field
    if ((flg & FEXTRA) == FEXTRA) {
      if (buf.length() < n + 2) {
        return 0;
      }
      int m = buf.getUnsignedShortLE(n);
      n += m + 2;
    }

    // Skip optional file name
    if ((flg & FNAME) == FNAME) {
      do {
        if (buf.length() <= n) {
          return 0;
        }
        n++;
      } while (buf.getByte(n - 1) != 0);
    }

    // Skip optional file comment
    if ((flg & FCOMMENT) == FCOMMENT) {
      do {
        if (buf.length() <= n) {
          return 0;
        }
        n++;
      } while (buf.getByte(n - 1) != 0);
    }

    // Check optional header CRC
    if ((flg & FHCRC) == FHCRC) {
      if (buf.length() < n + 2) {
        return 0;
      }
      crc.reset();
      crc.update(buf.getBytes(0, n));
      int v = (int)crc.getValue() & 0xffff;
      if (buf.getUnsignedShortLE(n) != v) {
        handleException(new ZipException("Corrupt GZIP header"));
        return 0;
      }
      n += 2;
      crc.reset();
    }

    return n;
  }

  /**
   * Try to parse the trailer and call {@link #endHandler} if successful
   */
  private void tryParseTrailer() {
    if (trailerBuffer.length() < 8) {
      // wait for more data
      return;
    }

    long v = trailerBuffer.getUnsignedIntLE(0);
    long bytesWritten = trailerBuffer.getUnsignedIntLE(4);

    if (v != crc.getValue() || bytesWritten != (inflater.getBytesWritten() & 0xffffffffL)) {
      handleException(new ZipException("Corrupt GZIP trailer"));
      return;
    }

    if (endHandler != null) {
      endHandler.handle(null);
    }
  }

  @Override
  public GzipReadStream endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }
}
