package io.georocket.client;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;

/**
 * A read stream from which merged chunks can be read
 * @since 1.3.0
 * @author Michel Kraemer
 */
public class SearchReadStream implements ReadStream<Buffer> {
  private static final Logger log = LoggerFactory.getLogger(SearchReadStream.class);

  private final HttpClientResponse delegate;
  private Handler<Throwable> exceptionHandler;

  /**
   * Create a new read stream
   * @param delegate the actual read stream to delegate to
   */
  public SearchReadStream(HttpClientResponse delegate) {
    this.delegate = delegate;
  }

  @Override
  public SearchReadStream exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    delegate.exceptionHandler(handler);
    return this;
  }

  @Override
  public SearchReadStream handler(Handler<Buffer> handler) {
    delegate.handler(handler);
    return this;
  }

  @Override
  public SearchReadStream pause() {
    delegate.pause();
    return this;
  }

  @Override
  public SearchReadStream resume() {
    delegate.resume();
    return this;
  }

  @Override
  public SearchReadStream endHandler(Handler<Void> endHandler) {
    endHandlerWithResult(sr -> endHandler.handle(null));
    return this;
  }

  /**
   * Set an end handler. Once the stream has ended, and there is no more data
   * to be read, this handler will be called. The handler will receive a
   * result object with information that is only available after the stream has
   * ended.
   * @return a reference to this, so the API can be used fluently
   */
  public SearchReadStream endHandlerWithResult(Handler<SearchReadStreamResult> endHandler) {
    delegate.endHandler(v -> {
      String strUnmergedChunks = delegate.getTrailer("GeoRocket-Unmerged-Chunks");
      long unmergedChunks = 0;
      if (strUnmergedChunks != null) {
        try {
          unmergedChunks = Long.parseLong(strUnmergedChunks);
        } catch (NumberFormatException e) {
          if (exceptionHandler != null) {
            exceptionHandler.handle(e);
          } else {
            log.error("Unhandled exception", e);
          }
          return;
        }
      }
      endHandler.handle(new SearchReadStreamResult(unmergedChunks));
    });
    return this;
  }
}
