package io.georocket.util;

import de.undercouch.actson.JsonEvent;
import de.undercouch.actson.JsonParser;
import io.vertx.core.buffer.Buffer;
import rx.Observable;
import rx.Observable.Transformer;

import java.util.Iterator;

/**
 * A reusable transformer that you can apply to an RxJava {@link rx.Observable}
 * using {@link rx.Observable#compose(Transformer)}. It transforms
 * {@link Buffer}s into {@link JsonStreamEvent}s.
 * @author Michel Kraemer
 */
public class JsonParserTransformer implements Transformer<Buffer, JsonStreamEvent> {
  private final JsonParser parser = new JsonParser();

  @Override
  public Observable<JsonStreamEvent> call(Observable<Buffer> o) {
    return o
      // first, convert all buffers to JSON events
      .flatMap(buf -> Observable.from(() -> new StreamEventIterator(buf)))
      // finally, close the parser and process remaining events
      .concatWith(Observable.from(() -> {
        parser.getFeeder().done();
        return new StreamEventIterator();
      }));
  }

  /**
   * Feeds {@link #parser} with a buffer and iterates over the generated JSON
   * events.
   */
  private class StreamEventIterator implements Iterator<JsonStreamEvent> {
    /**
     * The bytes to feed to {@link #parser}
     */
    private final byte[] bytes;

    /**
     * The current position within {@link #bytes}
     */
    private int pos = 0;

    /**
     * The next JSON event to return
     */
    private int nextEvent = JsonEvent.NEED_MORE_INPUT;

    /**
     * True if the end of file has been reached
     */
    private boolean eof = false;

    /**
     * Create a new iterator that does not feed any input but just processes
     * remaining events
     */
    public StreamEventIterator() {
      this.bytes = null;
    }

    /**
     * Create a new parser that feeds the given buffer into {@link #parser} and
     * iterates over generated events
     * @param buf the buffer to feed
     */
    public StreamEventIterator(Buffer buf) {
      this.bytes = buf.getBytes();
    }

    @Override
    public boolean hasNext() {
      if (eof) {
        return false;
      }

      // get next event and/or feed parser with more input
      nextEvent = parser.nextEvent();
      while (nextEvent == JsonEvent.NEED_MORE_INPUT) {
        if (bytes == null || pos >= bytes.length) {
          return false;
        }
        pos += parser.getFeeder().feed(bytes, pos, bytes.length - pos);
        nextEvent = parser.nextEvent();
      }

      if (nextEvent == JsonEvent.EOF) {
        eof = true;
      }

      return true;
    }

    @Override
    public JsonStreamEvent next() {
      if (nextEvent == JsonEvent.NEED_MORE_INPUT) {
        throw new IllegalStateException("Not enough input data to parse JSON file");
      }

      if (nextEvent == JsonEvent.EOF) {
        return new JsonStreamEvent(nextEvent, parser.getParsedCharacterCount());
      } else if (nextEvent == JsonEvent.ERROR) {
        throw new IllegalStateException("Syntax error");
      }

      Object value = null;
      if (nextEvent == JsonEvent.VALUE_STRING || nextEvent == JsonEvent.FIELD_NAME) {
        value = parser.getCurrentString();
      } else if (nextEvent == JsonEvent.VALUE_DOUBLE) {
        value = parser.getCurrentDouble();
      } else if (nextEvent == JsonEvent.VALUE_INT) {
        value = parser.getCurrentInt();
      }
      return new JsonStreamEvent(nextEvent, parser.getParsedCharacterCount(), value);
    }
  }
}
