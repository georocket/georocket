package io.georocket.util;

import de.undercouch.actson.JsonEvent;
import de.undercouch.actson.JsonParser;
import io.vertx.core.buffer.Buffer;
import rx.Observable.Operator;
import rx.Subscriber;

/**
 * A reusable operator function that you can lift into an RxJava
 * {@link rx.Observable}. It transforms byte arrays into JSON events.
 * @author Michel Kraemer
 */
public class JsonParserOperator implements Operator<JsonStreamEvent, Buffer> {
  /**
   * Non-blocking JSON parser
   */
  private JsonParser parser = new JsonParser();

  /**
   * Process events from the parser until it needs more input. Notify the
   * subscriber accordingly.
   * @param s the subscriber
   * @return true if the caller should continue parsing, false if there was an
   * error or if the end of the JSON text has been reached
   */
  private boolean processEvents(Subscriber<? super JsonStreamEvent> s) {
    int event;
    do {
      event = parser.nextEvent();

      // handle event and notify subscriber
      if (event == JsonEvent.EOF) {
        // notify the subscriber that the observable has finished
        s.onNext(new JsonStreamEvent(event, parser.getParsedCharacterCount()));
        s.onCompleted();
        return false;
      } else if (event == JsonEvent.ERROR) {
        // notify the subscriber about the error
        s.onError(new IllegalStateException("Syntax error"));
        return false;
      } else if (event != JsonEvent.NEED_MORE_INPUT) {
        // forward JSON event
        s.onNext(new JsonStreamEvent(event, parser.getParsedCharacterCount()));
      }
    } while (event != JsonEvent.NEED_MORE_INPUT);

    return true;
  }

  @Override
  public Subscriber<? super Buffer> call(Subscriber<? super JsonStreamEvent> s) {
    return new Subscriber<Buffer>(s) {
      @Override
      public void onCompleted() {
        if (!s.isUnsubscribed()) {
          // finish parsing and forward events to the
          // subscriber (including the EOF event)
          parser.getFeeder().done();
          processEvents(s);
        }
      }

      @Override
      public void onError(Throwable e) {
        if (!s.isUnsubscribed()) {
          s.onError(e);
        }
      }

      @Override
      public void onNext(Buffer buf) {
        if (s.isUnsubscribed()) {
          return;
        }

        // push bytes into the parser and then process JSON events
        byte[] bytes = buf.getBytes();
        int i = 0;
        while (i < bytes.length) {
          i += parser.getFeeder().feed(bytes, i, bytes.length - i);
          if (!processEvents(s)) {
            break;
          }
        }
      }
    };
  }
}
