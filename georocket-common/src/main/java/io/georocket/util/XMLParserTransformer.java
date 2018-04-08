package io.georocket.util;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import io.vertx.core.buffer.Buffer;
import rx.Observable;
import rx.Observable.Transformer;

import javax.xml.stream.XMLStreamException;
import java.util.Iterator;

/**
 * A reusable transformer that you can apply to an RxJava {@link Observable}
 * using {@link Observable#compose(Transformer)}. It transforms
 * {@link Buffer}s into {@link XMLStreamEvent}s.
 * @author Michel Kraemer
 */
public class XMLParserTransformer implements Transformer<Buffer, XMLStreamEvent> {
  private AsyncXMLStreamReader<AsyncByteArrayFeeder> parser =
      new InputFactoryImpl().createAsyncForByteArray();

  @Override
  public Observable<XMLStreamEvent> call(Observable<Buffer> o) {
    return o
      // first, convert all buffers to XML events
      .flatMap(buf -> Observable.from(() -> new StreamEventIterator(buf)))
      // finally, close the parser and process remaining events
      .concatWith(Observable.from(() -> {
        parser.getInputFeeder().endOfInput();
        return new StreamEventIterator();
      }));
  }

  /**
   * Feeds {@link #parser} with a buffer and iterates over the generated XML
   * events.
   */
  private class StreamEventIterator implements Iterator<XMLStreamEvent> {
    private XMLStreamEvent nextEvent = null;

    /**
     * Create a new iterator that does not feed any input but just processes
     * remaining events
     */
    public StreamEventIterator() {
      // nothing to do here
    }

    /**
     * Create a new parser that feeds the given buffer into {@link #parser} and
     * iterates over generated events
     * @param buf the buffer to feed
     */
    public StreamEventIterator(Buffer buf) {
      byte[] bytes = buf.getBytes();
      try {
        parser.getInputFeeder().feedInput(bytes, 0, bytes.length);
      } catch (XMLStreamException e) {
        throw new IllegalStateException("Could not feed input", e);
      }
    }

    @Override
    public boolean hasNext() {
      // read next token
      int event;
      try {
        event = parser.next();
      } catch (XMLStreamException e) {
        throw new IllegalStateException("Could not parse input", e);
      }

      if (event == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
        // wait for more input
        return false;
      } else if (event == AsyncXMLStreamReader.END_DOCUMENT) {
        try {
          parser.close();
          return false;
        } catch (XMLStreamException e) {
          throw new IllegalStateException("Could not close input", e);
        }
      }

      // create stream event
      int pos = parser.getLocation().getCharacterOffset();
      nextEvent = new XMLStreamEvent(event, pos, parser);

      return true;
    }

    @Override
    public XMLStreamEvent next() {
      return nextEvent;
    }
  }
}
