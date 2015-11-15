package io.georocket.util;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;

import io.vertx.core.buffer.Buffer;
import rx.Observable;

/**
 * An asynchronous XML parser that can be fed with data and emits
 * {@link XMLStreamEvent}s
 * @author Michel Kraemer
 */
public class AsyncXMLParser {
  private AsyncXMLInputFactory xmlInputFactory = new InputFactoryImpl();
  private AsyncXMLStreamReader<AsyncByteArrayFeeder> xmlParser =
      xmlInputFactory.createAsyncForByteArray();
  
  /**
   * Feed the parser with data and create an observable that emits
   * {@link XMLStreamEvent}s until all data has been consumed
   * @param data the data
   * @return the observable
   */
  public Observable<XMLStreamEvent> feed(Buffer data) {
    try {
      byte[] bytes = data.getBytes();
      xmlParser.getInputFeeder().feedInput(bytes, 0, bytes.length);
    } catch (XMLStreamException e) {
      return Observable.error(e);
    }
    
    // consume all data
    return Observable.create(subscriber -> {
      while (true) {
        // read next token
        int event;
        try {
          event = xmlParser.next();
        } catch (XMLStreamException e) {
          subscriber.onError(e);
          break;
        }
        
        if (event == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
          // wait for more input
          subscriber.onCompleted();
          break;
        }
    
        // create stream event
        int pos = xmlParser.getLocation().getCharacterOffset();
        XMLStreamEvent e = new XMLStreamEvent(event, pos, xmlParser);
        subscriber.onNext(e);
      }
    });
  }
  
  /**
   * Close the parser and release all resources
   * @return an observable that emits when the parser has been closed
   */
  public Observable<Void> close() {
    try {
      xmlParser.close();
      return Observable.just(null);
    } catch (XMLStreamException e) {
      return Observable.error(e);
    }
  }
}
