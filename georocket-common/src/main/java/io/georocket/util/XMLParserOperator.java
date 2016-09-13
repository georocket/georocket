package io.georocket.util;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;

import io.vertx.core.buffer.Buffer;
import rx.Observable.Operator;
import rx.Subscriber;

/**
 * A reusable operator function that you can lift into an RxJava
 * {@link rx.Observable}. It transforms {@link Buffer}s into {@link XMLStreamEvent}s.
 * @author Michel Kraemer
 */
public class XMLParserOperator implements Operator<XMLStreamEvent, Buffer> {
  private AsyncXMLInputFactory xmlInputFactory = new InputFactoryImpl();
  private AsyncXMLStreamReader<AsyncByteArrayFeeder> xmlParser =
      xmlInputFactory.createAsyncForByteArray();

  private void processEvents(Subscriber<? super XMLStreamEvent> s) {
    while (true) {
      // read next token
      int event;
      try {
        event = xmlParser.next();
      } catch (XMLStreamException e) {
        s.onError(e);
        break;
      }
      
      if (event == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
        // wait for more input
        break;
      } else if (event == AsyncXMLStreamReader.END_DOCUMENT) {
        try {
          xmlParser.close();
          s.onCompleted();
        } catch (XMLStreamException e) {
          s.onError(e);
        }
        break;
      }
  
      // create stream event
      int pos = xmlParser.getLocation().getCharacterOffset();
      XMLStreamEvent e = new XMLStreamEvent(event, pos, xmlParser);
      s.onNext(e);
    }
  }
  
  @Override
  public Subscriber<? super Buffer> call(Subscriber<? super XMLStreamEvent> s) {
    return new Subscriber<Buffer>(s) {
      @Override
      public void onCompleted() {
        if (!s.isUnsubscribed()) {
          // finish parsing
          xmlParser.getInputFeeder().endOfInput();
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
        
        // push bytes into the parser and then process XML events
        try {
          byte[] bytes = buf.getBytes();
          xmlParser.getInputFeeder().feedInput(bytes, 0, bytes.length);
          processEvents(s);
        } catch (XMLStreamException e) {
          s.onError(e);
        }
      }
    };
  }
}
