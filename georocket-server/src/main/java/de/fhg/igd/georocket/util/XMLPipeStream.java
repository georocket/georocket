package de.fhg.igd.georocket.util;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

/**
 * A {@link PipeStream} that converts {@link Buffer}s to {@link XMLStreamEvent}s.
 * In other words it parses an XML stream.
 * @author Michel Kraemer
 */
public class XMLPipeStream extends PipeStream<Buffer, XMLStreamEvent> {
  private final AsyncXMLStreamReader<AsyncByteArrayFeeder> xmlParser;
  
  private final Vertx vertx;
  
  /**
   * Constructs the stream
   * @param vertx the Vertx instance
   */
  public XMLPipeStream(Vertx vertx) {
    AsyncXMLInputFactory xmlInputFactory = new InputFactoryImpl();
    xmlParser = xmlInputFactory.createAsyncForByteArray();
    this.vertx = vertx;
  }
  
  @Override
  protected void doRead() {
    while (!paused && dataHandler != null) {
      // read next token
      int event;
      try {
        event = xmlParser.next();
      } catch (XMLStreamException e) {
        handleException(e);
        break;
      }
      
      if (event == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
        // wait for more input
        doDrain();
        break;
      }
    
      // forward token
      int pos = xmlParser.getLocation().getCharacterOffset();
      XMLStreamEvent e = new XMLStreamEvent(event, pos, xmlParser);
      dataHandler.handle(e);
      if (event == XMLEvent.END_DOCUMENT) {
        handleEnd();
        break;
      }
    }
  }
  
  @Override
  protected void doWrite(Buffer data, Handler<AsyncResult<Void>> handler) {
    try {
      byte[] bytes = data.getBytes();
      xmlParser.getInputFeeder().feedInput(bytes, 0, bytes.length);
      handler.handle(Future.succeededFuture());
    } catch (XMLStreamException e) {
      handler.handle(Future.failedFuture(e));
    }
  }
  
  @Override
  protected void doDrain() {
    if (xmlParser.getInputFeeder().needMoreInput()) {
      super.doDrain();
    }
  }
  
  @Override
  public boolean writeQueueFull() {
    return super.writeQueueFull() || !xmlParser.getInputFeeder().needMoreInput();
  }
  
  /**
   * Closes the stream
   */
  public void close() {
    vertx.runOnContext(v -> {
      xmlParser.getInputFeeder().endOfInput();
      doRead();
    });
  }
}
