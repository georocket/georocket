package io.georocket.util;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import javax.xml.stream.events.XMLEvent;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import rx.Observable;

/**
 * Test {@link XMLParserOperator}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class XMLParserOperatorTest {
  /**
   * Test input data
   */
  private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  private static final String XML_CHUNK1 = XMLHEADER + "<test>";
  private static final String XML_CHUNK2 = "</test>";
  private static final String XML = XML_CHUNK1 + XML_CHUNK2;
  
  /**
   * The expected events
   */
  private static final List<XMLStreamEvent> EXPECTED_EVENTS = Arrays.asList(
      new XMLStreamEvent(XMLEvent.START_DOCUMENT, 0, null),
      new XMLStreamEvent(XMLEvent.START_ELEMENT, 0, null),
      new XMLStreamEvent(XMLEvent.END_ELEMENT, 62, null)
  );
  
  private Deque<XMLStreamEvent> expectedEvents;
  
  private static void handler(XMLStreamEvent e, Deque<XMLStreamEvent> expectedEvents, TestContext context) {
    XMLStreamEvent expected = expectedEvents.pop();
    context.assertEquals(expected.getEvent(), e.getEvent());
    context.assertEquals(expected.getPos(), e.getPos());
  }
  
  private static void endHandler(Async async, Deque<XMLStreamEvent> expectedEvents, TestContext context) {
    context.assertTrue(expectedEvents.isEmpty());
    async.complete();
  }
  
  /**
   * Create objects required for all tests
   * @param context the Vert.x test context
   */
  @Before
  public void setUp(TestContext context) {
    expectedEvents = new ArrayDeque<>(EXPECTED_EVENTS);
  }
  
  /**
   * Test if a simple XML string can be parsed
   * @param context the test context
   */
  @Test
  public void parseSimple(TestContext context) {
    Async async = context.async();
    Observable.just(Buffer.buffer(XML))
      .lift(new XMLParserOperator())
      .doOnNext(e -> handler(e, expectedEvents, context))
      .last()
      .subscribe(r -> {
        endHandler(async, expectedEvents, context);
      }, err -> {
        context.fail(err);
      });
  }
  
  /**
   * Test if a simple XML string can be parsed when split into two chunks
   * @param context the test context
   */
  @Test
  public void parseChunks(TestContext context) {
    Async async = context.async();
    Observable.just(Buffer.buffer(XML_CHUNK1), Buffer.buffer(XML_CHUNK2))
      .lift(new XMLParserOperator())
      .doOnNext(e -> handler(e, expectedEvents, context))
      .last()
      .subscribe(r -> {
        endHandler(async, expectedEvents, context);
      }, err -> {
        context.fail(err);
      });
  }
  
  /**
   * Test if a simple XML string can be parsed when split into arbitrary chunks
   * @param context the test context
   */
  @Test
  public void parseIncompleteElements(TestContext context) {
    Async async = context.async();
    Observable.just(Buffer.buffer(XML.substring(0, XMLHEADER.length() + 3)),
        Buffer.buffer(XML.substring(XMLHEADER.length() + 3, 64)),
        Buffer.buffer(XML.substring(64)))
      .lift(new XMLParserOperator())
      .doOnNext(e -> handler(e, expectedEvents, context))
      .last()
      .subscribe(r -> {
        endHandler(async, expectedEvents, context);
      }, err -> {
        context.fail(err);
      });
  }
}
