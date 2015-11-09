package io.georocket.util;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import javax.xml.stream.events.XMLEvent;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.streams.Pump;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test {@link XMLPipeStream}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class XMLPipeStreamTest {
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  /**
   * Create a temporary folder
   */
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
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
      new XMLStreamEvent(XMLEvent.END_ELEMENT, 62, null),
      new XMLStreamEvent(XMLEvent.END_DOCUMENT, 69, null)
  );
  
  private XMLPipeStream xs;
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
    xs = new XMLPipeStream(rule.vertx());
    expectedEvents = new ArrayDeque<>(EXPECTED_EVENTS);
  }
  
  /**
   * Check if the write queue is full after write
   * @param context the test context
   */
  @Test
  public void writeQueueFull(TestContext context) {
    xs.write(Buffer.buffer(XML));
    context.assertTrue(xs.writeQueueFull());
  }
  
  /**
   * Test if a simple XML string can be parsed
   * @param context the test context
   */
  @Test
  public void parseSimple(TestContext context) {
    Async async = context.async();
    xs.exceptionHandler(t -> context.fail(t));
    xs.write(Buffer.buffer(XML));
    xs.handler(e -> handler(e, expectedEvents, context));
    xs.endHandler(v -> endHandler(async, expectedEvents, context));
    xs.drainHandler(v -> xs.close());
  }
  
  /**
   * Test if a simple XML string can be parsed when split into two chunks
   * @param context the test context
   */
  @Test
  public void parseChunks(TestContext context) {
    Async async = context.async();
    xs.exceptionHandler(t -> context.fail(t));
    xs.write(Buffer.buffer(XML_CHUNK1));
    context.assertTrue(xs.writeQueueFull());
    xs.handler(e -> handler(e, expectedEvents, context));
    xs.endHandler(v -> endHandler(async, expectedEvents, context));
    xs.drainHandler(v1 -> {
      xs.write(Buffer.buffer(XML_CHUNK2));
      xs.drainHandler(v2 -> xs.close());
    });
  }
  
  /**
   * Test if a simple XML string can be parsed when split into arbitrary chunks
   * @param context the test context
   */
  @Test
  public void parseIncompleteElements(TestContext context) {
    Async async = context.async();
    xs.exceptionHandler(t -> context.fail(t));
    xs.write(Buffer.buffer(XML.substring(0, XMLHEADER.length() + 3)));
    context.assertTrue(xs.writeQueueFull());
    xs.handler(e -> handler(e, expectedEvents, context));
    xs.endHandler(v -> endHandler(async, expectedEvents, context));
    xs.drainHandler(v1 -> {
      xs.write(Buffer.buffer(XML.substring(XMLHEADER.length() + 3, 64)));
      xs.drainHandler(v2 -> {
        xs.write(Buffer.buffer(XML.substring(64)));
        xs.drainHandler(v3 -> xs.close());
      });
    });
  }
  
  /**
   * Test what happens if the input data is written after setting the data handler
   * @param context the test context
   */
  @Test
  public void writeAfterHandler(TestContext context) {
    Async async = context.async();
    xs.exceptionHandler(t -> context.fail(t));
    xs.handler(e -> handler(e, expectedEvents, context));
    xs.write(Buffer.buffer(XML));
    xs.endHandler(v -> endHandler(async, expectedEvents, context));
    xs.drainHandler(v -> xs.close());
  }
  
  /**
   * Test what happens if the input data is written after setting the end handler
   * @param context the test context
   */
  @Test
  public void writeAfterEndHandler(TestContext context) {
    Async async = context.async();
    xs.exceptionHandler(t -> context.fail(t));
    xs.write(Buffer.buffer(XML));
    xs.endHandler(v -> endHandler(async, expectedEvents, context));
    xs.handler(e -> handler(e, expectedEvents, context));
    xs.drainHandler(v -> xs.close());
  }
  
  /**
   * Test what happens if the input data is written after setting the drain handler
   * @param context the test context
   */
  @Test
  public void writeAfterDrainHandler(TestContext context) {
    Async async = context.async();
    xs.exceptionHandler(t -> context.fail(t));
    xs.endHandler(v -> endHandler(async, expectedEvents, context));
    xs.handler(e -> handler(e, expectedEvents, context));
    xs.drainHandler(v -> xs.close());
    xs.write(Buffer.buffer(XML));
  }
  
  /**
   * Test if the {@link XMLPipeStream} can be used with a {@link Pump}
   * @param context the test context
   * @throws Exception if an error occurs
   */
  @Test
  public void pump(TestContext context) throws Exception {
    Async async = context.async();
    
    File f = folder.newFile();
    FileSystem fs = rule.vertx().fileSystem();
    fs.writeFile(f.getAbsolutePath(), Buffer.buffer(XML), context.asyncAssertSuccess(v1 -> {
      fs.open(f.getAbsolutePath(), new OpenOptions(), context.asyncAssertSuccess(asyncFile -> {
        Pump.pump(asyncFile, xs).start();
        xs.exceptionHandler(t -> context.fail(t));
        xs.endHandler(v -> endHandler(async, expectedEvents, context));
        xs.handler(e -> handler(e, expectedEvents, context));
        asyncFile.endHandler(v2 ->  {
          asyncFile.close();
          xs.close();
        });
      }));
    }));
  }
}
