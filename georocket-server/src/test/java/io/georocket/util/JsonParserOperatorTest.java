package io.georocket.util;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.undercouch.actson.JsonEvent;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import rx.Observable;

/**
 * Test {@link JsonParserOperator}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class JsonParserOperatorTest {
  /**
   * Test input data
   */
  private static final String JSON_CHUNK1 = "{\"name\":\"";
  private static final String JSON_CHUNK2 = "Elvis\"}";
  private static final String JSON = JSON_CHUNK1 + JSON_CHUNK2;
  
  /**
   * The expected events
   */
  private static final List<JsonStreamEvent> EXPECTED_EVENTS = Arrays.asList(
      new JsonStreamEvent(JsonEvent.START_OBJECT, 1),
      new JsonStreamEvent(JsonEvent.FIELD_NAME, 7),
      new JsonStreamEvent(JsonEvent.VALUE_STRING, 15),
      new JsonStreamEvent(JsonEvent.END_OBJECT, 16),
      new JsonStreamEvent(JsonEvent.EOF, 16)
  );
  
  private Deque<JsonStreamEvent> expectedEvents;
  
  private static void handler(JsonStreamEvent e, Deque<JsonStreamEvent> expectedEvents, TestContext context) {
    JsonStreamEvent expected = expectedEvents.pop();
    context.assertEquals(expected.getEvent(), e.getEvent());
    context.assertEquals(expected.getPos(), e.getPos());
  }
  
  private static void endHandler(Async async, Deque<JsonStreamEvent> expectedEvents, TestContext context) {
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
   * Test if a simple JSON string can be parsed
   * @param context the test context
   */
  @Test
  public void parseSimple(TestContext context) {
    Async async = context.async();
    Observable.just(Buffer.buffer(JSON))
      .lift(new JsonParserOperator())
      .doOnNext(e -> handler(e, expectedEvents, context))
      .last()
      .subscribe(r -> {
        endHandler(async, expectedEvents, context);
      }, err -> {
        context.fail(err);
      });
  }
  
  /**
   * Test if a simple JSON string can be parsed when split into two chunks
   * @param context the test context
   */
  @Test
  public void parseChunks(TestContext context) {
    Async async = context.async();
    Observable.just(Buffer.buffer(JSON_CHUNK1), Buffer.buffer(JSON_CHUNK2))
      .lift(new JsonParserOperator())
      .doOnNext(e -> handler(e, expectedEvents, context))
      .last()
      .subscribe(r -> {
        endHandler(async, expectedEvents, context);
      }, err -> {
        context.fail(err);
      });
  }
  
  /**
   * Test if a simple JSON string can be parsed when split into arbitrary chunks
   * @param context the test context
   */
  @Test
  public void parseIncompleteElements(TestContext context) {
    Async async = context.async();
    Observable.just(Buffer.buffer(JSON.substring(0, 3)),
        Buffer.buffer(JSON.substring(3, 12)),
        Buffer.buffer(JSON.substring(12)))
      .lift(new JsonParserOperator())
      .doOnNext(e -> handler(e, expectedEvents, context))
      .last()
      .subscribe(r -> {
        endHandler(async, expectedEvents, context);
      }, err -> {
        context.fail(err);
      });
  }
}
