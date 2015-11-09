package io.georocket.util;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test {@link WindowPipeStream}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class WindowPipeStreamTest {
  private final static Buffer DATA = Buffer.buffer("DATA");
  
  /**
   * Test the general functionality of the {@link WindowPipeStream}
   * @param context the test context
   */
  @Test
  public void simple(TestContext context) {
    WindowPipeStream wps = new WindowPipeStream();
    wps.exceptionHandler(e -> context.fail(e));
    wps.write(DATA);
    context.assertTrue(wps.writeQueueFull());
    byte[] bytes = wps.getWindow().getBytes(0, DATA.length());
    context.assertTrue(Arrays.equals(bytes, DATA.getBytes()));
    Async async1 = context.async();
    wps.handler(data -> {
      context.assertTrue(Arrays.equals(data.getBytes(), DATA.getBytes()));
      context.assertFalse(wps.writeQueueFull());
      async1.complete();
    });
    Async async2 = context.async();
    wps.drainHandler(v -> {
      context.assertFalse(wps.writeQueueFull());
      async2.complete();
    });
  }
}
