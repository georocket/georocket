package io.georocket.index;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test {@link IndexableChunkCache}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class IndexableChunkCacheTest {
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  /**
   * Test if a chunk is removed from the cache once it has been retrieved
   * @param ctx the current test context
   */
  @Test
  public void get(TestContext ctx) {
    IndexableChunkCache c = new IndexableChunkCache();
    String path = "path";
    Buffer chunk = Buffer.buffer("CHUNK");
    c.put(path, chunk);
    ctx.assertEquals(1L, c.getNumberOfChunks());
    ctx.assertEquals((long)chunk.length(), c.getSize());
    ctx.assertEquals(chunk, c.get(path));
    ctx.assertEquals(0L, c.getNumberOfChunks());
    ctx.assertEquals(0L, c.getSize());
    ctx.assertNull(c.get(path));
  }

  /**
   * Test if a chunk is removed from the cache after a certain amount of seconds
   * @param ctx the current test context
   */
  @Test
  public void timeout(TestContext ctx) {
    IndexableChunkCache c = new IndexableChunkCache(1024, 1);
    String path = "path";
    Buffer chunk = Buffer.buffer("CHUNK");
    c.put(path, chunk);
    Async async = ctx.async();
    rule.vertx().setTimer(1100, l -> {
      // We need to call the get() method at least 64 times so the internal
      // Guava cache completely evicts the expired item and calls our removal
      // listener that updates the cache size.
      // See com.google.common.cache.LocalCache#DRAIN_THRESHOLD
      for (int i = 0; i < 64; ++i) {
        ctx.assertNull(c.get(path));
      }
      ctx.assertEquals(0L, c.getSize());
      async.complete();
    });
  }

  /**
   * Test if a chunk is removed from the cache if it exceeds the maximum size
   * @param ctx the current test context
   */
  @Test
  public void maxSize(TestContext ctx) {
    IndexableChunkCache c = new IndexableChunkCache(1024, 60);
    String path1 = "path1";
    String path2 = "path2";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1023; ++i) {
      sb.append((char)('a' + (i % 26)));
    }
    Buffer chunk1 = Buffer.buffer(sb.toString() + "1");
    Buffer chunk2 = Buffer.buffer(sb.toString() + "2");
    c.put(path1, chunk1);
    c.put(path2, chunk2);
    ctx.assertEquals((long)chunk1.length(), c.getSize());
    ctx.assertEquals(1L, c.getNumberOfChunks());
    ctx.assertNull(c.get(path2));
    ctx.assertEquals((long)chunk1.length(), c.getSize());
    ctx.assertEquals(1L, c.getNumberOfChunks());
    ctx.assertEquals(chunk1, c.get(path1));
    ctx.assertEquals(0L, c.getSize());
    ctx.assertEquals(0L, c.getNumberOfChunks());
  }

  /**
   * Test if a second chunk can be added after the first has been evicted due to
   * timeout if the sizes of both chunks would exceed the cache's maximum size
   * @param ctx the current test context
   */
  @Test
  public void maxSizeAndTimeout(TestContext ctx) {
    IndexableChunkCache c = new IndexableChunkCache(1024, 1);
    String path1 = "path1";
    String path2 = "path2";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1023; ++i) {
      sb.append((char)('a' + (i % 26)));
    }
    Buffer chunk1 = Buffer.buffer(sb.toString());
    Buffer chunk2 = Buffer.buffer(sb.toString() + "2");
    c.put(path1, chunk1);
    Async async = ctx.async();
    rule.vertx().setTimer(1100, l -> {
      ctx.assertNull(c.get(path1));
      c.put(path2, chunk2);
      ctx.assertEquals((long)chunk2.length(), c.getSize());
      ctx.assertEquals(1L, c.getNumberOfChunks());
      ctx.assertEquals(chunk2, c.get(path2));
      ctx.assertEquals(0L, c.getSize());
      ctx.assertEquals(0L, c.getNumberOfChunks());
      async.complete();
    });
  }
}
