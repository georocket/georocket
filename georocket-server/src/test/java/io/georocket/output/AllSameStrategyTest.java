package io.georocket.output;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.BufferWriteStream;
import io.georocket.storage.ChunkMeta;
import io.georocket.util.XMLStartElement;
import io.georocket.util.io.DelegateChunkReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test {@link AllSameStrategy}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class AllSameStrategyTest {
  private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  
  private Buffer chunk1 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"1\"></test></root>");
  private Buffer chunk2 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"2\"></test></root>");
  private ChunkMeta cm = new ChunkMeta(Arrays.asList(new XMLStartElement("root")),
      XMLHEADER.length() + 6, chunk1.length() - 7);
  
  /**
   * Test a simple merge
   * @param context the test context
   */
  @Test
  public void simple(TestContext context) {
    Async async = context.async();
    MergeStrategy strategy = new AllSameStrategy();
    BufferWriteStream bws = new BufferWriteStream();
    strategy.init(cm, context.asyncAssertSuccess(v1 -> {
      strategy.init(cm, context.asyncAssertSuccess(v2 -> {
        strategy.merge(new DelegateChunkReadStream(chunk1), cm, bws, context.asyncAssertSuccess(v3 -> {
          strategy.merge(new DelegateChunkReadStream(chunk2), cm, bws, context.asyncAssertSuccess(v4 -> {
            strategy.finishMerge(bws);
            context.assertEquals(XMLHEADER + "<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>",
                bws.getBuffer().toString("utf-8"));
            async.complete();
          }));
        }));
      }));
    }));
  }
  
  /**
   * Test if chunks that have not been passed to the initalize method can be merged
   * @param context the test context
   */
  @Test
  public void mergeUninitialized(TestContext context) {
    Async async = context.async();
    MergeStrategy strategy = new AllSameStrategy();
    BufferWriteStream bws = new BufferWriteStream();
    strategy.init(cm, context.asyncAssertSuccess(v1 -> {
      // skip second init
      strategy.merge(new DelegateChunkReadStream(chunk1), cm, bws, context.asyncAssertSuccess(v3 -> {
        strategy.merge(new DelegateChunkReadStream(chunk2), cm, bws, context.asyncAssertSuccess(v4 -> {
          strategy.finishMerge(bws);
          context.assertEquals(XMLHEADER + "<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>",
              bws.getBuffer().toString("utf-8"));
          async.complete();
        }));
      }));
    }));
  }
  
  /**
   * Test if canMerge works correctly
   * @param context the test context
   */
  @Test
  public void canMerge(TestContext context) {
    ChunkMeta cm2 = new ChunkMeta(Arrays.asList(new XMLStartElement("other")), 10, 20);
    ChunkMeta cm3 = new ChunkMeta(Arrays.asList(new XMLStartElement("pre", "root")), 10, 20);
    ChunkMeta cm4 = new ChunkMeta(Arrays.asList(new XMLStartElement(null, "root",
        new String[] { "" }, new String[] { "uri" })), 10, 20);
    
    Async async = context.async();
    MergeStrategy strategy = new AllSameStrategy();
    strategy.canMerge(cm, context.asyncAssertSuccess(canMerge1 -> {
      context.assertTrue(canMerge1);
      strategy.init(cm, context.asyncAssertSuccess(v1 -> {
        strategy.canMerge(cm, context.asyncAssertSuccess(canMerge2 -> {
          context.assertTrue(canMerge2);
          strategy.canMerge(cm2, context.asyncAssertSuccess(canMerge3 -> {
            context.assertFalse(canMerge3);
            strategy.canMerge(cm3, context.asyncAssertSuccess(canMerge4 -> {
              context.assertFalse(canMerge4);
              strategy.canMerge(cm4, context.asyncAssertSuccess(canMerge5 -> {
                context.assertFalse(canMerge5);
                async.complete();
              }));
            }));
          }));
        }));
      }));
    }));
  }
  
  /**
   * Test if the merge method fails if it is called with an unexpected chunk
   * @param context the test context
   */
  @Test
  public void mergeFail(TestContext context) {
    ChunkMeta cm2 = new ChunkMeta(Arrays.asList(new XMLStartElement("other")), 10, 20);
    
    Async async = context.async();
    MergeStrategy strategy = new AllSameStrategy();
    BufferWriteStream bws = new BufferWriteStream();
    strategy.init(cm, context.asyncAssertSuccess(v1 -> {
      strategy.merge(new DelegateChunkReadStream(chunk2), cm2, bws, context.asyncAssertFailure(err -> {
        async.complete();
      }));
    }));
  }
}
