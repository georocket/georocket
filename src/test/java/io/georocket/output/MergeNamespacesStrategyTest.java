package io.georocket.output;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.BufferWriteStream;
import io.georocket.SimpleChunkReadStream;
import io.georocket.storage.ChunkMeta;
import io.georocket.util.XMLStartElement;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test {@link MergeNamespacesStrategy}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class MergeNamespacesStrategyTest {
  private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  
  private final static XMLStartElement ROOT1 = new XMLStartElement(null, "root",
      new String[] { "", "ns1", "xsi" },
      new String[] { "uri0", "uri1", "http://www.w3.org/2001/XMLSchema-instance" },
      new String[] { "xsi", "ns1" },
      new String[] { "schemaLocation", "attr1" },
      new String[] { "uri0 location0 uri1 location1", "value1" });
  private final static XMLStartElement ROOT2 = new XMLStartElement(null, "root",
      new String[] { "", "ns2", "xsi" },
      new String[] { "uri0", "uri2", "http://www.w3.org/2001/XMLSchema-instance" },
      new String[] { "xsi", "ns2" },
      new String[] { "schemaLocation", "attr2" },
      new String[] { "uri0 location0 uri2 location2", "value2" });
  private final static XMLStartElement EXPECTEDROOT = new XMLStartElement(null, "root",
      new String[] { "", "ns1", "xsi", "ns2" },
      new String[] { "uri0", "uri1", "http://www.w3.org/2001/XMLSchema-instance", "uri2" },
      new String[] { "xsi", "ns1", "ns2" },
      new String[] { "schemaLocation", "attr1", "attr2" },
      new String[] { "uri0 location0 uri1 location1 uri2 location2", "value1", "value2" });
  
  private final static String CONTENTS1 = "<elem><ns1:child1></ns1:child1></elem>";
  private final static Buffer CHUNK1 = Buffer.buffer(XMLHEADER + ROOT1 + CONTENTS1 + "</" + ROOT1.getName() + ">");
  private final static String CONTENTS2 = "<elem><ns2:child2></ns2:child2></elem>";
  private final static Buffer CHUNK2 = Buffer.buffer(XMLHEADER + ROOT2 + CONTENTS2 + "</" + ROOT2.getName() + ">");
  
  private final static ChunkMeta META1 = new ChunkMeta(Arrays.asList(ROOT1),
      XMLHEADER.length() + ROOT1.toString().length(),
      CHUNK1.length() - ROOT1.getName().length() - 3);
  private final static ChunkMeta META2 = new ChunkMeta(Arrays.asList(ROOT2),
      XMLHEADER.length() + ROOT2.toString().length(),
      CHUNK2.length() - ROOT2.getName().length() - 3);
  
  /**
   * Test a simple merge
   * @param context the test context
   */
  @Test
  public void simple(TestContext context) {
    Async async = context.async();
    MergeStrategy strategy = new MergeNamespacesStrategy();
    BufferWriteStream bws = new BufferWriteStream();
    strategy.init(META1, context.asyncAssertSuccess(v1 -> {
      strategy.init(META2, context.asyncAssertSuccess(v2 -> {
        strategy.merge(new SimpleChunkReadStream(CHUNK1), META1, bws, context.asyncAssertSuccess(v3 -> {
          strategy.merge(new SimpleChunkReadStream(CHUNK2), META2, bws, context.asyncAssertSuccess(v4 -> {
            strategy.finishMerge(bws);
            context.assertEquals(XMLHEADER + EXPECTEDROOT + CONTENTS1 + CONTENTS2 + "</" + EXPECTEDROOT.getName() + ">",
                bws.getBuffer().toString("utf-8"));
            async.complete();
          }));
        }));
      }));
    }));
  }
  
  /**
   * Make sure that chunks that have not been passed to the initalize method cannot be merged
   * @param context the test context
   */
  @Test
  public void mergeUninitialized(TestContext context) {
    Async async = context.async();
    MergeStrategy strategy = new MergeNamespacesStrategy();
    BufferWriteStream bws = new BufferWriteStream();
    strategy.init(META1, context.asyncAssertSuccess(v1 -> {
      // skip second init
      strategy.merge(new SimpleChunkReadStream(CHUNK1), META1, bws, context.asyncAssertSuccess(v3 -> {
        strategy.merge(new SimpleChunkReadStream(CHUNK2), META2, bws, context.asyncAssertFailure(err -> {
          async.complete();
        }));
      }));
    }));
  }
}
