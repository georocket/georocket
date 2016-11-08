package io.georocket.output.xml;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.XMLStartElement;
import io.georocket.util.io.BufferWriteStream;
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
  private XMLChunkMeta cm = new XMLChunkMeta(Arrays.asList(new XMLStartElement("root")),
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
    strategy.init(cm)
      .flatMap(v -> strategy.init(cm))
      .flatMap(v -> strategy.merge(new DelegateChunkReadStream(chunk1), cm, bws))
      .flatMap(v -> strategy.merge(new DelegateChunkReadStream(chunk2), cm, bws))
      .doOnNext(v -> strategy.finish(bws))
      .subscribe(v -> {
        context.assertEquals(XMLHEADER + "<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>",
            bws.getBuffer().toString("utf-8"));
        async.complete();
      }, context::fail);
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
    strategy.init(cm)
      // skip second init
      .flatMap(v -> strategy.merge(new DelegateChunkReadStream(chunk1), cm, bws))
      .flatMap(v -> strategy.merge(new DelegateChunkReadStream(chunk2), cm, bws))
      .doOnNext(v -> strategy.finish(bws))
      .subscribe(v -> {
        context.assertEquals(XMLHEADER + "<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>",
            bws.getBuffer().toString("utf-8"));
        async.complete();
      }, context::fail);
  }
  
  /**
   * Test if canMerge works correctly
   * @param context the test context
   */
  @Test
  public void canMerge(TestContext context) {
    XMLChunkMeta cm2 = new XMLChunkMeta(Arrays.asList(new XMLStartElement("other")), 10, 20);
    XMLChunkMeta cm3 = new XMLChunkMeta(Arrays.asList(new XMLStartElement("pre", "root")), 10, 20);
    XMLChunkMeta cm4 = new XMLChunkMeta(Arrays.asList(new XMLStartElement(null, "root",
        new String[] { "" }, new String[] { "uri" })), 10, 20);
    
    Async async = context.async();
    MergeStrategy strategy = new AllSameStrategy();
    strategy.canMerge(cm)
      .doOnNext(c -> context.assertTrue(c))
      .flatMap(v -> strategy.init(cm))
      .flatMap(v -> strategy.canMerge(cm))
      .doOnNext(c -> context.assertTrue(c))
      .flatMap(v -> strategy.canMerge(cm2))
      .doOnNext(c -> context.assertFalse(c))
      .flatMap(v -> strategy.canMerge(cm3))
      .doOnNext(c -> context.assertFalse(c))
      .flatMap(v -> strategy.canMerge(cm4))
      .doOnNext(c -> context.assertFalse(c))
      .subscribe(v -> {
        async.complete();
      }, context::fail);
  }
  
  /**
   * Test if the merge method fails if it is called with an unexpected chunk
   * @param context the test context
   */
  @Test
  public void mergeFail(TestContext context) {
    XMLChunkMeta cm2 = new XMLChunkMeta(Arrays.asList(new XMLStartElement("other")), 10, 20);
    
    Async async = context.async();
    MergeStrategy strategy = new AllSameStrategy();
    BufferWriteStream bws = new BufferWriteStream();
    strategy.init(cm)
      .flatMap(v -> strategy.merge(new DelegateChunkReadStream(chunk2), cm2, bws))
      .subscribe(v -> context.fail(), err -> {
        context.assertTrue(err instanceof IllegalArgumentException);
        async.complete();
      });
  }
}
