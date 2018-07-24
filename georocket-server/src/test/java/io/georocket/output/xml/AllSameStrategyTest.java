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
      .andThen(strategy.init(cm))
      .andThen(strategy.merge(new DelegateChunkReadStream(chunk1), cm, bws))
      .andThen(strategy.merge(new DelegateChunkReadStream(chunk2), cm, bws))
      .doOnCompleted(() -> strategy.finish(bws))
      .subscribe(() -> {
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
      .andThen(strategy.merge(new DelegateChunkReadStream(chunk1), cm, bws))
      .andThen(strategy.merge(new DelegateChunkReadStream(chunk2), cm, bws))
      .doOnCompleted(() -> strategy.finish(bws))
      .subscribe(() -> {
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
      .doOnSuccess(context::assertTrue)
      .flatMapCompletable(v -> strategy.init(cm))
      .andThen(strategy.canMerge(cm))
      .doOnSuccess(context::assertTrue)
      .flatMap(v -> strategy.canMerge(cm2))
      .doOnSuccess(context::assertFalse)
      .flatMap(v -> strategy.canMerge(cm3))
      .doOnSuccess(context::assertFalse)
      .flatMap(v -> strategy.canMerge(cm4))
      .doOnSuccess(context::assertFalse)
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
      .andThen(strategy.merge(new DelegateChunkReadStream(chunk2), cm2, bws))
      .subscribe(context::fail, err -> {
        context.assertTrue(err instanceof IllegalStateException);
        async.complete();
      });
  }
}
