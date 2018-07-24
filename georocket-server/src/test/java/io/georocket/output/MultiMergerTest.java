package io.georocket.output;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.GeoJsonChunkMeta;
import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.XMLStartElement;
import io.georocket.util.io.BufferWriteStream;
import io.georocket.util.io.DelegateChunkReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import rx.Observable;

/**
 * Tests for {@link MultiMerger}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class MultiMergerTest {
  private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  private void doMerge(TestContext context, Observable<Buffer> chunks,
      Observable<ChunkMeta> metas, String jsonContents) {
    MultiMerger m = new MultiMerger(false);
    BufferWriteStream bws = new BufferWriteStream();
    Async async = context.async();
    metas
      .flatMapSingle(meta -> m.init(meta).toSingleDefault(meta))
      .toList()
      .flatMap(l -> chunks.map(DelegateChunkReadStream::new)
          .<ChunkMeta, Pair<ChunkReadStream, ChunkMeta>>zipWith(l, Pair::of))
      .flatMapCompletable(p -> m.merge(p.getLeft(), p.getRight(), bws))
      .toCompletable()
      .subscribe(() -> {
        m.finish(bws);
        context.assertEquals(jsonContents, bws.getBuffer().toString("utf-8"));
        async.complete();
      }, context::fail);
  }
  
  /**
   * Test if one GeoJSON geometry is rendered directly
   * @param context the Vert.x test context
   */
  @Test
  public void geoJsonOneGeometry(TestContext context) {
    String strChunk1 = "{\"type\":\"Polygon\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length());
    doMerge(context, Observable.just(chunk1), Observable.just(cm1), strChunk1);
  }
  
  /**
   * Test if two GeoJSON features can be merged to a feature collection
   * @param context the Vert.x test context
   */
  @Test
  public void geoJsonTwoFeatures(TestContext context) {
    String strChunk1 = "{\"type\":\"Feature\"}";
    String strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Feature", "features", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Feature", "features", 0, chunk2.length());
    doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
      "{\"type\":\"FeatureCollection\",\"features\":[" + strChunk1 + "," + strChunk2 + "]}");
  }
  
  /**
   * Test if simple XML chunks can be merged
   * @param context the Vert.x test context
   */
  @Test
  public void xmlSimple(TestContext context) {
    Buffer chunk1 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"1\"></test></root>");
    Buffer chunk2 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"2\"></test></root>");
    XMLChunkMeta cm = new XMLChunkMeta(Arrays.asList(new XMLStartElement("root")),
        XMLHEADER.length() + 6, chunk1.length() - 7);
    doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm, cm),
      XMLHEADER + "<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>");
  }
  
  /**
   * Test if the merger fails if chunks with a different type should be merged
   * @param context the Vert.x test context
   */
  @Test
  public void mixedInit(TestContext context) {
    String strChunk1 = "{\"type\":\"Feature\"}";
    String strChunk2 = XMLHEADER + "<root><test chunk=\"2\"></test></root>";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Feature", "features", 0, chunk1.length());
    XMLChunkMeta cm2 = new XMLChunkMeta(Arrays.asList(new XMLStartElement("root")),
      XMLHEADER.length() + 6, chunk2.length() - 7);
    
    MultiMerger m = new MultiMerger(false);
    Async async = context.async();
    m.init(cm1)
      .andThen(m.init(cm2))
      .subscribe(context::fail, err -> {
        context.assertTrue(err instanceof IllegalStateException);
        async.complete();
      });
  }
  
  /**
   * Test if the merger fails if chunks with a different type should be merged
   * @param context the Vert.x test context
   */
  @Test
  public void mixedMerge(TestContext context) {
    String strChunk1 = "{\"type\":\"Feature\"}";
    String strChunk2 = XMLHEADER + "<root><test chunk=\"2\"></test></root>";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Feature", "features", 0, chunk1.length());
    XMLChunkMeta cm2 = new XMLChunkMeta(Arrays.asList(new XMLStartElement("root")),
      XMLHEADER.length() + 6, chunk2.length() - 7);
    
    MultiMerger m = new MultiMerger(false);
    BufferWriteStream bws = new BufferWriteStream();
    Async async = context.async();
    m.init(cm1)
      .andThen(m.merge(new DelegateChunkReadStream(chunk1), cm1, bws))
      .andThen(m.merge(new DelegateChunkReadStream(chunk2), cm2, bws))
      .subscribe(context::fail, err -> {
        context.assertTrue(err instanceof IllegalStateException);
        async.complete();
      });
  }
}
