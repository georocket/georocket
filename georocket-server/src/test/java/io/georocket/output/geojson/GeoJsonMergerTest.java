package io.georocket.output.geojson;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.GeoJsonChunkMeta;
import io.georocket.util.io.BufferWriteStream;
import io.georocket.util.io.DelegateChunkReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import rx.Observable;

/**
 * Test {@link GeoJsonMerger}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class GeoJsonMergerTest {
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  private void doMerge(TestContext context, Observable<Buffer> chunks,
      Observable<GeoJsonChunkMeta> metas, String jsonContents) {
    GeoJsonMerger m = new GeoJsonMerger();
    BufferWriteStream bws = new BufferWriteStream();
    Async async = context.async();
    metas
      .flatMapSingle(meta -> m.init(meta).toSingleDefault(meta))
      .toList()
      .flatMap(l -> chunks.map(DelegateChunkReadStream::new)
          .<GeoJsonChunkMeta, Pair<ChunkReadStream, GeoJsonChunkMeta>>zipWith(l, Pair::of))
      .flatMapCompletable(p -> m.merge(p.getLeft(), p.getRight(), bws))
      .toCompletable()
      .subscribe(() -> {
        m.finish(bws);
        context.assertEquals(jsonContents, bws.getBuffer().toString("utf-8"));
        async.complete();
      }, context::fail);
  }
  
  /**
   * Test if one geometry is rendered directly
   * @param context the Vert.x test context
   */
  @Test
  public void oneGeometry(TestContext context) {
    String strChunk1 = "{\"type\":\"Polygon\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length());
    doMerge(context, Observable.just(chunk1), Observable.just(cm1), strChunk1);
  }
  
  /**
   * Test if one feature is rendered directly
   * @param context the Vert.x test context
   */
  @Test
  public void oneFeature(TestContext context) {
    String strChunk1 = "{\"type\":\"Feature\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Feature", "features", 0, chunk1.length());
    doMerge(context, Observable.just(chunk1), Observable.just(cm1), strChunk1);
  }
  
  /**
   * Test if two geometries can be merged to a geometry collection
   * @param context the Vert.x test context
   */
  @Test
  public void twoGeometries(TestContext context) {
    String strChunk1 = "{\"type\":\"Polygon\"}";
    String strChunk2 = "{\"type\":\"Point\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length());
    doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
      "{\"type\":\"GeometryCollection\",\"geometries\":[" + strChunk1 + "," + strChunk2 + "]}");
  }
  
  /**
   * Test if three geometries can be merged to a geometry collection
   * @param context the Vert.x test context
   */
  @Test
  public void threeGeometries(TestContext context) {
    String strChunk1 = "{\"type\":\"Polygon\"}";
    String strChunk2 = "{\"type\":\"Point\"}";
    String strChunk3 = "{\"type\":\"MultiPoint\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    Buffer chunk3 = Buffer.buffer(strChunk3);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length());
    GeoJsonChunkMeta cm3 = new GeoJsonChunkMeta("MultiPoint", "geometries", 0, chunk3.length());
    doMerge(context, Observable.just(chunk1, chunk2, chunk3), Observable.just(cm1, cm2, cm3),
      "{\"type\":\"GeometryCollection\",\"geometries\":[" + strChunk1 + "," +
        strChunk2 + "," + strChunk3 + "]}");
  }
  
  /**
   * Test if two features can be merged to a feature collection
   * @param context the Vert.x test context
   */
  @Test
  public void twoFeatures(TestContext context) {
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
   * Test if three features can be merged to a feature collection
   * @param context the Vert.x test context
   */
  @Test
  public void threeFeatures(TestContext context) {
    String strChunk1 = "{\"type\":\"Feature\"}";
    String strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}";
    String strChunk3 = "{\"type\":\"Feature\",\"geometry\":[]}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    Buffer chunk3 = Buffer.buffer(strChunk3);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Feature", "features", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Feature", "features", 0, chunk2.length());
    GeoJsonChunkMeta cm3 = new GeoJsonChunkMeta("Feature", "features", 0, chunk3.length());
    doMerge(context, Observable.just(chunk1, chunk2, chunk3), Observable.just(cm1, cm2, cm3),
      "{\"type\":\"FeatureCollection\",\"features\":[" + strChunk1 + "," +
        strChunk2 + "," + strChunk3 + "]}");
  }
  
  /**
   * Test if two geometries and a feature can be merged to a feature collection
   * @param context the Vert.x test context
   */
  @Test
  public void geometryAndFeature(TestContext context) {
    String strChunk1 = "{\"type\":\"Polygon\"}";
    String strChunk2 = "{\"type\":\"Feature\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Feature", "features", 0, chunk2.length());
    doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
      "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":" + strChunk1 + "}," +
        strChunk2 + "]}");
  }
  
  /**
   * Test if two geometries and a feature can be merged to a feature collection
   * @param context the Vert.x test context
   */
  @Test
  public void featureAndGeometry(TestContext context) {
    String strChunk1 = "{\"type\":\"Feature\"}";
    String strChunk2 = "{\"type\":\"Polygon\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Feature", "features", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Polygon", "geometries", 0, chunk2.length());
    doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
      "{\"type\":\"FeatureCollection\",\"features\":[" + strChunk1 +
        ",{\"type\":\"Feature\",\"geometry\":" + strChunk2 + "}]}");
  }
  
  /**
   * Test if two geometries and a feature can be merged to a feature collection
   * @param context the Vert.x test context
   */
  @Test
  public void twoGeometriesAndAFeature(TestContext context) {
    String strChunk1 = "{\"type\":\"Polygon\"}";
    String strChunk2 = "{\"type\":\"Point\"}";
    String strChunk3 = "{\"type\":\"Feature\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    Buffer chunk3 = Buffer.buffer(strChunk3);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Polygon", "geometries", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Point", "geometries", 0, chunk2.length());
    GeoJsonChunkMeta cm3 = new GeoJsonChunkMeta("Feature", "features", 0, chunk3.length());
    doMerge(context, Observable.just(chunk1, chunk2, chunk3), Observable.just(cm1, cm2, cm3),
      "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":" + strChunk1 + "}," +
        "{\"type\":\"Feature\",\"geometry\":" + strChunk2 + "}," + strChunk3 + "]}");
  }
  
  /**
   * Test if two geometries and a feature can be merged to a feature collection
   * @param context the Vert.x test context
   */
  @Test
  public void twoFeaturesAndAGeometry(TestContext context) {
    String strChunk1 = "{\"type\":\"Feature\"}";
    String strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}";
    String strChunk3 = "{\"type\":\"Point\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    Buffer chunk3 = Buffer.buffer(strChunk3);
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Feature", "features", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Feature", "features", 0, chunk2.length());
    GeoJsonChunkMeta cm3 = new GeoJsonChunkMeta("Point", "geometries", 0, chunk3.length());
    doMerge(context, Observable.just(chunk1, chunk2, chunk3), Observable.just(cm1, cm2, cm3),
      "{\"type\":\"FeatureCollection\",\"features\":[" + strChunk1 + "," + strChunk2 +
        ",{\"type\":\"Feature\",\"geometry\":" + strChunk3 + "}]}");
  }
  
  /**
   * Test if the merger fails if {@link GeoJsonMerger#init(GeoJsonChunkMeta)} has
   * not been called often enough
   * @param context the Vert.x test context
   */
  @Test
  public void notEnoughInits(TestContext context) {
    String strChunk1 = "{\"type\":\"Feature\"}";
    String strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Feature", "features", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Feature", "features", 0, chunk2.length());
    
    GeoJsonMerger m = new GeoJsonMerger();
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
  
  /**
   * Test if the merger succeeds if {@link GeoJsonMerger#init(GeoJsonChunkMeta)} has
   * not been called just often enough
   * @param context the Vert.x test context
   */
  @Test
  public void enoughInits(TestContext context) {
    String strChunk1 = "{\"type\":\"Feature\"}";
    String strChunk2 = "{\"type\":\"Feature\",\"properties\":{}}";
    String strChunk3 = "{\"type\":\"Polygon\"}";
    Buffer chunk1 = Buffer.buffer(strChunk1);
    Buffer chunk2 = Buffer.buffer(strChunk2);
    Buffer chunk3 = Buffer.buffer(strChunk3);
    
    GeoJsonChunkMeta cm1 = new GeoJsonChunkMeta("Feature", "features", 0, chunk1.length());
    GeoJsonChunkMeta cm2 = new GeoJsonChunkMeta("Feature", "features", 0, chunk2.length());
    GeoJsonChunkMeta cm3 = new GeoJsonChunkMeta("Polygon", "geometries", 0, chunk2.length());
    
    String jsonContents = "{\"type\":\"FeatureCollection\",\"features\":[" + strChunk1 +
      "," + strChunk2 + ",{\"type\":\"Feature\",\"geometry\":" + strChunk3 + "}]}";
    
    GeoJsonMerger m = new GeoJsonMerger();
    BufferWriteStream bws = new BufferWriteStream();
    Async async = context.async();
    m.init(cm1)
      .andThen(m.init(cm2))
      .andThen(m.merge(new DelegateChunkReadStream(chunk1), cm1, bws))
      .andThen(m.merge(new DelegateChunkReadStream(chunk2), cm2, bws))
      .andThen(m.merge(new DelegateChunkReadStream(chunk3), cm3, bws))
      .subscribe(() -> {
        m.finish(bws);
        context.assertEquals(jsonContents, bws.getBuffer().toString("utf-8"));
        async.complete();
      }, context::fail);
  }
}
