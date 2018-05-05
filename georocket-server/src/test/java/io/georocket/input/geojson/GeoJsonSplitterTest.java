package io.georocket.input.geojson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import io.georocket.storage.GeoJsonChunkMeta;
import io.georocket.util.JsonParserTransformer;
import io.georocket.util.StringWindow;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import rx.Observable;

/**
 * Test for {@link GeoJsonSplitter}
 * @author Michel Kraemer
 */
public class GeoJsonSplitterTest {
  private long getFileSize(String file) throws IOException {
    try (InputStream is = GeoJsonSplitterTest.class.getResourceAsStream(file)) {
      return IOUtils.skip(is, Long.MAX_VALUE);
    }
  }

  private List<Tuple2<GeoJsonChunkMeta, JsonObject>> split(String file)
      throws IOException {
    byte[] json = IOUtils.toByteArray(GeoJsonSplitterTest.class.getResource(file));
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = new ArrayList<>();
    StringWindow window = new StringWindow();
    GeoJsonSplitter splitter = new GeoJsonSplitter(window);
    Observable.just(json)
      .map(Buffer::buffer)
      .doOnNext(window::append)
      .compose(new JsonParserTransformer())
      .flatMap(splitter::onEventObservable)
      .toBlocking()
      .forEach(result -> {
        JsonObject o = new JsonObject(result.getChunk());
        chunks.add(Tuple.tuple((GeoJsonChunkMeta)result.getMeta(), o));
      });
    return chunks;
  }
  
  /**
   * Test if a Feature can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void feature() throws IOException {
    String filename = "feature.json";
    long size = getFileSize(filename);
    
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(1, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertNull(m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(size, m1.getEnd());
    assertEquals("Feature", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("Feature", o1.getString("type"));
    assertEquals("Fraunhofer IGD", o1.getJsonObject("properties").getString("name"));
  }

  /**
   * Test if a Feature with a greek property can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void greek() throws IOException {
    String filename = "greek.json";
    long size = getFileSize(filename);

    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(1, chunks.size());

    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);

    GeoJsonChunkMeta m1 = t1.v1;
    assertNull(m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(size, m1.getEnd());
    assertEquals("Feature", m1.getType());

    JsonObject o1 = t1.v2;
    assertEquals("Feature", o1.getString("type"));
    assertEquals("\u03a1\u039f\u0394\u0391\u039a\u0399\u039d\u0399\u0395\u03a3 " +
      "\u039c\u0395\u03a4\u0391\u03a0\u039f\u0399\u0397\u03a3\u0397\u03a3",
      o1.getJsonObject("properties").getString("name"));
  }
  
  /**
   * Test if a FeatureCollection can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void featureCollection() throws IOException {
    String filename = "featurecollection.json";
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(2, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertEquals("features", m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(307, m1.getEnd());
    assertEquals("Feature", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("Feature", o1.getString("type"));
    assertEquals("Fraunhofer IGD", o1.getJsonObject("properties").getString("name"));
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t2 = chunks.get(1);
    
    GeoJsonChunkMeta m2 = t2.v1;
    assertEquals("features", m2.getParentFieldName());
    assertEquals(0, m2.getStart());
    assertEquals(305, m2.getEnd());
    assertEquals("Feature", m2.getType());
    
    JsonObject o2 = t2.v2;
    assertEquals("Feature", o2.getString("type"));
    assertEquals("Darmstadtium", o2.getJsonObject("properties").getString("name"));
  }
  
  /**
   * Test if a GeometryCollection can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void geometryCollection() throws IOException {
    String filename = "geometrycollection.json";
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(2, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertEquals("geometries", m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(132, m1.getEnd());
    assertEquals("Point", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("Point", o1.getString("type"));
    assertEquals(8.6599, o1.getJsonArray("coordinates").getDouble(0).doubleValue(), 0.00001);
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t2 = chunks.get(1);
    
    GeoJsonChunkMeta m2 = t2.v1;
    assertEquals("geometries", m2.getParentFieldName());
    assertEquals(0, m2.getStart());
    assertEquals(132, m2.getEnd());
    assertEquals("Point", m2.getType());
    
    JsonObject o2 = t2.v2;
    assertEquals("Point", o2.getString("type"));
    assertEquals(8.6576, o2.getJsonArray("coordinates").getDouble(0).doubleValue(), 0.00001);
  }
  
  /**
   * Test if a LineString can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void lineString() throws IOException {
    String filename = "linestring.json";
    long size = getFileSize(filename);
    
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(1, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertNull(m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(size, m1.getEnd());
    assertEquals("LineString", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("LineString", o1.getString("type"));
    assertEquals(13, o1.getJsonArray("coordinates").size());
  }
  
  /**
   * Test if a MultiLineString can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void muliLineString() throws IOException {
    String filename = "multilinestring.json";
    long size = getFileSize(filename);
    
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(1, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertNull(m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(size, m1.getEnd());
    assertEquals("MultiLineString", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("MultiLineString", o1.getString("type"));
    assertEquals(3, o1.getJsonArray("coordinates").size());
  }
  
  /**
   * Test if a MultiPoint can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void multiPoint() throws IOException {
    String filename = "multipoint.json";
    long size = getFileSize(filename);
    
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(1, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertNull(m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(size, m1.getEnd());
    assertEquals("MultiPoint", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("MultiPoint", o1.getString("type"));
    assertEquals(2, o1.getJsonArray("coordinates").size());
  }
  
  /**
   * Test if a MultiPolygon can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void multiPolygon() throws IOException {
    String filename = "multipolygon.json";
    long size = getFileSize(filename);
    
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(1, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertNull(m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(size, m1.getEnd());
    assertEquals("MultiPolygon", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("MultiPolygon", o1.getString("type"));
    assertEquals(1, o1.getJsonArray("coordinates").size());
    assertEquals(1, o1.getJsonArray("coordinates").getJsonArray(0).size());
    assertEquals(13, o1.getJsonArray("coordinates").getJsonArray(0)
        .getJsonArray(0).size());
  }
  
  /**
   * Test if a Point can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void point() throws IOException {
    String filename = "point.json";
    long size = getFileSize(filename);
    
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(1, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertNull(m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(size, m1.getEnd());
    assertEquals("Point", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("Point", o1.getString("type"));
    assertEquals(2, o1.getJsonArray("coordinates").size());
  }
  
  /**
   * Test if a Polygon can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void polygon() throws IOException {
    String filename = "polygon.json";
    long size = getFileSize(filename);
    
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(1, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertNull(m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(size, m1.getEnd());
    assertEquals("Polygon", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("Polygon", o1.getString("type"));
    assertEquals(1, o1.getJsonArray("coordinates").size());
    assertEquals(13, o1.getJsonArray("coordinates").getJsonArray(0).size());
  }
  
  /**
   * Test if extra attributes not part of the standard are ignored
   * @throws IOException if the test file could not be read
   */
  @Test
  public void extraAttributes() throws IOException {
    String filename = "featurecollectionext.json";
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(2, chunks.size());
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t1 = chunks.get(0);
    
    GeoJsonChunkMeta m1 = t1.v1;
    assertEquals("features", m1.getParentFieldName());
    assertEquals(0, m1.getStart());
    assertEquals(307, m1.getEnd());
    assertEquals("Feature", m1.getType());
    
    JsonObject o1 = t1.v2;
    assertEquals("Feature", o1.getString("type"));
    assertEquals("Fraunhofer IGD", o1.getJsonObject("properties").getString("name"));
    
    Tuple2<GeoJsonChunkMeta, JsonObject> t2 = chunks.get(1);
    
    GeoJsonChunkMeta m2 = t2.v1;
    assertEquals("features", m2.getParentFieldName());
    assertEquals(0, m2.getStart());
    assertEquals(305, m2.getEnd());
    assertEquals("Feature", m2.getType());
    
    JsonObject o2 = t2.v2;
    assertEquals("Feature", o2.getString("type"));
    assertEquals("Darmstadtium", o2.getJsonObject("properties").getString("name"));
  }
  
  /**
   * Make sure the splitter doesn't find any chunks in an empty feature collection
   * @throws IOException if the test file could not be read
   */
  @Test
  public void emptyFeatureCollection() throws IOException {
    String filename = "featurecollectionempty.json";
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(0, chunks.size());
  }
  
  /**
   * Make sure the splitter doesn't find any chunks in an empty geometry collection
   * @throws IOException if the test file could not be read
   */
  @Test
  public void emptyGeometryCollection() throws IOException {
    String filename = "geometrycollectionempty.json";
    List<Tuple2<GeoJsonChunkMeta, JsonObject>> chunks = split(filename);
    assertEquals(0, chunks.size());
  }
}
