package io.georocket.input.json;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import io.georocket.util.JsonParserOperator;
import io.georocket.util.StringWindow;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import rx.Observable;

/**
 * Test for {@link JsonSplitter}
 * @author Michel Kraemer
 */
public class JsonSplitterTest {
  private List<JsonObject> split(String file) throws IOException {
    byte[] json = IOUtils.toByteArray(JsonSplitterTest.class.getResource(file));
    List<JsonObject> chunks = new ArrayList<>();
    StringWindow window = new StringWindow();
    JsonSplitter splitter = new JsonSplitter(window);
    Observable.just(json)
      .map(Buffer::buffer)
      .doOnNext(buf -> window.append(buf.toString(StandardCharsets.UTF_8)))
      .lift(new JsonParserOperator())
      .flatMap(splitter::onEventObservable)
      .toBlocking()
      .forEach(result -> {
        JsonObject o = new JsonObject(result.getChunk());
        chunks.add(o);
      });
    return chunks;
  }
  
  /**
   * Test if a Feature can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void feature() throws IOException {
    List<JsonObject> chunks = split("feature.json");
    assertEquals(1, chunks.size());
    JsonObject o1 = chunks.get(0);
    assertEquals("Feature", o1.getString("type"));
    assertEquals("Fraunhofer IGD", o1.getJsonObject("properties").getString("name"));
  }
  
  /**
   * Test if a FeatureCollection can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void featureCollection() throws IOException {
    List<JsonObject> chunks = split("featurecollection.json");
    assertEquals(2, chunks.size());
    JsonObject o1 = chunks.get(0);
    assertEquals("Feature", o1.getString("type"));
    assertEquals("Fraunhofer IGD", o1.getJsonObject("properties").getString("name"));
    JsonObject o2 = chunks.get(1);
    assertEquals("Feature", o2.getString("type"));
    assertEquals("Darmstadtium", o2.getJsonObject("properties").getString("name"));
  }
  
  /**
   * Test if a GeometryCollection can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void geometryCollection() throws IOException {
    List<JsonObject> chunks = split("geometrycollection.json");
    assertEquals(2, chunks.size());
    JsonObject o1 = chunks.get(0);
    assertEquals("Point", o1.getString("type"));
    assertEquals(8.6599, o1.getJsonArray("coordinates").getDouble(0).doubleValue(), 0.00001);
    JsonObject o2 = chunks.get(1);
    assertEquals("Point", o2.getString("type"));
    assertEquals(8.6576, o2.getJsonArray("coordinates").getDouble(0).doubleValue(), 0.00001);
  }
  
  /**
   * Test if a LineString can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void lineString() throws IOException {
    List<JsonObject> chunks = split("linestring.json");
    assertEquals(1, chunks.size());
    JsonObject o1 = chunks.get(0);
    assertEquals("LineString", o1.getString("type"));
    assertEquals(13, o1.getJsonArray("coordinates").size());
  }
  
  /**
   * Test if a MultiLineString can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void muliLineString() throws IOException {
    List<JsonObject> chunks = split("multilinestring.json");
    assertEquals(1, chunks.size());
    JsonObject o1 = chunks.get(0);
    assertEquals("MultiLineString", o1.getString("type"));
    assertEquals(3, o1.getJsonArray("coordinates").size());
  }
  
  /**
   * Test if a MultiPoint can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void multiPoint() throws IOException {
    List<JsonObject> chunks = split("multipoint.json");
    assertEquals(1, chunks.size());
    JsonObject o1 = chunks.get(0);
    assertEquals("MultiPoint", o1.getString("type"));
    assertEquals(2, o1.getJsonArray("coordinates").size());
  }
  
  /**
   * Test if a MultiPolygon can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void multiPolygon() throws IOException {
    List<JsonObject> chunks = split("multipolygon.json");
    assertEquals(1, chunks.size());
    JsonObject o1 = chunks.get(0);
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
    List<JsonObject> chunks = split("point.json");
    assertEquals(1, chunks.size());
    JsonObject o1 = chunks.get(0);
    assertEquals("Point", o1.getString("type"));
    assertEquals(2, o1.getJsonArray("coordinates").size());
  }
  
  /**
   * Test if a Polygon can be split correctly
   * @throws IOException if the test file could not be read
   */
  @Test
  public void polygon() throws IOException {
    List<JsonObject> chunks = split("polygon.json");
    assertEquals(1, chunks.size());
    JsonObject o1 = chunks.get(0);
    assertEquals("Polygon", o1.getString("type"));
    assertEquals(1, o1.getJsonArray("coordinates").size());
    assertEquals(13, o1.getJsonArray("coordinates").getJsonArray(0).size());
  }
}
