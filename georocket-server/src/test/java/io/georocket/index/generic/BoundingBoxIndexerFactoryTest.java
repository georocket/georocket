package io.georocket.index.generic;

import static org.junit.Assert.assertEquals;

import org.geotools.referencing.CRS;
import org.junit.Test;

import io.georocket.index.Indexer;
import io.georocket.query.QueryCompiler.MatchPriority;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Test the {@link BoundingBoxIndexerFactory}
 * @author Tim Hellhake
 */
public class BoundingBoxIndexerFactoryTest {
  private static class BoundingBoxIndexerFactoryImpl extends BoundingBoxIndexerFactory {
    @Override
    public Indexer createIndexer() {
      return null;
    }
  }

  /**
   * Test if the factory returns NONE for invalid queries
   */
  @Test
  public void testInvalid() {
    BoundingBoxIndexerFactory factory = new BoundingBoxIndexerFactoryImpl();

    assertEquals(MatchPriority.NONE, factory.getQueryPriority(""));
    assertEquals(MatchPriority.NONE, factory.getQueryPriority("42"));
  }

  /**
   * Test if the factory compiles simple queries
   */
  @Test
  public void testQuery() {
    String point = "3477534.683,5605739.857";
    String query = point + "," + point;
    BoundingBoxIndexerFactory factory = new BoundingBoxIndexerFactoryImpl();

    assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query));
    double[] destination = {3477534.683, 5605739.857, 3477534.683, 5605739.857};
    testQuery(factory.compileQuery(query), destination);
  }

  /**
   * Test if the factory compiles EPSG queries
   */
  @Test
  public void testEPSG() {
    String point = "3477534.683,5605739.857";
    String query = "EPSG:31467:" + point + "," + point;

    BoundingBoxIndexerFactory factory = new BoundingBoxIndexerFactoryImpl();
    assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query));
    assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query.toLowerCase()));
    double[] destination = {
      8.681739535269804, 50.58691850210496, 8.681739535269804, 50.58691850210496
    };
    testQuery(factory.compileQuery(query), destination);
    testQuery(factory.compileQuery(query.toLowerCase()), destination);
  }

  /**
   * Test if the factory uses the configured default CRS code
   */
  @Test
  public void testEPSGDefault() {
    String point = "3477534.683,5605739.857";
    String query = point + "," + point;

    BoundingBoxIndexerFactory factory = new BoundingBoxIndexerFactoryImpl();
    factory.setDefaultCrs("EPSG:31467");
    assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query));
    assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query.toLowerCase()));
    double[] destination = {
      8.681739535269804, 50.58691850210496, 8.681739535269804, 50.58691850210496
    };
    testQuery(factory.compileQuery(query), destination);
    testQuery(factory.compileQuery(query.toLowerCase()), destination);
  }

  /**
   * Test if CRS codes in queries have priority over the configured default CRS
   */
  @Test
  public void testEPSGDefaultQueryOverride() {
    String point = "3477534.683,5605739.857";
    String query = "EPSG:31467:" + point + "," + point;

    BoundingBoxIndexerFactory factory = new BoundingBoxIndexerFactoryImpl();
    factory.setDefaultCrs("invalid string");
    assertEquals(MatchPriority.ONLY, factory.getQueryPriority(query));
    double[] destination = {
      8.681739535269804, 50.58691850210496, 8.681739535269804, 50.58691850210496
    };
    testQuery(factory.compileQuery(query), destination);
  }

  /**
   * Test if the factory uses the configured default CRS WKT
   * @throws Exception if the test fails
   */
  @Test
  public void testWKTDefault() throws Exception {
    String wkt = CRS.decode("epsg:31467").toWKT();
    String point = "3477534.683,5605739.857";
    String query = point + "," + point;

    BoundingBoxIndexerFactory factory = new BoundingBoxIndexerFactoryImpl();
    factory.setDefaultCrs(wkt);
    double[] destination = {8.681739535269804, 50.58691850210496, 8.681739535269804, 50.58691850210496};
    testQuery(factory.compileQuery(query), destination);
  }

  /**
   * Test if query contains correct coordinates
   * @param jsonQuery the query
   * @param destination the expected coordinates
   */
  private static void testQuery(JsonObject jsonQuery, double[] destination) {
    JsonArray coordinates = jsonQuery
      .getJsonObject("geo_shape")
      .getJsonObject("bbox")
      .getJsonObject("shape")
      .getJsonArray("coordinates");

    JsonArray first = coordinates.getJsonArray(0);
    assertEquals(destination[0], first.getDouble(0), 0.0001);
    assertEquals(destination[1], first.getDouble(1), 0.0001);
    JsonArray second = coordinates.getJsonArray(1);
    assertEquals(destination[2], second.getDouble(0), 0.0001);
    assertEquals(destination[3], second.getDouble(1), 0.0001);
  }
}
