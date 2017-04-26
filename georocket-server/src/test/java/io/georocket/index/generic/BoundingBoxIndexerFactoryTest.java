package io.georocket.index.generic;

import io.georocket.index.Indexer;
import io.georocket.query.QueryCompiler.MatchPriority;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
   * Test if query contains correct coordinates
   * @param jsonQuery   the query
   * @param destination the expected coordinates
   */
  private static void testQuery(JsonObject jsonQuery, double[] destination) {
    JsonArray coordinates = jsonQuery
      .getJsonObject("geo_shape")
      .getJsonObject("bbox")
      .getJsonObject("shape")
      .getJsonArray("coordinates");

    JsonArray first = coordinates.getJsonArray(0);
    assertTrue(first.getDouble(0) - destination[0] < 0.01);
    assertTrue(first.getDouble(1) - destination[1] < 0.01);
    JsonArray second = coordinates.getJsonArray(1);
    assertTrue(second.getDouble(0) - destination[2] < 0.01);
    assertTrue(second.getDouble(1) - destination[3] < 0.01);
  }
}
