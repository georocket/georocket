package io.georocket.index.geojson;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableMap;

import io.georocket.util.JsonParserTransformer;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import rx.Observable;

/**
 * Tests {@link GeoJsonBoundingBoxIndexer}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class GeoJsonBoundingBoxIndexerTest {
  /**
   * Indexes the given JSON file and checks if the result matches the
   * expected bounding box
   * @param expected the expected bounding box
   * @param jsonFile the JSON file to parse
   * @param context the current test context
   * @throws IOException if the JSON file could not be read
   */
  private void assertIndexed(List<List<Double>> expected, String jsonFile,
      TestContext context) throws IOException {
    String json;
    try (InputStream is = getClass().getResourceAsStream(jsonFile);
        Scanner scanner = new Scanner(is, "UTF-8")) {
      scanner.useDelimiter("\\A");
      json = scanner.next();
    }
    
    GeoJsonBoundingBoxIndexer indexer = new GeoJsonBoundingBoxIndexer();

    Map<String, Object> expectedMap = ImmutableMap.of("bbox", ImmutableMap.of(
      "type", "envelope",
      "coordinates", expected
    ));

    Async async = context.async();
    Observable.just(Buffer.buffer(json))
      .compose(new JsonParserTransformer())
      .doOnNext(indexer::onEvent)
      .last()
      .subscribe(r -> {
        context.assertEquals(expectedMap, indexer.getResult());
        async.complete();
      }, err -> {
        context.fail(err);
      });
  }

  /**
   * Test if a JSON file containing a Point geometry can be indexed
   * @param context the current test context
   * @throws IOException if the JSON file could not be read
   */
  @Test
  public void point(TestContext context) throws IOException {
    List<List<Double>> expected = Arrays.asList(
      Arrays.asList(8.6599, 49.87424),
      Arrays.asList(8.6599, 49.87424)
    );
    assertIndexed(expected, "point.json", context);
  }
  
  /**
   * Test if a JSON file containing a LineString geometry can be indexed
   * @param context the current test context
   * @throws IOException if the JSON file could not be read
   */
  @Test
  public void lineString(TestContext context) throws IOException {
    List<List<Double>> expected = Arrays.asList(
      Arrays.asList(8.0, 49.5),
      Arrays.asList(8.5, 49.0)
    );
    assertIndexed(expected, "linestring.json", context);
  }
  
  /**
   * Test if a JSON file containing a Feature with a LineString geometry
   * can be indexed
   * @param context the current test context
   * @throws IOException if the JSON file could not be read
   */
  @Test
  public void feature(TestContext context) throws IOException {
    List<List<Double>> expected = Arrays.asList(
      Arrays.asList(8.0, 49.5),
      Arrays.asList(8.5, 49.0)
    );
    assertIndexed(expected, "feature.json", context);
  }
}
