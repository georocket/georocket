package io.georocket.index.geojson;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Scanner;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableMap;

import io.georocket.util.JsonParserOperator;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import rx.Observable;

/**
 * Tests {@link GeoJsonGenericAttributeIndexer}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class GeoJsonGenericAttributeIndexerTest {
  /**
   * Indexes the given JSON file and checks if the result matches the
   * expected properties map
   * @param expected the expected properties map
   * @param jsonFile the JSON file to parse
   * @param context the current test context
   * @throws IOException if the JSON file could not be read
   */
  private void assertIndexed(Map<String, Object> expected, String jsonFile,
      TestContext context) throws IOException {
    String json;
    try (InputStream is = getClass().getResourceAsStream(jsonFile);
        Scanner scanner = new Scanner(is, "UTF-8")) {
      scanner.useDelimiter("\\A");
      json = scanner.next();
    }
    
    GeoJsonGenericAttributeIndexer indexer = new GeoJsonGenericAttributeIndexer();

    Map<String, Object> expectedMap = ImmutableMap.of(
      "genAttrs", expected);

    Async async = context.async();
    Observable.just(Buffer.buffer(json))
      .lift(new JsonParserOperator())
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
   * Test if a JSON file containing a Feature can be indexed
   * @param context the current test context
   * @throws IOException if the JSON file could not be read
   */
  @Test
  public void feature(TestContext context) throws IOException {
    Map<String, Object> expected = ImmutableMap.of(
      "name", "My Feature",
      "location", "Moon",
      "owner", "Elvis",
      "year", "2016",
      "solid", "true"
    );
    assertIndexed(expected, "feature.json", context);
  }
}
