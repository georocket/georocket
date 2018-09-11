package io.georocket.index.xml;

import com.google.common.collect.ImmutableMap;
import io.georocket.util.XMLParserTransformer;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import rx.Observable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Scanner;

/**
 * Tests {@link XalAddressIndexer}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class XalAddressIndexerTest {
  /**
   * Indexes the given XML file and checks if the result matches the
   * expected properties map
   * @param expected the expected properties map
   * @param xmlFile the XML file to parse
   * @param context the current test context
   * @throws IOException if the JSON file could not be read
   */
  private void assertIndexed(Map<String, Object> expected, String xmlFile,
      TestContext context) throws IOException {
    String json;
    try (InputStream is = getClass().getResourceAsStream(xmlFile);
         Scanner scanner = new Scanner(is, "UTF-8")) {
      scanner.useDelimiter("\\A");
      json = scanner.next();
    }

    XalAddressIndexer indexer = new XalAddressIndexer();
    Map<String, Object> expectedMap = ImmutableMap.of("address", expected);

    Async async = context.async();
    Observable.just(Buffer.buffer(json))
      .compose(new XMLParserTransformer())
      .doOnNext(indexer::onEvent)
      .last()
      .subscribe(r -> {
        context.assertEquals(expectedMap, indexer.getResult());
        async.complete();
      }, context::fail);
  }

  /**
   * Test if an XML file containing a XAL address can be indexed
   * @param context the current test context
   * @throws IOException if the XML file could not be read
   */
  @Test
  public void feature(TestContext context) throws IOException {
    Map<String, Object> expected = ImmutableMap.of(
      "Country", "Germany",
      "Locality", "Darmstadt",
      "Street", "Fraunhoferstra\u00DFe",
      "Number", "5"
    );
    assertIndexed(expected, "xal_simple_address.xml", context);
  }
}
