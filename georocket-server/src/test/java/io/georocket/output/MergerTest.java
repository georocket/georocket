package io.georocket.output;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.BufferWriteStream;
import io.georocket.SimpleChunkReadStream;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.util.XMLStartElement;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import rx.Observable;

/**
 * Test {@link Merger}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class MergerTest {
  private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  
  private static final String XSI = "xsi";
  private static final String SCHEMA_LOCATION = "schemaLocation";
  
  private static final String NS_CITYGML_1_0 =
      "http://www.opengis.net/citygml/1.0";
  private static final String NS_CITYGML_1_0_BUILDING =
      "http://www.opengis.net/citygml/building/1.0";
  private static final String NS_CITYGML_1_0_BUILDING_URL =
      "http://schemas.opengis.net/citygml/building/1.0/building.xsd";
  private static final String NS_CITYGML_1_0_BUILDING_SCHEMA_LOCATION =
      NS_CITYGML_1_0_BUILDING + " " + NS_CITYGML_1_0_BUILDING_URL;
  private static final String NS_CITYGML_1_0_GENERICS =
      "http://www.opengis.net/citygml/generics/1.0";
  private static final String NS_CITYGML_1_0_GENERICS_URL =
      "http://schemas.opengis.net/citygml/generics/1.0/generics.xsd";
  private static final String NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION =
      NS_CITYGML_1_0_GENERICS + " " + NS_CITYGML_1_0_GENERICS_URL;
  private static final String NS_GML =
      "http://www.opengis.net/gml";
  private static final String NS_SCHEMA_INSTANCE =
      "http://www.w3.org/2001/XMLSchema-instance";
  
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  private void doMerge(TestContext context, Observable<Buffer> chunks,
      Observable<ChunkMeta> metas, String xmlContents) {
    Merger m = new Merger();
    BufferWriteStream bws = new BufferWriteStream();
    Async async = context.async();
    metas
      .flatMap(meta -> m.initObservable(meta).map(v -> meta))
      .toList()
      .flatMap(l -> chunks.map(SimpleChunkReadStream::new)
          .<ChunkMeta, Pair<ChunkReadStream, ChunkMeta>>zipWith(l, Pair::of))
      .flatMap(p -> m.mergeObservable(p.getLeft(), p.getRight(), bws))
      .last()
      .subscribe(v -> {
        m.finishMerge(bws);
        context.assertEquals(XMLHEADER + xmlContents, bws.getBuffer().toString("utf-8"));
        async.complete();
      }, err -> {
        context.fail(err);
      });
  }
  
  /**
   * Test if simple chunks can be merged
   * @param context the Vert.x test context
   */
  @Test
  public void simple(TestContext context) {
    Buffer chunk1 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"1\"></test></root>");
    Buffer chunk2 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"2\"></test></root>");
    ChunkMeta cm = new ChunkMeta(Arrays.asList(new XMLStartElement("root")),
        XMLHEADER.length() + 6, chunk1.length() - 7);
    doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm, cm),
        "<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>");
  }

  /**
   * Test if chunks with different namespaces can be merged
   * @param context the Vert.x test context
   */
  @Test
  public void mergeNamespaces(TestContext context) {
    XMLStartElement root1 = new XMLStartElement(null, "CityModel",
        new String[] { "", "gml", "gen", XSI },
        new String[] { NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_GENERICS, NS_SCHEMA_INSTANCE },
        new String[] { XSI },
        new String[] { SCHEMA_LOCATION },
        new String[] { NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION });
    XMLStartElement root2 = new XMLStartElement(null, "CityModel",
        new String[] { "", "gml", "bldg", XSI },
        new String[] { NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_BUILDING, NS_SCHEMA_INSTANCE },
        new String[] { XSI },
        new String[] { SCHEMA_LOCATION },
        new String[] { NS_CITYGML_1_0_BUILDING_SCHEMA_LOCATION });
    
    String contents1 = "<cityObjectMember><gen:GenericCityObject></gen:GenericCityObject></cityObjectMember>";
    Buffer chunk1 = Buffer.buffer(XMLHEADER + root1 + contents1 + "</" + root1.getName() + ">");
    String contents2 = "<cityObjectMember><bldg:Building></bldg:Building></cityObjectMember>";
    Buffer chunk2 = Buffer.buffer(XMLHEADER + root2 + contents2 + "</" + root2.getName() + ">");
    
    ChunkMeta cm1 = new ChunkMeta(Arrays.asList(root1),
        XMLHEADER.length() + root1.toString().length(),
        chunk1.length() - root1.getName().length() - 3);
    ChunkMeta cm2 = new ChunkMeta(Arrays.asList(root2),
        XMLHEADER.length() + root2.toString().length(),
        chunk2.length() - root2.getName().length() - 3);
    
    XMLStartElement expectedRoot = new XMLStartElement(null, "CityModel",
        new String[] { "", "gml", "gen", XSI, "bldg" },
        new String[] { NS_CITYGML_1_0, NS_GML, NS_CITYGML_1_0_GENERICS, NS_SCHEMA_INSTANCE, NS_CITYGML_1_0_BUILDING },
        new String[] { XSI },
        new String[] { SCHEMA_LOCATION },
        new String[] { NS_CITYGML_1_0_GENERICS_SCHEMA_LOCATION + " " + NS_CITYGML_1_0_BUILDING_SCHEMA_LOCATION });
    
    doMerge(context, Observable.just(chunk1, chunk2), Observable.just(cm1, cm2),
        expectedRoot + contents1 + contents2 + "</" + expectedRoot.getName() + ">");
  }
}
