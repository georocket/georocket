package io.georocket.output.xml;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.storage.ChunkReadStream;
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
 * Test {@link XMLMerger}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class XMLMergerTest {
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
      Observable<XMLChunkMeta> metas, String xmlContents) {
    XMLMerger m = new XMLMerger();
    BufferWriteStream bws = new BufferWriteStream();
    Async async = context.async();
    metas
      .flatMapSingle(meta -> m.init(meta).toSingleDefault(meta))
      .toList()
      .flatMap(l -> chunks.map(DelegateChunkReadStream::new)
          .<XMLChunkMeta, Pair<ChunkReadStream, XMLChunkMeta>>zipWith(l, Pair::of))
      .flatMapCompletable(p -> m.merge(p.getLeft(), p.getRight(), bws))
      .toCompletable()
      .subscribe(() -> {
        m.finish(bws);
        context.assertEquals(XMLHEADER + xmlContents, bws.getBuffer().toString("utf-8"));
        async.complete();
      }, context::fail);
  }
  
  /**
   * Test if simple chunks can be merged
   * @param context the Vert.x test context
   */
  @Test
  public void simple(TestContext context) {
    Buffer chunk1 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"1\"></test></root>");
    Buffer chunk2 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"2\"></test></root>");
    XMLChunkMeta cm = new XMLChunkMeta(Arrays.asList(new XMLStartElement("root")),
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
    
    XMLChunkMeta cm1 = new XMLChunkMeta(Arrays.asList(root1),
        XMLHEADER.length() + root1.toString().length(),
        chunk1.length() - root1.getName().length() - 3);
    XMLChunkMeta cm2 = new XMLChunkMeta(Arrays.asList(root2),
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
