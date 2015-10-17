package de.fhg.igd.georocket.output;

import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.fhg.igd.georocket.BufferReadStream;
import de.fhg.igd.georocket.BufferWriteStream;
import de.fhg.igd.georocket.SimpleChunkReadStream;
import de.fhg.igd.georocket.util.ChunkMeta;
import de.fhg.igd.georocket.util.XMLStartElement;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test {@link Merger}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class MergerTest {
  private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  /**
   * Test if simple chunks can be merged
   * @param context the Vert.x test context
   */
  @Test
  public void simple(TestContext context) {
    BufferWriteStream bws = new BufferWriteStream();
    Buffer chunk1 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"1\"></test></root>");
    Buffer chunk2 = Buffer.buffer(XMLHEADER + "<root><test chunk=\"2\"></test></root>");
    ChunkMeta cm = new ChunkMeta(Arrays.asList(new XMLStartElement("root")),
        XMLHEADER.length() + 6, chunk1.length() - 7);
    
    Merger m = new Merger();
    m.init(cm);
    m.init(cm);
    
    Async async = context.async();
    m.merge(new SimpleChunkReadStream(chunk1.length(), new BufferReadStream(chunk1)), cm, bws, v1 -> {
      m.merge(new SimpleChunkReadStream(chunk2.length(), new BufferReadStream(chunk2)), cm, bws, v2 -> {
        context.assertEquals(XMLHEADER + "<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>",
            bws.getBuffer().toString());
        async.complete();
      });
    });
  }
}
