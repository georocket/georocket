package io.georocket.output;

import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.georocket.BufferReadStream;
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
  
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  
  private ChunkReadStream createChunkReadStream(Buffer chunk) {
    return new SimpleChunkReadStream(chunk.length(), new BufferReadStream(chunk));
  }
  
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
    Observable.just(chunk1, chunk2)
      .map(this::createChunkReadStream)
      .flatMap(c -> m.mergeObservable(c, cm, bws))
      .last()
      .subscribe(v -> {
        m.finishMerge(bws);
        context.assertEquals(XMLHEADER + "<root><test chunk=\"1\"></test><test chunk=\"2\"></test></root>",
            bws.getBuffer().toString());
        async.complete();
      }, err -> {
        context.fail(err);
      });
  }
}
