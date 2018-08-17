package io.georocket.util.io;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.streams.Pump;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

/**
 * Test for {@link GzipWriteStream}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class GzipWriteStreamTest {
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  /**
   * Create a temporary folder
   */
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private ByteBuffer makeBuffer() {
    ByteBuffer bb = ByteBuffer.allocate(1024 * 1024 * 5);
    for (int i = 0; i < bb.capacity() / Character.BYTES; ++i) {
      bb.putChar((char)('a' + (i % 26)));
    }
    bb.flip();
    return bb;
  }

  /**
   * Compresses a buffer to a file and tries to decompress it
   * @param context the test context
   * @throws Exception if the test fails
   */
  @Test
  public void compressAndDecompress(TestContext context) throws Exception {
    ByteBuffer bb = makeBuffer();

    FileSystem fs = rule.vertx().fileSystem();
    File tempFile = tempFolder.newFile();
    String path = tempFile.getPath();

    Async async = context.async();
    fs.open(path, new OpenOptions(), context.asyncAssertSuccess(file -> {
      GzipWriteStream s = new GzipWriteStream(file);
      s.exceptionHandler(context::fail);
      s.end(Buffer.buffer(bb.array()));
      s.close(v -> {
        byte[] fileContents;
        try (FileInputStream fis = new FileInputStream(tempFile);
             GZIPInputStream gis = new GZIPInputStream(fis)) {
          fileContents = IOUtils.toByteArray(gis);
        } catch (IOException e) {
          context.fail(e);
          return;
        }
        context.assertEquals(bb, ByteBuffer.wrap(fileContents));
        async.complete();
      });
    }));
  }

  /**
   * Compresses a file to another file with a pump and tries to decompress it
   * @param context the test context
   * @throws Exception if the test fails
   */
  @Test
  public void pump(TestContext context) throws Exception {
    ByteBuffer bb = makeBuffer();

    FileSystem fs = rule.vertx().fileSystem();
    File tempFileIn = tempFolder.newFile();
    String pathIn = tempFileIn.getPath();
    File tempFileOut = tempFolder.newFile();
    String pathOut = tempFileOut.getPath();

    Async async = context.async();
    fs.writeFile(pathIn, Buffer.buffer(bb.array()), context.asyncAssertSuccess(v -> {
      fs.open(pathIn, new OpenOptions(), context.asyncAssertSuccess(fileIn -> {
        fs.open(pathOut, new OpenOptions(), context.asyncAssertSuccess(fileOut -> {
          GzipWriteStream s = new GzipWriteStream(fileOut);
          s.exceptionHandler(context::fail);
          fileIn.exceptionHandler(context::fail);
          Pump.pump(fileIn, s).start();
          fileIn.endHandler(v3 -> s.end());
          s.close(v2 -> {
            byte[] fileContents;
            try (FileInputStream fis = new FileInputStream(tempFileOut);
                 GZIPInputStream gis = new GZIPInputStream(fis)) {
              fileContents = IOUtils.toByteArray(gis);
            } catch (IOException e) {
              context.fail(e);
              return;
            }
            context.assertEquals(bb, ByteBuffer.wrap(fileContents));
            async.complete();
          });
        }));
      }));
    }));
  }
}
