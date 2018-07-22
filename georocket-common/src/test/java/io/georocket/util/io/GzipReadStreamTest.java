package io.georocket.util.io;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Test for {@link GzipReadStream}
 * @author Michel Kraemer
 */
@RunWith(VertxUnitRunner.class)
public class GzipReadStreamTest {
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
   * Tries to decompress a compressed file
   * @param context the test context
   * @throws Exception if the test fails
   */
  @Test
  public void decompress(TestContext context) throws Exception {
    ByteBuffer bb = makeBuffer();

    FileSystem fs = rule.vertx().fileSystem();
    File tempFile = tempFolder.newFile();
    String path = tempFile.getPath();

    // write compressed file
    try (FileOutputStream fos = new FileOutputStream(tempFile);
        GZIPOutputStream gos = new GZIPOutputStream(fos)) {
      gos.write(bb.array());
    }

    Async async = context.async();
    fs.open(path, new OpenOptions(), context.asyncAssertSuccess(file -> {
      GzipReadStream s = new GzipReadStream(file);
      s.exceptionHandler(context::fail);
      Buffer buf = Buffer.buffer();
      s.handler(buf::appendBuffer);
      s.endHandler(v -> {
        context.assertEquals(bb, ByteBuffer.wrap(buf.getBytes()));
        async.complete();
      });
    }));
  }

  /**
   * Creates a small GZIP file with a complex header and tries to decompress it
   * @param context the test context
   * @throws Exception if the test fails
   */
  @Test
  public void decompressWithHeader(TestContext context) throws Exception {
    // create a GZIP file in memory with all header fields enabled
    Buffer b = Buffer.buffer();

    b.appendUnsignedShortLE((short)GZIPInputStream.GZIP_MAGIC);
    b.appendByte((byte)8);
    b.appendByte((byte)(1 | 2 | 4 | 8 | 16));
    b.appendByte((byte)0);
    b.appendByte((byte)0);
    b.appendByte((byte)0);
    b.appendByte((byte)0);
    b.appendByte((byte)0);
    b.appendByte((byte)0);

    b.appendUnsignedShortLE(20);
    for (int i = 0; i < 20; ++i) {
      b.appendByte((byte)i);
    }

    b.appendString("filename", "US-ASCII");
    b.appendByte((byte)0);

    b.appendString("this is a comment", "US-ASCII");
    b.appendByte((byte)0);

    CRC32 crc = new CRC32();
    crc.update(b.getBytes());
    b.appendUnsignedShortLE((short)crc.getValue());

    byte[] content = "Hello World!\n".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DeflaterOutputStream dos = new DeflaterOutputStream(baos,
          new Deflater(Deflater.DEFAULT_COMPRESSION, true))) {
      dos.write(content);
    }
    crc.reset();
    crc.update(content);
    Buffer trailer = Buffer.buffer();
    trailer.appendUnsignedIntLE((int)crc.getValue());
    trailer.appendUnsignedIntLE(content.length);
    baos.write(trailer.getBytes());
    b.appendBytes(baos.toByteArray());

    // decompress file
    Async async = context.async();
    BufferReadStream brs = new BufferReadStream(b);
    GzipReadStream grs = new GzipReadStream(brs);
    Buffer decompressed = Buffer.buffer();
    grs.exceptionHandler(context::fail);
    grs.handler(decompressed::appendBuffer);
    grs.endHandler(v -> {
      context.assertEquals(Buffer.buffer(content), decompressed);
      async.complete();
    });
  }
}
