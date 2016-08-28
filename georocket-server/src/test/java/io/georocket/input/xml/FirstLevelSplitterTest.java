package io.georocket.input.xml;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;

import io.georocket.input.Splitter.Result;
import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.Window;
import io.georocket.util.XMLStartElement;
import io.georocket.util.XMLStreamEvent;
import io.vertx.core.buffer.Buffer;

/**
 * Test the {@link FirstLevelSplitter}
 * @author Michel Kraemer
 */
public class FirstLevelSplitterTest {
  private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  
  /**
   * Use the {@link FirstLevelSplitter} and split an XML string
   * @param xml the XML string
   * @return the chunks created by the splitter
   * @throws Exception if the XML string could not be parsed
   */
  private List<Result<XMLChunkMeta>> split(String xml) throws Exception {
    Window window = new Window();
    window.append(Buffer.buffer(xml));
    AsyncXMLInputFactory xmlInputFactory = new InputFactoryImpl();
    AsyncXMLStreamReader<AsyncByteArrayFeeder> reader =
        xmlInputFactory.createAsyncForByteArray();
    byte[] xmlBytes = xml.getBytes(StandardCharsets.UTF_8);
    reader.getInputFeeder().feedInput(xmlBytes, 0, xmlBytes.length);
    FirstLevelSplitter splitter = new FirstLevelSplitter(window);
    List<Result<XMLChunkMeta>> chunks = new ArrayList<>();
    while (reader.hasNext()) {
      int event = reader.next();
      if (event == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
        reader.close();
        continue;
      }
      int pos = reader.getLocation().getCharacterOffset();
      Result<XMLChunkMeta> chunk = splitter.onEvent(
          new XMLStreamEvent(event, pos, reader));
      if (chunk != null) {
        chunks.add(chunk);
      }
    }
    return chunks;
  }
  
  /**
   * Test if an XML string with one chunk can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void oneChunk() throws Exception {
    String xml = XMLHEADER + "<root>\n<object><child></child></object>\n</root>";
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(1, chunks.size());
    Result<XMLChunkMeta> chunk = chunks.get(0);
    XMLChunkMeta meta = new XMLChunkMeta(Arrays.asList(new XMLStartElement("root")),
        XMLHEADER.length() + 7, xml.length() - 8);
    assertEquals(meta, chunk.getMeta());
    assertEquals(xml, chunk.getChunk());
  }
  
  /**
   * Test if an XML string with tow chunks can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void twoChunks() throws Exception {
    String xml = XMLHEADER + "<root><object><child></child></object>"
        + "<object><child2></child2></object></root>";
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(2, chunks.size());
    Result<XMLChunkMeta> chunk1 = chunks.get(0);
    Result<XMLChunkMeta> chunk2 = chunks.get(1);
    List<XMLStartElement> parents = Arrays.asList(new XMLStartElement("root"));
    XMLChunkMeta meta1 = new XMLChunkMeta(parents,
        XMLHEADER.length() + 7, XMLHEADER.length() + 7 + 32);
    XMLChunkMeta meta2 = new XMLChunkMeta(parents,
        XMLHEADER.length() + 7, XMLHEADER.length() + 7 + 34);
    assertEquals(meta1, chunk1.getMeta());
    assertEquals(meta2, chunk2.getMeta());
    assertEquals(XMLHEADER + "<root>\n<object><child></child></object>\n</root>", chunk1.getChunk());
    assertEquals(XMLHEADER + "<root>\n<object><child2></child2></object>\n</root>", chunk2.getChunk());
  }
  
  /**
   * Test if an XML string with two chunks and a namespace can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void namespace() throws Exception {
    String root = "<root xmlns=\"http://example.com\" xmlns:p=\"http://example.com\">";
    String xml = XMLHEADER + root + "<p:object><p:child></p:child></p:object>"
        + "<p:object><child2></child2></p:object></root>";
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(2, chunks.size());
    Result<XMLChunkMeta> chunk1 = chunks.get(0);
    Result<XMLChunkMeta> chunk2 = chunks.get(1);
    List<XMLStartElement> parents = Arrays.asList(new XMLStartElement(null, "root",
        new String[] { "", "p" }, new String[] { "http://example.com", "http://example.com" }));
    XMLChunkMeta meta1 = new XMLChunkMeta(parents,
        XMLHEADER.length() + root.length() + 1, XMLHEADER.length() + root.length() + 1 + 40);
    XMLChunkMeta meta2 = new XMLChunkMeta(parents,
        XMLHEADER.length() + root.length() + 1, XMLHEADER.length() + root.length() + 1 + 38);
    assertEquals(meta1, chunk1.getMeta());
    assertEquals(meta2, chunk2.getMeta());
    assertEquals(XMLHEADER + root + "\n<p:object><p:child></p:child></p:object>\n</root>", chunk1.getChunk());
    assertEquals(XMLHEADER + root + "\n<p:object><child2></child2></p:object>\n</root>", chunk2.getChunk());
  }
  
  /**
   * Test if an XML string with two chunks and a attributes can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void attributes() throws Exception {
    String root = "<root key=\"value\" key2=\"value2\">";
    String xml = XMLHEADER + root + "<object ok=\"ov\"><child></child></object>"
        + "<object><child2></child2></object></root>";
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(2, chunks.size());
    Result<XMLChunkMeta> chunk1 = chunks.get(0);
    Result<XMLChunkMeta> chunk2 = chunks.get(1);
    List<XMLStartElement> parents = Arrays.asList(new XMLStartElement(null, "root",
        new String[] { "", "" }, new String[] { "key", "key2" }, new String[] { "value", "value2" }));
    XMLChunkMeta meta1 = new XMLChunkMeta(parents,
        XMLHEADER.length() + root.length() + 1, XMLHEADER.length() + root.length() + 1 + 40);
    XMLChunkMeta meta2 = new XMLChunkMeta(parents,
        XMLHEADER.length() + root.length() + 1, XMLHEADER.length() + root.length() + 1 + 34);
    assertEquals(meta1, chunk1.getMeta());
    assertEquals(meta2, chunk2.getMeta());
    assertEquals(XMLHEADER + root + "\n<object ok=\"ov\"><child></child></object>\n</root>", chunk1.getChunk());
    assertEquals(XMLHEADER + root + "\n<object><child2></child2></object>\n</root>", chunk2.getChunk());
  }
  
  /**
   * Test if an XML string with two chunks, a namespace and attributes can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void full() throws Exception {
    String root = "<root xmlns=\"http://example.com\" xmlns:p=\"http://example.com\" key=\"value\" key2=\"value2\">";
    String xml = XMLHEADER + root + "<p:object ok=\"ov\"><p:child></p:child></p:object>"
        + "<p:object><child2></child2></p:object></root>";
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(2, chunks.size());
    Result<XMLChunkMeta> chunk1 = chunks.get(0);
    Result<XMLChunkMeta> chunk2 = chunks.get(1);
    List<XMLStartElement> parents = Arrays.asList(new XMLStartElement(null, "root",
        new String[] { "", "p" }, new String[] { "http://example.com", "http://example.com" },
        new String[] { "", "" }, new String[] { "key", "key2" }, new String[] { "value", "value2" }));
    XMLChunkMeta meta1 = new XMLChunkMeta(parents,
        XMLHEADER.length() + root.length() + 1, XMLHEADER.length() + root.length() + 1 + 48);
    XMLChunkMeta meta2 = new XMLChunkMeta(parents,
        XMLHEADER.length() + root.length() + 1, XMLHEADER.length() + root.length() + 1 + 38);
    assertEquals(meta1, chunk1.getMeta());
    assertEquals(meta2, chunk2.getMeta());
    assertEquals(XMLHEADER + root + "\n<p:object ok=\"ov\"><p:child></p:child></p:object>\n</root>", chunk1.getChunk());
    assertEquals(XMLHEADER + root + "\n<p:object><child2></child2></p:object>\n</root>", chunk2.getChunk());
  }
  
  /**
   * Test if an XML string with an UTF8 character can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void utf8() throws Exception {
    String xml = XMLHEADER + "<root>\n<object><child name=\"\u2248\"></child></object>\n</root>";
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(1, chunks.size());
    Result<XMLChunkMeta> chunk = chunks.get(0);
    XMLChunkMeta meta = new XMLChunkMeta(Arrays.asList(new XMLStartElement("root")),
        XMLHEADER.length() + 7, xml.getBytes(StandardCharsets.UTF_8).length - 8);
    assertEquals(meta, chunk.getMeta());
    assertEquals(xml, chunk.getChunk());
  }
}
