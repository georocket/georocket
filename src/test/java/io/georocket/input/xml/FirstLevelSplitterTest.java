package io.georocket.input.xml;

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
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test the {@link FirstLevelSplitter}
 * @author Michel Kraemer
 */
public class FirstLevelSplitterTest {
  private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
  private static final String PREFIX = "<root>\n";
  private static final String SUFFIX = "\n</root>";
  
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
    String contents = "<object><child></child></object>";
    String xml = XMLHEADER + PREFIX + contents + SUFFIX;
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(1, chunks.size());
    Result<XMLChunkMeta> chunk = chunks.get(0);
    XMLChunkMeta meta = new XMLChunkMeta(List.of(new XMLStartElement("root")));
    assertEquals(meta, chunk.getMeta());
    assertEquals(XMLHEADER + PREFIX, chunk.getPrefix().toString());
    assertEquals(SUFFIX, chunk.getSuffix().toString());
    assertEquals(contents, chunk.getChunk().toString());
  }
  
  /**
   * Test if an XML string with tow chunks can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void twoChunks() throws Exception {
    String contents1 = "<object><child></child></object>";
    String contents2 = "<object><child2></child2></object>";
    String xml = XMLHEADER + PREFIX + contents1 + contents2 + SUFFIX;
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(2, chunks.size());
    Result<XMLChunkMeta> chunk1 = chunks.get(0);
    Result<XMLChunkMeta> chunk2 = chunks.get(1);
    List<XMLStartElement> parents = List.of(new XMLStartElement("root"));
    XMLChunkMeta meta = new XMLChunkMeta(parents);
    assertEquals(meta, chunk1.getMeta());
    assertEquals(XMLHEADER + PREFIX, chunk1.getPrefix().toString());
    assertEquals(SUFFIX, chunk1.getSuffix().toString());
    assertEquals(meta, chunk2.getMeta());
    assertEquals(XMLHEADER + PREFIX, chunk2.getPrefix().toString());
    assertEquals(SUFFIX, chunk2.getSuffix().toString());
    assertEquals(contents1, chunk1.getChunk().toString());
    assertEquals(contents2, chunk2.getChunk().toString());
  }
  
  /**
   * Test if an XML string with two chunks and a namespace can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void namespace() throws Exception {
    String contents1 = "<p:object><p:child></p:child></p:object>";
    String contents2 = "<p:object><child2></child2></p:object>";
    String root = "<root xmlns=\"http://example.com\" xmlns:p=\"http://example.com\">";
    String xml = XMLHEADER + root + contents1 + contents2 + "</root>";
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(2, chunks.size());
    Result<XMLChunkMeta> chunk1 = chunks.get(0);
    Result<XMLChunkMeta> chunk2 = chunks.get(1);
    List<XMLStartElement> parents = List.of(new XMLStartElement(null, "root",
            new String[]{"", "p"}, new String[]{"http://example.com", "http://example.com"}));
    XMLChunkMeta meta = new XMLChunkMeta(parents);
    assertEquals(meta, chunk1.getMeta());
    assertEquals(XMLHEADER + root + "\n", chunk1.getPrefix().toString());
    assertEquals(SUFFIX, chunk1.getSuffix().toString());
    assertEquals(meta, chunk2.getMeta());
    assertEquals(XMLHEADER + root + "\n", chunk2.getPrefix().toString());
    assertEquals(SUFFIX, chunk2.getSuffix().toString());
    assertEquals(contents1, chunk1.getChunk().toString());
    assertEquals(contents2, chunk2.getChunk().toString());
  }
  
  /**
   * Test if an XML string with two chunks and a attributes can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void attributes() throws Exception {
    String contents1 = "<object ok=\"ov\"><child></child></object>";
    String contents2 = "<object><child2></child2></object>";
    String root = "<root key=\"value\" key2=\"value2\">";
    String xml = XMLHEADER + root + contents1 + contents2 + "</root>";
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(2, chunks.size());
    Result<XMLChunkMeta> chunk1 = chunks.get(0);
    Result<XMLChunkMeta> chunk2 = chunks.get(1);
    List<XMLStartElement> parents = List.of(new XMLStartElement(null, "root",
            new String[]{"", ""}, new String[]{"key", "key2"}, new String[]{"value", "value2"}));
    XMLChunkMeta meta = new XMLChunkMeta(parents);
    assertEquals(meta, chunk1.getMeta());
    assertEquals(XMLHEADER + root + "\n", chunk1.getPrefix().toString());
    assertEquals(SUFFIX, chunk1.getSuffix().toString());
    assertEquals(meta, chunk2.getMeta());
    assertEquals(XMLHEADER + root + "\n", chunk2.getPrefix().toString());
    assertEquals(SUFFIX, chunk2.getSuffix().toString());
    assertEquals(contents1, chunk1.getChunk().toString());
    assertEquals(contents2, chunk2.getChunk().toString());
  }
  
  /**
   * Test if an XML string with two chunks, a namespace and attributes can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void full() throws Exception {
    String contents1 = "<p:object ok=\"ov\"><p:child></p:child></p:object>";
    String contents2 = "<p:object><child2></child2></p:object>";
    String root = "<root xmlns=\"http://example.com\" xmlns:p=\"http://example.com\" key=\"value\" key2=\"value2\">";
    String xml = XMLHEADER + root + contents1 + contents2 + "</root>";
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(2, chunks.size());
    Result<XMLChunkMeta> chunk1 = chunks.get(0);
    Result<XMLChunkMeta> chunk2 = chunks.get(1);
    List<XMLStartElement> parents = List.of(new XMLStartElement(null, "root",
            new String[]{"", "p"}, new String[]{"http://example.com", "http://example.com"},
            new String[]{"", ""}, new String[]{"key", "key2"}, new String[]{"value", "value2"}));
    XMLChunkMeta meta = new XMLChunkMeta(parents);
    assertEquals(meta, chunk1.getMeta());
    assertEquals(XMLHEADER + root + "\n", chunk1.getPrefix().toString());
    assertEquals(SUFFIX, chunk1.getSuffix().toString());
    assertEquals(meta, chunk2.getMeta());
    assertEquals(XMLHEADER + root + "\n", chunk2.getPrefix().toString());
    assertEquals(SUFFIX, chunk2.getSuffix().toString());
    assertEquals(contents1, chunk1.getChunk().toString());
    assertEquals(contents2, chunk2.getChunk().toString());
  }
  
  /**
   * Test if an XML string with an UTF8 character can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void utf8() throws Exception {
    String contents = "<object><child name=\"\u2248\"></child></object>";
    String xml = XMLHEADER + PREFIX + contents + SUFFIX;
    List<Result<XMLChunkMeta>> chunks = split(xml);
    assertEquals(1, chunks.size());
    Result<XMLChunkMeta> chunk = chunks.get(0);
    XMLChunkMeta meta = new XMLChunkMeta(List.of(new XMLStartElement("root")));
    assertEquals(meta, chunk.getMeta());
    assertEquals(XMLHEADER + PREFIX, chunk.getPrefix().toString());
    assertEquals(SUFFIX, chunk.getSuffix().toString());
    assertEquals(contents, chunk.getChunk().toString());
  }
}
