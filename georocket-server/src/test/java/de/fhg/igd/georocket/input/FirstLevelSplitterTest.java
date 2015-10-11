package de.fhg.igd.georocket.input;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.junit.Test;

import de.fhg.igd.georocket.util.Window;
import de.fhg.igd.georocket.util.XMLStreamEvent;
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
  private List<String> split(String xml) throws Exception {
    Window window = new Window();
    window.append(Buffer.buffer(xml));
    XMLStreamReader reader = XMLInputFactory.newFactory().createXMLStreamReader(new StringReader(xml));
    FirstLevelSplitter splitter = new FirstLevelSplitter(window);
    List<String> chunks = new ArrayList<>();
    while (reader.hasNext()) {
      int event = reader.next();
      int pos = reader.getLocation().getCharacterOffset();
      String chunk = splitter.onEvent(new XMLStreamEvent(event, pos, reader));
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
    List<String> chunks = split(xml);
    assertEquals(Arrays.asList(xml), chunks);
  }
  
  /**
   * Test if an XML string with tow chunks can be split
   * @throws Exception if an error has occurred
   */
  @Test
  public void twoChunks() throws Exception {
    String xml = XMLHEADER + "<root><object><child></child></object>"
        + "<object><child2></child2></object></root>";
    List<String> chunks = split(xml);
    assertEquals(Arrays.asList(XMLHEADER + "<root>\n<object><child></child></object>\n</root>",
        XMLHEADER + "<root>\n<object><child2></child2></object>\n</root>"), chunks);
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
    List<String> chunks = split(xml);
    assertEquals(Arrays.asList(XMLHEADER + root + "\n<p:object><p:child></p:child></p:object>\n</root>",
        XMLHEADER + root + "\n<p:object><child2></child2></p:object>\n</root>"), chunks);
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
    List<String> chunks = split(xml);
    assertEquals(Arrays.asList(XMLHEADER + root + "\n<object ok=\"ov\"><child></child></object>\n</root>",
        XMLHEADER + root + "\n<object><child2></child2></object>\n</root>"), chunks);
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
    List<String> chunks = split(xml);
    assertEquals(Arrays.asList(XMLHEADER + root + "\n<p:object ok=\"ov\"><p:child></p:child></p:object>\n</root>",
        XMLHEADER + root + "\n<p:object><child2></child2></p:object>\n</root>"), chunks);
  }
}
