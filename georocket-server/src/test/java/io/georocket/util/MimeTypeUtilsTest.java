package io.georocket.util;

import static io.georocket.util.MimeTypeUtils.JSON;
import static io.georocket.util.MimeTypeUtils.XML;
import static io.georocket.util.MimeTypeUtils.belongsTo;
import static io.georocket.util.MimeTypeUtils.detect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test cases for {@link MimeTypeUtils}
 * @author Michel Kraemer
 */
public class MimeTypeUtilsTest {
  /**
   * A folder keeping temporary files for the tests
   */
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  /**
   * Test the {@link MimeTypeUtils#belongsTo(String, String, String)} method
   */
  @Test
  public void testBelongsTo() {
    assertTrue(belongsTo("application/gml+xml", "application", "xml"));
    assertTrue(belongsTo("application/exp+xml", "application", "xml"));
    assertTrue(belongsTo("application/xml", "application", "xml"));
    assertFalse(belongsTo("application/exp+xml", "text", "xml"));
    assertFalse(belongsTo("application/exp+xml", "application", "json"));
  }
  
  /**
   * Check if a JSON file can be detected
   * @throws IOException if the temporary file could not be read
   */
  @Test
  public void detectJSON() throws IOException {
    File tempFile = folder.newFile();
    FileUtils.write(tempFile, "   \n  {\"name\": \"Elvis\"}", StandardCharsets.UTF_8);
    assertEquals(JSON, detect(tempFile));
  }
  
  /**
   * Check if an XML file can be detected
   * @throws IOException if the temporary file could not be read
   */
  @Test
  public void detectXML() throws IOException {
    File tempFile = folder.newFile();
    FileUtils.write(tempFile, "   \n\n\n  <root></root>  ", StandardCharsets.UTF_8);
    assertEquals(XML, detect(tempFile));
  }
  
  /**
   * Check if a file with an unknown file type can really be not detected
   * @throws IOException if the temporary file could not be read
   */
  @Test
  public void detectNone() throws IOException {
    File tempFile = folder.newFile();
    FileUtils.write(tempFile, "   \n\n\n  ", StandardCharsets.UTF_8);
    assertNull(detect(tempFile));
  }
}
