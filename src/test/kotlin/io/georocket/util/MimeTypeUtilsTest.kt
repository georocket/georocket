package io.georocket.util

import io.georocket.util.MimeTypeUtils.JSON
import io.georocket.util.MimeTypeUtils.XML
import io.georocket.util.MimeTypeUtils.belongsTo
import io.georocket.util.MimeTypeUtils.detect
import org.apache.commons.io.FileUtils
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.IOException
import java.nio.charset.StandardCharsets

/**
 * Test cases for [MimeTypeUtils]
 * @author Michel Kraemer
 */
class MimeTypeUtilsTest {
  /**
   * A folder keeping temporary files for the tests
   */
  @Rule
  @JvmField
  var folder = TemporaryFolder()

  /**
   * Test the [MimeTypeUtils.belongsTo] method
   */
  @Test
  fun testBelongsTo() {
    Assert.assertTrue(belongsTo("application/gml+xml", "application", "xml"))
    Assert.assertTrue(belongsTo("application/exp+xml", "application", "xml"))
    Assert.assertTrue(belongsTo("application/xml", "application", "xml"))
    Assert.assertFalse(belongsTo("application/exp+xml", "text", "xml"))
    Assert.assertFalse(belongsTo("application/exp+xml", "application", "json"))
  }

  /**
   * Check if a JSON file can be detected
   * @throws IOException if the temporary file could not be read
   */
  @Test
  @Throws(IOException::class)
  fun detectJSON() {
    val tempFile = folder.newFile()
    FileUtils.write(tempFile, "   \n  {\"name\": \"Elvis\"}", StandardCharsets.UTF_8)
    assertEquals(JSON, detect(tempFile))
  }

  /**
   * Check if an XML file can be detected
   * @throws IOException if the temporary file could not be read
   */
  @Test
  @Throws(IOException::class)
  fun detectXML() {
    val tempFile = folder.newFile()
    FileUtils.write(tempFile, "   \n\n\n  <root></root>  ", StandardCharsets.UTF_8)
    assertEquals(XML, detect(tempFile))
  }

  /**
   * Check if a file with an unknown file type can really be not detected
   * @throws IOException if the temporary file could not be read
   */
  @Test
  @Throws(IOException::class)
  fun detectNone() {
    val tempFile = folder.newFile()
    FileUtils.write(tempFile, "   \n\n\n  ", StandardCharsets.UTF_8)
    Assert.assertNull(detect(tempFile))
  }
}
