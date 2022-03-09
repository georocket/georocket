package io.georocket.input.xml

import com.fasterxml.aalto.AsyncXMLInputFactory
import com.fasterxml.aalto.AsyncXMLStreamReader
import com.fasterxml.aalto.stax.InputFactoryImpl
import io.georocket.input.Splitter
import io.georocket.storage.XMLChunkMeta
import io.georocket.util.Window
import io.georocket.util.XMLStartElement
import io.georocket.util.XMLStreamEvent
import io.vertx.core.buffer.Buffer
import org.junit.Assert
import org.junit.Test
import java.nio.charset.StandardCharsets

/**
 * Test the [FirstLevelSplitter]
 * @author Michel Kraemer
 */
class FirstLevelSplitterTest {
  /**
   * Use the [FirstLevelSplitter] and split an XML string
   * @param xml the XML string
   * @return the chunks created by the splitter
   * @throws Exception if the XML string could not be parsed
   */
  @Throws(Exception::class)
  private fun split(xml: String): List<Splitter.Result<XMLChunkMeta>> {
    val window = Window()
    window.append(Buffer.buffer(xml))
    val xmlInputFactory: AsyncXMLInputFactory = InputFactoryImpl()
    val reader = xmlInputFactory.createAsyncForByteArray()
    val xmlBytes = xml.toByteArray(StandardCharsets.UTF_8)
    reader.inputFeeder.feedInput(xmlBytes, 0, xmlBytes.size)
    val splitter = FirstLevelSplitter(window)
    val chunks: MutableList<Splitter.Result<XMLChunkMeta>> = ArrayList()
    while (reader.hasNext()) {
      val event = reader.next()
      if (event == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
        reader.close()
        continue
      }
      val pos = reader.location.characterOffset
      val chunk = splitter.onEvent(
        XMLStreamEvent(event, pos.toLong(), reader)
      )
      if (chunk != null) {
        chunks.add(chunk)
      }
    }
    return chunks
  }

  /**
   * Test if an XML string with one chunk can be split
   * @throws Exception if an error has occurred
   */
  @Test
  @Throws(Exception::class)
  fun oneChunk() {
    val contents = "<object><child></child></object>"
    val xml = XMLHEADER + PREFIX + contents + SUFFIX
    val chunks = split(xml)
    Assert.assertEquals(1, chunks.size.toLong())
    val (chunk1, prefix, suffix, meta1) = chunks[0]
    val meta = XMLChunkMeta(listOf(XMLStartElement(localName = "root")))
    Assert.assertEquals(meta, meta1)
    Assert.assertEquals(XMLHEADER + PREFIX, prefix.toString())
    Assert.assertEquals(SUFFIX, suffix.toString())
    Assert.assertEquals(contents, chunk1.toString())
  }

  /**
   * Test if an XML string with tow chunks can be split
   * @throws Exception if an error has occurred
   */
  @Test
  @Throws(Exception::class)
  fun twoChunks() {
    val contents1 = "<object><child></child></object>"
    val contents2 = "<object><child2></child2></object>"
    val xml = XMLHEADER + PREFIX + contents1 + contents2 + SUFFIX
    val chunks = split(xml)
    Assert.assertEquals(2, chunks.size.toLong())
    val (chunk, prefix, suffix, meta1) = chunks[0]
    val (chunk1, prefix1, suffix1, meta2) = chunks[1]
    val parents = listOf(XMLStartElement(localName = "root"))
    val meta = XMLChunkMeta(parents)
    Assert.assertEquals(meta, meta1)
    Assert.assertEquals(XMLHEADER + PREFIX, prefix.toString())
    Assert.assertEquals(SUFFIX, suffix.toString())
    Assert.assertEquals(meta, meta2)
    Assert.assertEquals(XMLHEADER + PREFIX, prefix1.toString())
    Assert.assertEquals(SUFFIX, suffix1.toString())
    Assert.assertEquals(contents1, chunk.toString())
    Assert.assertEquals(contents2, chunk1.toString())
  }

  /**
   * Test if an XML string with two chunks and a namespace can be split
   * @throws Exception if an error has occurred
   */
  @Test
  @Throws(Exception::class)
  fun namespace() {
    val contents1 = "<p:object><p:child></p:child></p:object>"
    val contents2 = "<p:object><child2></child2></p:object>"
    val root = "<root xmlns=\"http://example.com\" xmlns:p=\"http://example.com\">"
    val xml = "$XMLHEADER$root$contents1$contents2</root>"
    val chunks = split(xml)
    Assert.assertEquals(2, chunks.size.toLong())
    val (chunk, prefix, suffix, meta1) = chunks[0]
    val (chunk1, prefix1, suffix1, meta2) = chunks[1]
    val parents = listOf(
      XMLStartElement(
        null,
        "root",
        namespacePrefixes = listOf(null, "p"),
        namespaceUris = listOf("http://example.com", "http://example.com")
      )
    )
    val meta = XMLChunkMeta(parents)
    Assert.assertEquals(meta, meta1)
    Assert.assertEquals("$XMLHEADER$root\n", prefix.toString())
    Assert.assertEquals(SUFFIX, suffix.toString())
    Assert.assertEquals(meta, meta2)
    Assert.assertEquals("$XMLHEADER$root\n", prefix1.toString())
    Assert.assertEquals(SUFFIX, suffix1.toString())
    Assert.assertEquals(contents1, chunk.toString())
    Assert.assertEquals(contents2, chunk1.toString())
  }

  /**
   * Test if an XML string with two chunks and a attributes can be split
   * @throws Exception if an error has occurred
   */
  @Test
  @Throws(Exception::class)
  fun attributes() {
    val contents1 = "<object ok=\"ov\"><child></child></object>"
    val contents2 = "<object><child2></child2></object>"
    val root = "<root key=\"value\" key2=\"value2\">"
    val xml = "$XMLHEADER$root$contents1$contents2</root>"
    val chunks = split(xml)
    Assert.assertEquals(2, chunks.size.toLong())
    val (chunk, prefix, suffix, meta1) = chunks[0]
    val (chunk1, prefix1, suffix1, meta2) = chunks[1]
    val parents = listOf(
      XMLStartElement(
        null,
        "root",
        attributePrefixes = listOf(null, null),
        attributeLocalNames = listOf("key", "key2"),
        attributeValues = listOf("value", "value2")
      )
    )
    val meta = XMLChunkMeta(parents)
    Assert.assertEquals(meta, meta1)
    Assert.assertEquals("$XMLHEADER$root\n", prefix.toString())
    Assert.assertEquals(SUFFIX, suffix.toString())
    Assert.assertEquals(meta, meta2)
    Assert.assertEquals("$XMLHEADER$root\n", prefix1.toString())
    Assert.assertEquals(SUFFIX, suffix1.toString())
    Assert.assertEquals(contents1, chunk.toString())
    Assert.assertEquals(contents2, chunk1.toString())
  }

  /**
   * Test if an XML string with two chunks, a namespace and attributes can be split
   * @throws Exception if an error has occurred
   */
  @Test
  @Throws(Exception::class)
  fun full() {
    val contents1 = "<p:object ok=\"ov\"><p:child></p:child></p:object>"
    val contents2 = "<p:object><child2></child2></p:object>"
    val root = "<root xmlns=\"http://example.com\" xmlns:p=\"http://example.com\" key=\"value\" key2=\"value2\">"
    val xml = "$XMLHEADER$root$contents1$contents2</root>"
    val chunks = split(xml)
    Assert.assertEquals(2, chunks.size.toLong())
    val (chunk, prefix, suffix, meta1) = chunks[0]
    val (chunk1, prefix1, suffix1, meta2) = chunks[1]
    val parents = listOf(
      XMLStartElement(
        null,
        "root",
        namespacePrefixes = listOf(null, "p"),
        namespaceUris = listOf("http://example.com", "http://example.com"),
        attributePrefixes = listOf(null, null),
        attributeLocalNames = listOf("key", "key2"),
        attributeValues = listOf("value", "value2")
      )
    )
    val meta = XMLChunkMeta(parents)
    Assert.assertEquals(meta, meta1)
    Assert.assertEquals("$XMLHEADER$root\n", prefix.toString())
    Assert.assertEquals(SUFFIX, suffix.toString())
    Assert.assertEquals(meta, meta2)
    Assert.assertEquals("$XMLHEADER$root\n", prefix1.toString())
    Assert.assertEquals(SUFFIX, suffix1.toString())
    Assert.assertEquals(contents1, chunk.toString())
    Assert.assertEquals(contents2, chunk1.toString())
  }

  /**
   * Test if an XML string with an UTF8 character can be split
   * @throws Exception if an error has occurred
   */
  @Test
  @Throws(Exception::class)
  fun utf8() {
    val contents = "<object><child name=\"\u2248\"></child></object>"
    val xml = XMLHEADER + PREFIX + contents + SUFFIX
    val chunks = split(xml)
    Assert.assertEquals(1, chunks.size.toLong())
    val (chunk1, prefix, suffix, meta1) = chunks[0]
    val meta = XMLChunkMeta(listOf(XMLStartElement(localName = "root")))
    Assert.assertEquals(meta, meta1)
    Assert.assertEquals(XMLHEADER + PREFIX, prefix.toString())
    Assert.assertEquals(SUFFIX, suffix.toString())
    Assert.assertEquals(contents, chunk1.toString())
  }

  companion object {
    private const val XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
    private const val PREFIX = "<root>\n"
    private const val SUFFIX = "\n</root>"
  }
}
