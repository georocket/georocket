package io.georocket.input.xml

import io.georocket.input.Splitter
import io.georocket.storage.GenericXmlChunkMeta
import io.georocket.storage.XmlChunkMeta
import io.georocket.util.Window
import io.georocket.util.XMLStartElement
import io.georocket.util.XMLStreamEvent
import io.vertx.core.buffer.Buffer
import java.util.*
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent

/**
 * Abstract base class for splitters that split XML streams
 * @author Michel Kraemer
 */
abstract class XMLSplitter(private val window: Window) : Splitter<XMLStreamEvent, XmlChunkMeta> {

  /**
   * A marked position. See [.mark]
   */
  private var mark: Long = -1

  /**
   * A stack keeping all encountered start elements
   */
  private val startElements: Deque<XMLStartElement> = ArrayDeque()

  /**
   * `true` if [.startElements] has changed since the last time
   * [.makeResult] has been called.
   */
  private var startElementsChanged = true

  /**
   * The [ChunkMeta] object created by the last call to [.makeResult]
   */
  private var lastChunkMeta: XmlChunkMeta? = null

  /**
   * The prefix created by the last call to [.makeResult]
   */
  private var lastPrefix: Buffer? = null

  /**
   * The suffix created by the last call to [.makeResult]
   */
  private var lastSuffix: Buffer? = null

  override fun onEvent(event: XMLStreamEvent): Splitter.Result<XmlChunkMeta>? {
    val chunk = onXMLEvent(event)
    if (!isMarked) {
      if (event.event == XMLEvent.START_ELEMENT) {
        startElements.push(makeXMLStartElement(event.xmlReader))
        startElementsChanged = true
      } else if (event.event == XMLEvent.END_ELEMENT) {
        startElements.pop()
        startElementsChanged = true
      }
    }
    return chunk
  }

  /**
   * Creates an [XMLStartElement] from the current parser state
   * @param xmlReader the XML parser
   * @return the [XMLStartElement]
   */
  private fun makeXMLStartElement(xmlReader: XMLStreamReader): XMLStartElement {
    // copy namespaces (if there are any)
    val nc = xmlReader.namespaceCount
    val namespacePrefixes = (0 until nc).map { index ->
      xmlReader.getNamespacePrefix(index).takeIf { it.isNotEmpty() }
    }
    val namespaceUris = (0 until nc).map { index ->
      xmlReader.getNamespaceURI(index)
    }

    // copy attributes (if there are any)
    val ac = xmlReader.attributeCount
    val attributePrefixes = (0 until ac).map { index ->
      xmlReader.getAttributePrefix(index).takeIf { it.isNotEmpty() }
    }
    val attributeLocalNames = (0 until ac).map { index ->
      xmlReader.getAttributeLocalName(index)
    }
    val attributeValues = (0 until ac).map { index ->
      xmlReader.getAttributeValue(index)
    }

    // make element
    return XMLStartElement(
      xmlReader.prefix.takeIf { it.isNotEmpty() },
      xmlReader.localName,
      namespacePrefixes,
      namespaceUris,
      attributePrefixes,
      attributeLocalNames,
      attributeValues
    )
  }

  /**
   * Mark a position
   * @param pos the position to mark
   */
  protected fun mark(pos: Long) {
    mark = pos
  }

  /**
   * @return true if a position is marked currently
   */
  protected val isMarked: Boolean
    get() = mark >= 0

  /**
   * Create a new chunk starting from the marked position and ending on the
   * given position. Reset the mark afterwards and advance the window to the
   * end position. Return a [io.georocket.input.Splitter.Result] object
   * with the new chunk and its metadata.
   * @param pos the end position
   * @return the [io.georocket.input.Splitter.Result] object
   */
  protected fun makeResult(pos: Long): Splitter.Result<XmlChunkMeta> {
    if (startElementsChanged) {
      startElementsChanged = false

      // get the full stack of start elements (backwards)
      val sbPrefix = StringBuilder()
      sbPrefix.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n")
      val chunkParents: MutableList<XMLStartElement> = ArrayList()
      startElements.descendingIterator().forEachRemaining { e: XMLStartElement ->
        chunkParents.add(e)
        sbPrefix.append(e)
        sbPrefix.append("\n")
      }
      lastPrefix = Buffer.buffer(sbPrefix.toString())

      // get the full stack of end elements
      val sbSuffix = StringBuilder()
      startElements.forEach { e -> sbSuffix.append("\n</").append(e.name).append(">") }
      lastSuffix = Buffer.buffer(sbSuffix.toString())
      lastChunkMeta = GenericXmlChunkMeta(chunkParents)
    }
    val bytes = window.getBytes(mark, pos)
    val buf = Buffer.buffer(bytes)
    window.advanceTo(pos)
    mark = -1
    return Splitter.Result(buf, lastPrefix!!, lastSuffix!!, lastChunkMeta!!)
  }

  /**
   * Will be called on every XML event
   * @param event the XML event
   * @return a new [io.georocket.input.Splitter.Result] object (containing
   * chunk and metadata) or `null` if no result was produced
   */
  protected abstract fun onXMLEvent(event: XMLStreamEvent): Splitter.Result<XmlChunkMeta>?
}
