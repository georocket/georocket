package io.georocket.input.xml

import io.georocket.input.Splitter
import io.georocket.storage.XmlChunkMeta
import io.georocket.util.Window
import io.georocket.util.XMLStreamEvent
import javax.xml.stream.events.XMLEvent

/**
 * Splits incoming XML tokens whenever a token in the first level (i.e. a
 * child of the XML document's root node) is encountered
 * @author Michel Kraemer
 */
class FirstLevelSplitter(window: Window) : XMLSplitter(window) {

  private var depth = 0

  override fun onXMLEvent(event: XMLStreamEvent): Splitter.Result<XmlChunkMeta>? {
    var result: Splitter.Result<XmlChunkMeta>? = null

    // create new chunk if we're just after the end of a first-level element
    if (depth == 1 && isMarked) {
      result = makeResult(event.pos)
    }
    when (event.event) {
      XMLEvent.START_ELEMENT -> {
        if (depth == 1) {
          mark(event.pos)
        }
        ++depth
      }
      XMLEvent.END_ELEMENT -> --depth
      else -> {}
    }
    return result
  }
}
