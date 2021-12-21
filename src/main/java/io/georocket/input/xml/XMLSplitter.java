package io.georocket.input.xml;

import io.georocket.input.Splitter;
import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.Window;
import io.georocket.util.XMLStartElement;
import io.georocket.util.XMLStreamEvent;
import io.vertx.core.buffer.Buffer;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Abstract base class for splitters that split XML streams
 * @author Michel Kraemer
 */
public abstract class XMLSplitter implements Splitter<XMLStreamEvent, XMLChunkMeta> {
  /**
   * A marked position. See {@link #mark(long)}
   */
  private long mark = -1;
  
  /**
   * A buffer for incoming data
   */
  private final Window window;
  
  /**
   * A stack keeping all encountered start elements
   */
  private final Deque<XMLStartElement> startElements = new ArrayDeque<>();

  /**
   * {@code true} if {@link #startElements} has changed since the last time
   * {@link #makeResult(long)} has been called.
   */
  private boolean startElementsChanged = true;

  /**
   * The [ChunkMeta] object created by the last call to {@link #makeResult(long)}
   */
  private XMLChunkMeta lastChunkMeta;

  /**
   * The prefix created by the last call to {@link #makeResult(long)}
   */
  private Buffer lastPrefix;

  /**
   * The suffix created by the last call to {@link #makeResult(long)}
   */
  private Buffer lastSuffix;
  
  /**
   * Create splitter
   * @param window a buffer for incoming data
   */
  public XMLSplitter(Window window) {
    this.window = window;
  }
  
  @Override
  public Result<XMLChunkMeta> onEvent(XMLStreamEvent event) {
    Result<XMLChunkMeta> chunk = onXMLEvent(event);
    if (!isMarked()) {
      if (event.getEvent() == XMLEvent.START_ELEMENT) {
        startElements.push(makeXMLStartElement(event.getXMLReader()));
        startElementsChanged = true;
      } else if (event.getEvent() == XMLEvent.END_ELEMENT) {
        startElements.pop();
        startElementsChanged = true;
      }
    }
    return chunk;
  }
  
  /**
   * Creates an {@link XMLStartElement} from the current parser state
   * @param xmlReader the XML parser
   * @return the {@link XMLStartElement}
   */
  private XMLStartElement makeXMLStartElement(XMLStreamReader xmlReader) {
    // copy namespaces (if there are any)
    int nc = xmlReader.getNamespaceCount();
    String[] namespacePrefixes = null;
    String[] namespaceUris = null;
    if (nc > 0) {
      namespacePrefixes = new String[nc];
      namespaceUris = new String[nc];
      for (int i = 0; i < nc; ++i) {
        namespacePrefixes[i] = xmlReader.getNamespacePrefix(i);
        namespaceUris[i] = xmlReader.getNamespaceURI(i);
      }
    }
    
    // copy attributes (if there are any)
    int ac = xmlReader.getAttributeCount();
    String[] attributePrefixes = null;
    String[] attributeLocalNames = null;
    String[] attributeValues = null;
    if (ac > 0) {
      attributePrefixes = new String[ac];
      attributeLocalNames = new String[ac];
      attributeValues = new String[ac];
      for (int i = 0; i < ac; ++i) {
        attributePrefixes[i] = xmlReader.getAttributePrefix(i);
        attributeLocalNames[i] = xmlReader.getAttributeLocalName(i);
        attributeValues[i] = xmlReader.getAttributeValue(i);
      }
    }
    
    // make element
    return new XMLStartElement(xmlReader.getPrefix(),
        xmlReader.getLocalName(), namespacePrefixes, namespaceUris,
        attributePrefixes, attributeLocalNames, attributeValues);
  }
  
  /**
   * Mark a position
   * @param pos the position to mark
   */
  protected void mark(long pos) {
    mark = pos;
  }
  
  /**
   * @return true if a position is marked currently
   */
  protected boolean isMarked() {
    return mark >= 0;
  }
  
  /**
   * Create a new chunk starting from the marked position and ending on the
   * given position. Reset the mark afterwards and advance the window to the
   * end position. Return a {@link io.georocket.input.Splitter.Result} object
   * with the new chunk and its metadata.
   * @param pos the end position
   * @return the {@link io.georocket.input.Splitter.Result} object
   */
  protected Result<XMLChunkMeta> makeResult(long pos) {
    StringBuilder sbPrefix = new StringBuilder();
    sbPrefix.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");

    if (startElementsChanged) {
      startElementsChanged = false;

      // get the full stack of start elements (backwards)
      List<XMLStartElement> chunkParents = new ArrayList<>();
      startElements.descendingIterator().forEachRemaining(e -> {
        chunkParents.add(e);
        sbPrefix.append(e);
        sbPrefix.append("\n");
      });
      lastPrefix = Buffer.buffer(sbPrefix.toString());

      // get the full stack of end elements
      StringBuilder sbSuffix = new StringBuilder();
      startElements.iterator().forEachRemaining(e ->
        sbSuffix.append("\n</").append(e.getName()).append(">"));
      lastSuffix = Buffer.buffer(sbSuffix.toString());

      lastChunkMeta = new XMLChunkMeta(chunkParents);
    }

    byte[] bytes = window.getBytes(mark, pos);
    Buffer buf = Buffer.buffer(bytes);
    window.advanceTo(pos);
    mark = -1;

    return new Result<>(buf, lastPrefix, lastSuffix, lastChunkMeta);
  }
  
  /**
   * Will be called on every XML event
   * @param event the XML event
   * @return a new {@link io.georocket.input.Splitter.Result} object (containing
   * chunk and metadata) or <code>null</code> if no result was produced
   */
  protected abstract Result<XMLChunkMeta> onXMLEvent(XMLStreamEvent event);
}
