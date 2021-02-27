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
   * A marked position. See {@link #mark(int)}
   */
  private int mark = -1;
  
  /**
   * A buffer for incoming data
   */
  private final Window window;
  
  /**
   * A stack keeping all encountered start elements
   */
  private final Deque<XMLStartElement> startElements = new ArrayDeque<>();
  
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
      } else if (event.getEvent() == XMLEvent.END_ELEMENT) {
        startElements.pop();
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
  protected void mark(int pos) {
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
  protected Result<XMLChunkMeta> makeResult(int pos) {
    StringBuilder sbStart = new StringBuilder();
    sbStart.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
    
    // append the full stack of start elements (backwards)
    List<XMLStartElement> chunkParents = new ArrayList<>();
    startElements.descendingIterator().forEachRemaining(e -> {
      chunkParents.add(e);
      sbStart.append(e);
      sbStart.append("\n");
    });
    
    // get chunk start in bytes
    Buffer buf = Buffer.buffer(sbStart.toString());
    int chunkStart = buf.length();
    
    // append current element
    byte[] bytes = window.getBytes(mark, pos);
    buf.appendBytes(bytes);
    window.advanceTo(pos);
    mark = -1;
    
    // get chunk end in bytes
    int chunkEnd = chunkStart + bytes.length;
    
    // append the full stack of end elements
    StringBuilder sbEnd = new StringBuilder();
    startElements.iterator().forEachRemaining(e ->
      sbEnd.append("\n</").append(e.getName()).append(">"));
    buf.appendString(sbEnd.toString());
    
    XMLChunkMeta meta = new XMLChunkMeta(chunkParents, chunkStart, chunkEnd);
    return new Result<>(buf, meta);
  }
  
  /**
   * Will be called on every XML event
   * @param event the XML event
   * @return a new {@link io.georocket.input.Splitter.Result} object (containing
   * chunk and metadata) or <code>null</code> if no result was produced
   */
  protected abstract Result<XMLChunkMeta> onXMLEvent(XMLStreamEvent event);
}
