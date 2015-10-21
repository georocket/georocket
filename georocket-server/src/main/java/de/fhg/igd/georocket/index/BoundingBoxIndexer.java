package de.fhg.igd.georocket.index;

import java.util.Arrays;
import java.util.Map;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.google.common.collect.ImmutableMap;

import de.fhg.igd.georocket.util.CompoundCRSDecoder;
import de.fhg.igd.georocket.util.XMLStreamEvent;

/**
 * Indexes bounding boxes of inserted chunks
 * @author Michel Kraemer
 */
public class BoundingBoxIndexer implements Indexer {
  private static final CoordinateReferenceSystem WGS84 = DefaultGeographicCRS.WGS84;
  
  /**
   * The string of the detected CRS
   */
  private String crsStr;
  
  /**
   * The detected CRS or <code>null</code> if either there was no CRS in the
   * chunk or if it was invalid
   */
  private CoordinateReferenceSystem crs;
  
  /**
   * A transformation from {@link #crs} to WGS84. <code>null</code> if
   * {@link #crs} is also <code>null</code>.
   */
  private MathTransform transform;
  
  /**
   * True if we're currently parsing a <code>&lt;lowerCorner&gt;</code> or
   * <code>&lt;upperCorner&gt;</code> element
   */
  private boolean parseCorner = false;
  
  /**
   * True if we're currently parsing a <code>&lt;posList&gt;</code> element
   */
  private boolean parsePosList = false;
  
  /**
   * The spatial dimension of the element we're currently parsing or
   * <code>-1</code> if it could not be determined or if we are currently
   * not parsing an element with spatial content.
   */
  private int srsDimension = -1;
  
  /**
   * Collects XML element contents
   */
  private StringBuilder stringBuilder;
  
  /**
   * True if {@link #addToBoundingBox(double, double)} has been called
   * at least once
   */
  private boolean boundingBoxInitialized = false;
  
  /**
   * The calculated bounding box of this chunk. Only contains valid values
   * if {@link #boundingBoxInitialized} is true
   */
  private double minX;
  private double maxX;
  private double minY;
  private double maxY;
  
  @Override
  public void onEvent(XMLStreamEvent event) {
    XMLStreamReader reader = event.getXMLReader();
    if (event.getEvent() == XMLEvent.START_ELEMENT) {
      // parse SRS and try to decode it
      handleSrsName(getAttribute("srsName", reader));
      
      // check if we've got an element containing spatial coordinates
      String localName = reader.getLocalName();
      if (localName.equals("Envelope")) {
        // try to parse the spatial dimension of this envelope
        handleSrsDimension(getAttribute("srsDimension", reader));
      } else if (localName.equals("lowerCorner") || localName.equals("upperCorner")) {
        // lower and upper corner of an envelope
        parseCorner = true;
        stringBuilder = new StringBuilder();
      } else if (localName.equals("posList")) {
        // list of positions of a GML geometry
        parsePosList = true;
        stringBuilder = new StringBuilder();
        handleSrsDimension(getAttribute("srsDimension", reader));
      }
    } else if (event.getEvent() == XMLEvent.CHARACTERS) {
      if (parseCorner || parsePosList) {
        // collect contents and parse them when we encounter the end element
        stringBuilder.append(reader.getText());
      }
    } else if (event.getEvent() == XMLEvent.END_ELEMENT) {
      String localName = reader.getLocalName();
      if (localName.equals("Envelope")) {
        srsDimension = -1;
      } else if (localName.equals("lowerCorner") || localName.equals("upperCorner")) {
        handlePosList(stringBuilder.toString());
        stringBuilder = null;
        parseCorner = false;
      } else if (localName.equals("posList")) {
        handlePosList(stringBuilder.toString());
        stringBuilder = null;
        parsePosList = false;
        srsDimension = -1;
      }
    }
  }
  
  /**
   * Find an attribute's value. Just compare the attribute's local name and
   * ignore the namespace
   * @param localName the attribute's local name
   * @param reader the XML reader
   * @return the attribute's value or null if there is no such attibute
   */
  private String getAttribute(String localName, XMLStreamReader reader) {
    int ac = reader.getAttributeCount();
    for (int i = 0; i < ac; ++i) {
      String name = reader.getAttributeLocalName(i);
      if (name.equalsIgnoreCase(localName)) {
        return reader.getAttributeValue(i);
      }
    }
    return null;
  }
  
  /**
   * Parse and decode a CRS string
   * @param srsName the CRS string (may be null)
   */
  private void handleSrsName(String srsName) {
    if (srsName == null || srsName.isEmpty()) {
      return;
    }
    
    if (crsStr != null && crsStr.equals(srsName)) {
      // same string, no need to parse
      return;
    }
    
    crsStr = srsName;
    try {
      // decode string
      if (CompoundCRSDecoder.isCompound(crsStr)) {
        crs = CompoundCRSDecoder.decode(crsStr);
      } else {
        crs = CRS.decode(crsStr);
      }
      
      // only keep horizontal CRS
      crs = CRS.getHorizontalCRS(crs);
      
      // find transformation to WGS84
      transform = CRS.findMathTransform(crs, WGS84, true);
    } catch (FactoryException e) {
      // unknown CRS or no transformation available
      crsStr = null;
      transform = null;
      crs = null;
    }
  }
  
  /**
   * Parse the <code>srsDimension</code> attribute
   * @param srsDimension the attribute's value
   */
  private void handleSrsDimension(String srsDimension) {
    if (srsDimension == null || srsDimension.isEmpty()) {
      this.srsDimension = -1;
      return;
    }
    try {
      this.srsDimension = Integer.parseInt(srsDimension);
    } catch (NumberFormatException e) {
      this.srsDimension = -1;
    }
  }
  
  /**
   * Handle the contents of a <code>posList</code> element. Parses all
   * coordinates and adds them to the bounding box.
   * @param text the contents
   */
  private void handlePosList(String text) {
    if (crs == null || text == null || text.isEmpty()) {
      return;
    }
    
    String[] coordinates = text.trim().split("\\s+");
    
    // check if dimension is correct
    int dim = srsDimension;
    if (dim <= 0) {
      if (coordinates.length % 3 == 0) {
        dim = 3;
      } else if (coordinates.length % 2 == 0) {
        dim = 2;
      } else {
        // unknown dimension. ignore this pos list
        return;
      }
    }
    
    // convert strings to doubles
    int count = coordinates.length / dim;
    double[] srcCoords = new double[count * 2];
    double[] dstCoords = new double[count * 2];
    try {
      for (int i = 0; i < count; ++i) {
        srcCoords[i * 2] = Double.parseDouble(coordinates[i * dim]);
        srcCoords[i * 2 + 1] = Double.parseDouble(coordinates[i * dim + 1]);
      }
    } catch (NumberFormatException e) {
      // invalid coordinates. ignore.
      return;
    }
    
    try {
      // transform coordinates to WGS84
      transform.transform(srcCoords, 0, dstCoords, 0, count);
      for (int i = 0; i < count; ++i) {
        // add coordinates to bounding box
        addToBoundingBox(dstCoords[i * 2], dstCoords[i * 2 + 1]);
      }
    } catch (TransformException e) {
      // ignore
    }
  }
  
  /**
   * Adds the given coordinate to the bounding box
   * @param x the x ordinate
   * @param y the y ordinate
   */
  private void addToBoundingBox(double x, double y) {
    if (!boundingBoxInitialized) {
      minX = maxX = x;
      minY = maxY = y;
      boundingBoxInitialized = true;
    } else {
      if (x < minX) {
        minX = x;
      }
      if (x > maxX) {
        maxX = x;
      }
      if (y < minY) {
        minY = y;
      }
      if (y > maxY) {
        maxY = y;
      }
    }
  }
  
  @Override
  public Map<String, Object> getResult() {
    if (!boundingBoxInitialized) {
      // the chunk's bounding box is unknown. do not add it to the index
      return ImmutableMap.of();
    }
    return ImmutableMap.of("bbox", ImmutableMap.of(
        "type", "envelope",
        "coordinates", Arrays.asList(
            Arrays.asList(minX, maxY), // upper left
            Arrays.asList(maxX, minY)  // lower right
        )
    ));
  }
}
