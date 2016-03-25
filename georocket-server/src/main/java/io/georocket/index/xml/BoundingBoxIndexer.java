package io.georocket.index.xml;

import java.util.Arrays;
import java.util.Map;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.AxisDirection;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.google.common.collect.ImmutableMap;

import io.georocket.index.CRSAware;
import io.georocket.util.CompoundCRSDecoder;
import io.georocket.util.XMLStreamEvent;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Indexes bounding boxes of inserted chunks
 * @author Michel Kraemer
 */
public class BoundingBoxIndexer implements XMLIndexer, CRSAware {
  private static final CoordinateReferenceSystem WGS84 = DefaultGeographicCRS.WGS84;
  private static Logger log = LoggerFactory.getLogger(BoundingBoxIndexer.class);
  
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
   * True if x and y are flipped in {@link #crs}
   */
  private boolean flippedCRS;
  
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
  
  /**
   * Check if x and y are flipped in the given CRS
   * @param crs the CRS
   * @return true if x and y are flipped, false otherwise
   */
  public static boolean isFlippedCRS(CoordinateReferenceSystem crs) {
    if (crs.getCoordinateSystem().getDimension() == 2) {
      AxisDirection direction = crs.getCoordinateSystem().getAxis(0).getDirection();
      if (direction.equals(AxisDirection.NORTH) ||
        direction.equals(AxisDirection.UP)) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public void setFallbackCRSString(String crsStr) {
    handleSrsName(crsStr);
  }
  
  @Override
  public void onEvent(XMLStreamEvent event) {
    XMLStreamReader reader = event.getXMLReader();
    if (event.getEvent() == XMLEvent.START_ELEMENT) {
      // parse SRS and try to decode it
      handleSrsName(reader.getAttributeValue(null, "srsName"));
      
      // check if we've got an element containing spatial coordinates
      String localName = reader.getLocalName();
      if (localName.equals("Envelope")) {
        // try to parse the spatial dimension of this envelope
        handleSrsDimension(reader.getAttributeValue(null, "srsDimension"));
      } else if (localName.equals("lowerCorner") || localName.equals("upperCorner")) {
        // lower and upper corner of an envelope
        parseCorner = true;
        stringBuilder = new StringBuilder();
      } else if (localName.equals("posList")) {
        // list of positions of a GML geometry
        parsePosList = true;
        stringBuilder = new StringBuilder();
        handleSrsDimension(reader.getAttributeValue(null, "srsDimension"));
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
      CoordinateReferenceSystem hcrs = CRS.getHorizontalCRS(crs);
      if (hcrs != null) {
        crs = hcrs;
      }
      
      // find transformation to WGS84
      transform = CRS.findMathTransform(crs, WGS84, true);
      flippedCRS = isFlippedCRS(crs);
    } catch (FactoryException e) {
      // unknown CRS or no transformation available
      crsStr = null;
      transform = null;
      crs = null;
      flippedCRS = false;
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
        int j1 = i * 2;
        int j2 = j1 + 1;
        if (flippedCRS) {
          j2 = j1;
          j1 = j2 + 1;
        }
        srcCoords[j1] = Double.parseDouble(coordinates[i * dim]);
        srcCoords[j2] = Double.parseDouble(coordinates[i * dim + 1]);
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
  
  /**
   * Check if the current bounding box contains valid values for WGS84
   * @return true if the current bounding box is valid, false otherwise
   */
  private boolean validate() {
    return (minX >= -180.0 && minY >= -90.0 && maxX <= 180.0 && maxY <= 90.0);
  }
  
  @Override
  public Map<String, Object> getResult() {
    if (!boundingBoxInitialized) {
      // the chunk's bounding box is unknown. do not add it to the index
      return ImmutableMap.of();
    }
    if (!validate()) {
      boundingBoxInitialized = false;
      log.warn("Invalid bounding box [" + minX + "," + minY + "," + maxX + "," + maxY + "]. "
          + "Values outside [-180.0, -90.0, 180.0, 90.0]. Skipping chunk.");
      return ImmutableMap.of();
    }
    //System.out.println(minX + "," + minY + "," + maxX + "," + maxY);
    return ImmutableMap.of("bbox", ImmutableMap.of(
        "type", "envelope",
        "coordinates", Arrays.asList(
            Arrays.asList(minX, maxY), // upper left
            Arrays.asList(maxX, minY)  // lower right
        )
    ));
  }
}
