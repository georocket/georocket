package io.georocket.index.xml;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.AxisDirection;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import io.georocket.index.CRSAware;
import io.georocket.index.generic.BoundingBoxIndexer;
import io.georocket.util.CompoundCRSDecoder;
import io.georocket.util.XMLStreamEvent;

/**
 * Indexes bounding boxes of inserted chunks
 * @author Michel Kraemer
 */
public class XMLBoundingBoxIndexer extends BoundingBoxIndexer
    implements XMLIndexer, CRSAware {
  private static final CoordinateReferenceSystem WGS84 = DefaultGeographicCRS.WGS84;
  
  /**
   * The string of the detected CRS
   */
  protected String crsStr;
  
  /**
   * The detected CRS or <code>null</code> if either there was no CRS in the
   * chunk or if it was invalid
   */
  protected CoordinateReferenceSystem crs;
  
  /**
   * True if x and y are flipped in {@link #crs}
   */
  protected boolean flippedCRS;
  
  /**
   * A transformation from {@link #crs} to WGS84. <code>null</code> if
   * {@link #crs} is also <code>null</code>.
   */
  protected MathTransform transform;
  
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
  protected int srsDimension = -1;
  
  /**
   * Collects XML element contents
   */
  private StringBuilder stringBuilder;
  
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
      } else if (localName.equals("posList") || localName.equals("pos")) {
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
      } else if (localName.equals("posList") || localName.equals("pos")) {
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

    try {
      CoordinateReferenceSystem crs;
      // decode string
      if (CompoundCRSDecoder.isCompound(srsName)) {
        crs = CompoundCRSDecoder.decode(srsName);
      } else {
        crs = CRS.decode(srsName);
      }
      
      // only keep horizontal CRS
      CoordinateReferenceSystem hcrs = CRS.getHorizontalCRS(crs);
      if (hcrs != null) {
        crs = hcrs;
      }
      
      // find transformation to WGS84
      MathTransform transform = CRS.findMathTransform(crs, WGS84, true);
      boolean flippedCRS = isFlippedCRS(crs);

      this.crsStr = srsName;
      this.crs = crs;
      this.transform = transform;
      this.flippedCRS = flippedCRS;
    } catch (FactoryException e) {
      // unknown CRS or no transformation available
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
}
