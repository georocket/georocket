package de.fhg.igd.georocket.util;

import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultCompoundCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Decode a OGC URN for a compound CRS. See OGC 07-092r3, section 7.5.4
 * @author Michel Kraemer
 */
public class CompoundCRSDecoder {
  private static final String AUTHORITY = "urn:ogc:def";
  private static final String PREFIX = AUTHORITY + ":crs,";
  
  /**
   * Check if the given code represents a compound CRS
   * @param code the code
   * @return true if the code represents a compound CRS, false otherwise
   */
  public static boolean isCompound(String code) {
    return code.trim().toLowerCase().startsWith(PREFIX);
  }
  
  /**
   * Decodes a given code to a {@link org.opengis.referencing.crs.CompoundCRS}
   * @param code the code
   * @return the compound CRS
   * @throws FactoryException if the code does not represent a compound CRS
   * or if one of its components could not be decoded
   */
  public static CoordinateReferenceSystem decode(String code) throws FactoryException {
    code = code.trim();
    if (!isCompound(code)) {
      throw new NoSuchAuthorityCodeException("No compound CRS", AUTHORITY, code);
    }
    
    code = code.substring(PREFIX.length());
    String[] parts = code.split(",");
    CoordinateReferenceSystem[] crss = new CoordinateReferenceSystem[parts.length];
    for (int i = 0; i < crss.length; ++i) {
      crss[i] = CRS.decode(AUTHORITY + ":" + parts[i].trim());
    }
    
    return new DefaultCompoundCRS("", crss);
  }
}
