package io.georocket.util;

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Decode a WKT CRS. See OGC 06-103r4, section 9.2 and
 * <a href="http://docs.geotools.org/stable/javadocs/org/opengis/referencing/doc-files/WKT.html">WKT grammar</a>
 * @author Tim Hellhake
 */
public class WKTCRSDecoder {
  private static final String[] TYPES = {"COMPD_CS", "FITTED_CS", "GEOCCS", "GEOGCS", "LOCAL_CS", "PROJCS", "VERT_CS"};

  /**
   * Check if the given code represents a WKT CRS
   * @param code the code
   * @return true if the code represents a WKT CRS, false otherwise
   */
  public static boolean isWKT(String code) {
    code = code.trim();

    for (String type : TYPES) {
      if (code.startsWith(type)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Decode a given WKT string
   * (see <a href="http://docs.geotools.org/stable/javadocs/org/opengis/referencing/doc-files/WKT.html">WKT grammar</a>)
   * @param wkt the WKT string
   * @return the parsed CRS
   * @throws FactoryException if the WKT string is not valid
   */
  public static CoordinateReferenceSystem decode(String wkt) throws FactoryException {
    return CRS.parseWKT(wkt.trim());
  }
}
