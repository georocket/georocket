package io.georocket.util

import org.geotools.referencing.CRS
import org.opengis.referencing.FactoryException
import org.opengis.referencing.crs.CoordinateReferenceSystem

/**
 * Decode a WKT CRS. See OGC 06-103r4, section 9.2 and
 * [WKT grammar](http://docs.geotools.org/stable/javadocs/org/opengis/referencing/doc-files/WKT.html)
 * @author Tim Hellhake
 */
object WKTCRSDecoder {
  private val TYPES = arrayOf("COMPD_CS", "FITTED_CS", "GEOCCS", "GEOGCS", "LOCAL_CS", "PROJCS", "VERT_CS")

  /**
   * Check if the given code represents a WKT CRS
   * @param code the code
   * @return true if the code represents a WKT CRS, false otherwise
   */
  fun isWKT(code: String): Boolean {
    var code = code
    code = code.trim { it <= ' ' }
    for (type in TYPES) {
      if (code.startsWith(type)) {
        return true
      }
    }
    return false
  }

  /**
   * Decode a given WKT string
   * (see [WKT grammar](http://docs.geotools.org/stable/javadocs/org/opengis/referencing/doc-files/WKT.html))
   * @param wkt the WKT string
   * @return the parsed CRS
   * @throws FactoryException if the WKT string is not valid
   */
  fun decode(wkt: String): CoordinateReferenceSystem {
    return CRS.parseWKT(wkt.trim { it <= ' ' })
  }
}
