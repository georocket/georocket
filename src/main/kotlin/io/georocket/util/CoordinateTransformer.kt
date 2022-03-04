package io.georocket.util

import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.referencing.FactoryException
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.cs.AxisDirection
import org.opengis.referencing.operation.MathTransform
import org.opengis.referencing.operation.TransformException

/**
 * Transforms coordinates from a specified CRS to [DefaultGeographicCRS.WGS84]
 * @author Tim Hellhake
 */
class CoordinateTransformer(crs: CoordinateReferenceSystem) {
  private val flipped: Boolean
  private val transform: MathTransform

  init {
    flipped = isFlipped(crs)
    transform = CRS.findMathTransform(crs, DefaultGeographicCRS.WGS84, true)
  }

  /**
   * Transform an array of coordinates to [DefaultGeographicCRS.WGS84]
   * @param from the source array of coordinates
   * @param dimension the dimension of the source coordinates. Allowed values
   * are &lt;= 0, 2 and 3. If the value is &lt;= 0 the dimension will be
   * guessed (see [.guessDimension]).
   * @return a new array with the destination coordinates of null if the dimension
   * is unknown or the source coordinates could not be transformed
   * @throws TransformException if the coordinates could not be transformed
   */
  fun transform(from: DoubleArray, dimension: Int): DoubleArray? {
    var dimension = dimension
    if (dimension <= 0) {
      dimension = guessDimension(from)
    }
    if (dimension != 2 && dimension != 3) {
      return null
    }
    if (from.size % dimension != 0) {
      return null
    }
    val count = from.size / dimension
    var i = 0
    while (i < from.size) {
      if (flipped) {
        val tmp = from[i]
        from[i] = from[i + 1]
        from[i + 1] = tmp
      }
      i += dimension
    }
    val destination = DoubleArray(count * 2)
    transform.transform(from, 0, destination, 0, count)
    return destination
  }

  companion object {
    /**
     * Guess the dimension by the length of the array
     * @param points the coordinates array
     * @return the guessed dimension, might be 2, 3 or -1 if the dimension is unknown
     */
    private fun guessDimension(points: DoubleArray): Int {
      if (points.size % 2 == 0) {
        return 2
      }
      return if (points.size % 3 == 0) {
        3
      } else -1
    }

    /**
     * Check if x and y are flipped in the given CRS
     * @param crs the CRS
     * @return true if x and y are flipped, false otherwise
     */
    private fun isFlipped(crs: CoordinateReferenceSystem): Boolean {
      if (crs.coordinateSystem.dimension == 2) {
        val direction = crs.coordinateSystem.getAxis(0).direction
        if (direction == AxisDirection.NORTH || direction == AxisDirection.UP) {
          return true
        }
      }
      return false
    }

    /**
     * Decode CRS from a string. This might be:
     *
     *  * a simple CRS (see [CRS.decode])
     *  * a compound CRS (see [CompoundCRSDecoder.decode])
     *  * a WKT string (see [WKTCRSDecoder.decode])
     *
     * @param s the string
     * @return the parsed CRS
     * @throws FactoryException if the CRS could not be parsed
     */
    fun decode(s: String): CoordinateReferenceSystem {
      if (CompoundCRSDecoder.isCompound(s)) {
        return CompoundCRSDecoder.decode(s)
      }
      return if (WKTCRSDecoder.isWKT(s)) {
        WKTCRSDecoder.decode(s)
      } else CRS.decode(s)
    }
  }
}
