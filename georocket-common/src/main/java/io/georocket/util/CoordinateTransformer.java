package io.georocket.util;

import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.AxisDirection;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import static org.geotools.referencing.crs.DefaultGeographicCRS.WGS84;

/**
 * Transforms coordinates from a specified CRS to {@link DefaultGeographicCRS#WGS84}
 * @author Tim Hellhake
 */
public class CoordinateTransformer {
  private final boolean flipped;
  private final MathTransform transform;

  /**
   * Create transformer from CRS
   * @param code the CRS code
   * @throws FactoryException if the CRS code is unknown or no transformation
   *                          for the CRS could be found
   */
  public CoordinateTransformer(String code) throws FactoryException {
    this(CRS.decode(code));
  }

  /**
   * Create transformer from CRS
   * @param crs the CRS
   * @throws FactoryException if no transformation for the CRS could be found
   */
  public CoordinateTransformer(CoordinateReferenceSystem crs) throws FactoryException {
    this.flipped = isFlipped(crs);
    this.transform = CRS.findMathTransform(crs, WGS84, true);
  }

  /**
   * Transform an array of coordinates to {@link DefaultGeographicCRS#WGS84}
   * @param from the source array of coordinates
   * @param dimension the dimension of the source coordinates. Allowed values
   *                  are &lt;= 0, 2 and 3. If the value is &lt;= 0 the dimension
   *                  will be guessed (see {@link #guessDimension(double[])}).
   * @return a new array with the destination coordinates of null if the dimension
   *         in unknown or the source coordinates could not be transformed
   * @throws TransformException if the coordinates could not be transformed
   */
  public double[] transform(double[] from, int dimension) throws TransformException {
    if (dimension <= 0) {
      dimension = guessDimension(from);
    }

    if (dimension != 2 && dimension != 3) {
      return null;
    }

    if (from.length % dimension != 0) {
      return null;
    }

    int count = from.length / dimension;

    for (int i = 0; i < from.length; i += dimension) {
      if (flipped) {
        double tmp = from[i];
        from[i] = from[i + 1];
        from[i + 1] = tmp;
      }
    }

    double[] destination = new double[count * 2];
    transform.transform(from, 0, destination, 0, count);

    return destination;
  }

  /**
   * Guess the dimension by the length of the array
   * @param points the coordinates array
   * @return the guessed dimension, might be 2, 3 or -1 if the dimension is unknown
   */
  private static int guessDimension(double[] points) {
    if (points.length % 2 == 0) {
      return 2;
    }

    if (points.length % 3 == 0) {
      return 3;
    }

    return -1;
  }

  /**
   * Check if x and y are flipped in the given CRS
   * @param crs the CRS
   * @return true if x and y are flipped, false otherwise
   */
  private static boolean isFlipped(CoordinateReferenceSystem crs) {
    if (crs.getCoordinateSystem().getDimension() == 2) {
      AxisDirection direction = crs.getCoordinateSystem().getAxis(0).getDirection();

      if (direction.equals(AxisDirection.NORTH) ||
        direction.equals(AxisDirection.UP)) {
        return true;
      }
    }

    return false;
  }
}
