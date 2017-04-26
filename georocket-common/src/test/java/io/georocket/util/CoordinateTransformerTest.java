package io.georocket.util;

import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

/**
 * Test the {@link CoordinateTransformer}
 * @author Tim Hellhake
 */
public class CoordinateTransformerTest {
  /**
   * Test transformation with EPSG code
   */
  @Test
  public void testEPSG() throws FactoryException, TransformException {
    CoordinateTransformer transformer = new CoordinateTransformer("EPSG:31467");
    double[] source = {3477534.683, 5605739.857};
    double[] destination = {8.681739535269804, 50.58691850210496};
    testTransformation(transformer, source, destination);
  }

  /**
   * Test transformation
   * @param transformer the transformer
   * @param source      the source coordinates
   * @param destination the expected destination coordinates in
   *                    {@link DefaultGeographicCRS#WGS84}
   */
  public void testTransformation(CoordinateTransformer transformer, double[] source,
                                 double[] destination)
    throws TransformException {
    double[] transformed = transformer.transform(source, 2);
    assertTrue(transformed[0] - destination[0] < 0.01);
    assertTrue(transformed[1] - destination[1] < 0.01);
  }
}
