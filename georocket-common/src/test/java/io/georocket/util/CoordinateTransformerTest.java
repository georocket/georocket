package io.georocket.util;

import static org.junit.Assert.assertEquals;

import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;

/**
 * Test the {@link CoordinateTransformer}
 * @author Tim Hellhake
 */
public class CoordinateTransformerTest {
  /**
   * Test transformation with EPSG code
   * @throws Exception if something has happened
   */
  @Test
  public void testEPSG() throws Exception {
    CoordinateTransformer transformer = new CoordinateTransformer("EPSG:31467");
    double[] source = {3477534.683, 5605739.857};
    double[] destination = {8.681739535269804, 50.58691850210496};
    testTransformation(transformer, source, destination);
  }

  /**
   * Test transformation
   * @param transformer the transformer
   * @param source the source coordinates
   * @param destination the expected destination coordinates in
   * {@link DefaultGeographicCRS#WGS84}
   * @throws Exception if something has happened
   */
  public void testTransformation(CoordinateTransformer transformer,
      double[] source, double[] destination) throws Exception {
    double[] transformed = transformer.transform(source, 2);
    assertEquals(destination[0], transformed[0], 0.00001);
    assertEquals(destination[1], transformed[1], 0.00001);
  }
}
