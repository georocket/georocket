package io.georocket.util;

import static org.junit.Assert.assertEquals;

import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

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
    CoordinateReferenceSystem crs = CRS.decode("EPSG:31467");
    CoordinateTransformer transformer = new CoordinateTransformer(crs);
    double[] source = {3477534.683, 5605739.857};
    double[] destination = {8.681739535269804, 50.58691850210496};
    testTransformation(transformer, source, destination);
  }

  /**
   * Test transformation with WKT code
   */
  @Test
  public void testWKT() throws FactoryException, TransformException {
    CoordinateReferenceSystem crs = CRS.decode("EPSG:31467");
    CoordinateReferenceSystem wktCrs = CoordinateTransformer.decode(crs.toWKT());
    CoordinateTransformer transformer = new CoordinateTransformer(wktCrs);
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
      double[] source, double[] destination) throws TransformException {
    double[] transformed = transformer.transform(source, 2);
    assertEquals(destination[0], transformed[0], 0.00001);
    assertEquals(destination[1], transformed[1], 0.00001);
  }
}
