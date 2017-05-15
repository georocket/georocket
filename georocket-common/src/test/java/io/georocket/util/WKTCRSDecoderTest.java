package io.georocket.util;

import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import static org.junit.Assert.assertEquals;

/**
 * Test the {@link WKTCRSDecoder}
 * @author Tim Hellhake
 */
public class WKTCRSDecoderTest {
  /**
   * Test if the WKT string is recognized and decoded properly
   * @throws Exception if the test fails
   */
  @Test
  public void testWksString() throws Exception {
    String wkt = DefaultGeographicCRS.WGS84.toWKT();
    assertEquals(true, WKTCRSDecoder.isWKT(wkt));
    CoordinateReferenceSystem c = WKTCRSDecoder.decode(wkt);
    assertEquals(180, (int)c.getCoordinateSystem().getAxis(0).getMaximumValue());
    assertEquals(90, (int)c.getCoordinateSystem().getAxis(1).getMaximumValue());
  }
}
