package io.georocket.util;

import org.geotools.referencing.CRS;
import org.junit.Test;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import static org.junit.Assert.assertEquals;

/**
 * Test the {@link CompoundCRSDecoder}
 * @author Michel Kraemer
 */
public class CompoundCRSDecoderTest {
  /**
   * A simple test
   * @throws Exception if the test fails
   */
  @Test
  public void compoundCrs() throws Exception {
    CoordinateReferenceSystem c = CompoundCRSDecoder.decode("urn:ogc:def:crs,crs:EPSG:6.12:3068,crs:EPSG:6.12:5783");
    assertEquals(CRS.decode("EPSG:3068"), CRS.getHorizontalCRS(c));
    assertEquals(CRS.decode("EPSG:5783"), CRS.getVerticalCRS(c));
  }
}
