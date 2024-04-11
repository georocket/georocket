package io.georocket.util

import io.georocket.util.WKTCRSDecoder.decode
import io.georocket.util.WKTCRSDecoder.isWKT
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.Assert
import org.junit.Test

/**
 * Test the [WKTCRSDecoder]
 * @author Tim Hellhake
 */
class WKTCRSDecoderTest {
  /**
   * Test if the WKT string is recognized and decoded properly
   * @throws Exception if the test fails
   */
  @Test
  @Throws(Exception::class)
  fun testWksString() {
    val wkt = DefaultGeographicCRS.WGS84.toWKT()
    Assert.assertEquals(true, isWKT(wkt))
    val c = decode(wkt)
    Assert.assertEquals(180, c.coordinateSystem.getAxis(0).maximumValue.toInt().toLong())
    Assert.assertEquals(90, c.coordinateSystem.getAxis(1).maximumValue.toInt().toLong())
  }
}
