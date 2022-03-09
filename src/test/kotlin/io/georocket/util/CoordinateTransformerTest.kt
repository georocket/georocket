package io.georocket.util

import org.geotools.referencing.CRS
import org.junit.Assert
import org.junit.Test

/**
 * Test the [CoordinateTransformer]
 * @author Tim Hellhake
 */
class CoordinateTransformerTest {
  /**
   * Test transformation with EPSG code
   * @throws Exception if something has happened
   */
  @Test
  @Throws(Exception::class)
  fun testEPSG() {
    val crs = CRS.decode("EPSG:31467")
    val transformer = CoordinateTransformer(crs)
    val source = doubleArrayOf(3477534.683, 5605739.857)
    val destination = doubleArrayOf(8.681739535269804, 50.58691850210496)
    testTransformation(transformer, source, destination)
  }

  /**
   * Test transformation with WKT code
   * @throws Exception if something has happened
   */
  @Test
  @Throws(Exception::class)
  fun testWKT() {
    val crs = CRS.decode("EPSG:31467")
    val wktCrs = CoordinateTransformer.decode(crs.toWKT())
    val transformer = CoordinateTransformer(wktCrs)
    val source = doubleArrayOf(3477534.683, 5605739.857)
    val destination = doubleArrayOf(8.681739535269804, 50.58691850210496)
    testTransformation(transformer, source, destination)
  }

  /**
   * Test transformation
   * @param transformer the transformer
   * @param source the source coordinates
   * @param destination the expected destination coordinates in
   * [DefaultGeographicCRS.WGS84]
   * @throws Exception if something has happened
   */
  @Throws(Exception::class)
  fun testTransformation(
    transformer: CoordinateTransformer,
    source: DoubleArray?, destination: DoubleArray
  ) {
    val transformed = transformer.transform(source!!, 2)
    Assert.assertEquals(destination[0], transformed!![0], 0.00001)
    Assert.assertEquals(destination[1], transformed[1], 0.00001)
  }
}
