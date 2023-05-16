package io.georocket.util

import io.georocket.util.CompoundCRSDecoder.decode
import kotlin.Throws
import org.geotools.referencing.CRS
import org.junit.Assert
import org.junit.Test
import java.lang.Exception

/**
 * Test the [CompoundCRSDecoder]
 * @author Michel Kraemer
 */
class CompoundCRSDecoderTest {
    /**
     * A simple test
     * @throws Exception if the test fails
     */
    @Test
    @Throws(Exception::class)
    fun compoundCrs() {
        val c = decode("urn:ogc:def:crs,crs:EPSG:6.12:3068,crs:EPSG:6.12:5783")
        Assert.assertEquals(CRS.decode("EPSG:3068"), CRS.getHorizontalCRS(c))
        Assert.assertEquals(CRS.decode("EPSG:5783"), CRS.getVerticalCRS(c))
    }
}
