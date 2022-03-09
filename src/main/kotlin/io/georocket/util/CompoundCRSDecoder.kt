package io.georocket.util

import org.geotools.referencing.CRS
import java.util.Locale
import org.opengis.referencing.FactoryException
import org.opengis.referencing.NoSuchAuthorityCodeException
import org.geotools.referencing.crs.DefaultCompoundCRS
import org.opengis.referencing.crs.CoordinateReferenceSystem

/**
 * Decode a OGC URN for a compound CRS. See OGC 07-092r3, section 7.5.4
 * @author Michel Kraemer
 */
object CompoundCRSDecoder {
    private const val AUTHORITY = "urn:ogc:def"
    private const val PREFIX = "$AUTHORITY:crs,"

    /**
     * Check if the given code represents a compound CRS
     * @param code the code
     * @return true if the code represents a compound CRS, false otherwise
     */
    fun isCompound(code: String): Boolean {
        return code.trim { it <= ' ' }.lowercase(Locale.getDefault()).startsWith(PREFIX)
    }

    /**
     * Decodes a given code to a [org.opengis.referencing.crs.CompoundCRS]
     * @param code the code
     * @return the compound CRS
     * @throws FactoryException if the code does not represent a compound CRS
     * or if one of its components could not be decoded
     */
    fun decode(code: String): CoordinateReferenceSystem {
        var trimmed = code
        trimmed = trimmed.trim { it <= ' ' }
        if (!isCompound(trimmed)) {
            throw NoSuchAuthorityCodeException("No compound CRS", AUTHORITY, trimmed)
        }
        trimmed = trimmed.substring(PREFIX.length)
        val parts = trimmed.split(",".toRegex()).toTypedArray()
        val crss = arrayOfNulls<CoordinateReferenceSystem>(parts.size)
        for (i in crss.indices) {
            crss[i] = CRS.decode(AUTHORITY + ":" + parts[i].trim { it <= ' ' })
        }
        return DefaultCompoundCRS("", *crss)
    }
}
