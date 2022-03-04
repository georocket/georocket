package io.georocket.util

import java.util.Locale
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import kotlin.math.log10
import kotlin.math.pow

/**
 * Convert data sizes to human-readable strings. Used by all commands that
 * output sizes, so the output always looks the same.
 *
 * Code has been adapted from
 * [https://stackoverflow.com/questions/3263892/format-file-size-as-mb-gb-etc](https://stackoverflow.com/questions/3263892/format-file-size-as-mb-gb-etc)
 * and corrected in a way that it displays correct SI units.
 * @author Michel Kraemer
 */
object SizeFormat {
    private val UNITS = arrayOf(
        "B", "kB", "MB", "GB", "TB", "PB", "EB"
    )
    private val FORMATTER = DecimalFormat(
        "#,##0.#",
        DecimalFormatSymbols.getInstance(Locale.ENGLISH)
    )

    /**
     * Convert the given data size to a human-readable string
     * @param size the data size
     * @return the human-readable string
     */
    fun format(size: Long): String {
        if (size <= 0) {
            return "0 B"
        }
        val d = (log10(size.toDouble()) / 3).toInt()
        return FORMATTER.format(size / 1000.0.pow(d.toDouble())) + " " + UNITS[d]
    }
}
