package io.georocket.util

import io.vertx.core.buffer.Buffer

/**
 * Removes a BOM (byte order marker) from a buffer
 * @author Michel Kraemer
 */
class UTF8BomFilter {
  /**
   * `true` if the filter has already checked for a BOM
   */
  private var bomChecked = false

  /**
   * Check the given buffer for a BOM. If this is the first buffer checked and
   * if it contains a BOM, the method will return a copy of the buffer without
   * the BOM. The method is a no-op for all subsequent buffers.
   * @param buf the buffer to check
   * @return a copy of `buf` without the BOM or `buf` if
   * it did not contain a BOM or if it was not the first buffer checked.
   */
  fun filter(buf: Buffer): Buffer {
    if (bomChecked) {
      return buf
    }
    bomChecked = true
    var i = 0
    while (i < UTF8_BOM_BYTES.size && i < buf.length()) {
      if (UTF8_BOM_BYTES[i] != buf.getByte(i)) {
        // we did not find a BOM
        return buf
      }
      ++i
    }

    // we found a BOM - truncate it
    return buf.getBuffer(UTF8_BOM_BYTES.size, buf.length())
  }

  companion object {
    private val UTF8_BOM_BYTES = byteArrayOf(0xEF.toByte(), 0xBB.toByte(), 0xBF.toByte())
  }
}
