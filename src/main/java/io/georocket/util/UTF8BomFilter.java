package io.georocket.util;

import io.vertx.core.buffer.Buffer;

/**
 * Removes a BOM (byte order marker) from a buffer
 * @author Michel Kraemer
 */
public class UTF8BomFilter {
  private static final byte[] UTF8_BOM_BYTES = new byte[] { (byte)0xEF, (byte)0xBB, (byte)0xBF };
  
  /**
   * <code>true</code> if the filter has already checked for a BOM
   */
  private boolean bomChecked = false;
  
  /**
   * Check the given buffer for a BOM. If this is the first buffer checked and
   * if it contains a BOM, the method will return a copy of the buffer without
   * the BOM. The method is a no-op for all subsequent buffers.
   * @param buf the buffer to check
   * @return a copy of <code>buf</code> without the BOM or <code>buf</code> if
   * it did not contain a BOM or if it was not the first buffer checked.
   */
  public Buffer filter(Buffer buf) {
    if (bomChecked) {
      return buf;
    }
    bomChecked = true;

    for (int i = 0; i < UTF8_BOM_BYTES.length && i < buf.length(); ++i) {
      if (UTF8_BOM_BYTES[i] != buf.getByte(i)) {
        // we did not find a BOM
        return buf;
      }
    }

    // we found a BOM - truncate it
    return buf.getBuffer(UTF8_BOM_BYTES.length, buf.length());
  }
}
