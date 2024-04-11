package io.georocket.util

import io.vertx.core.buffer.Buffer

/**
 * A dynamically resizable buffer that acts like a window being moved over
 * a larger input stream
 * @author Michel Kraemer
 */
class Window {
  private var buf = Buffer.buffer()
  private var pos: Long = 0

  /**
   * Append data to the window (i.e. make it larger)
   * @param buf the data to append
   */
  fun append(buf: Buffer) {
    this.buf.appendBuffer(buf)
  }

  /**
   * Return a chunk from the window
   * @param start the start position of the chunk. This value is absolute to
   * the position in the larger input stream the window is being moved over.
   * @param end the end position of the chunk. This value is absolute to
   * the position in the larger input stream the window is being moved over.
   * @return the chunk
   */
  fun getBytes(start: Long, end: Long): ByteArray {
    return buf.getBytes((start - pos).toInt(), (end - pos).toInt())
  }

  /**
   * Remove bytes from the beginning of the window (i.e. make it smaller)
   * @param pos the number of bytes to remove (or in other words: the number
   * of bytes to advance the window forward without changing its end)
   */
  fun advanceTo(pos: Long) {
    buf = buf.getBuffer((pos - this.pos).toInt(), buf.length())
    this.pos = pos
  }
}
