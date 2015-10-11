package de.fhg.igd.georocket.util;

import io.vertx.core.buffer.Buffer;

/**
 * A dynamically resizable buffer that acts like a window being moved over
 * a larger input stream
 * @author Michel Kraemer
 */
public class Window {
  private Buffer buf = Buffer.buffer();
  private int pos = 0;
  
  /**
   * Append data to the window (i.e. make it larger)
   * @param buf the data to append
   */
  public void append(Buffer buf) {
    this.buf.appendBuffer(buf);
  }
  
  /**
   * Return a chunk from the window
   * @param start the start position of the chunk. This value is absolute to
   * the position in the larger input stream the window is being moved over.
   * @param end the end position of the chunk. This value is absolute to
   * the position in the larger input stream the window is being moved over.
   * @return the chunk
   */
  public byte[] getBytes(int start, int end) {
    return buf.getBytes(start - pos, end - pos);
  }
  
  /**
   * Remove bytes from the beginning of the window (i.e. make it smaller)
   * @param pos the number of bytes to remove (or in other words: the number
   * of bytes to advance the window forward without changing its end)
   */
  public void advanceTo(int pos) {
    buf = buf.getBuffer(pos - this.pos, buf.length());
    this.pos = pos;
  }
}
