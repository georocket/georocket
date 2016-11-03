package io.georocket.util;

/**
 * A dynamically resizable buffer that acts like a window being moved over
 * a larger input stream
 * @author Michel Kraemer
 */
public class StringWindow {
  private StringBuilder buf = new StringBuilder();
  private int pos = 0;
  
  /**
   * Append data to the window (i.e. make it larger)
   * @param str the data to append
   */
  public void append(String str) {
    this.buf.append(str);
  }
  
  /**
   * Return a chunk from the window
   * @param start the start position of the chunk. This value is absolute to
   * the position in the larger input stream the window is being moved over.
   * @param end the end position of the chunk. This value is absolute to
   * the position in the larger input stream the window is being moved over.
   * @return the chunk
   */
  public String getChars(int start, int end) {
    return buf.substring(start - pos, end - pos);
  }
  
  /**
   * Remove bytes from the beginning of the window (i.e. make it smaller)
   * @param pos the number of bytes to remove (or in other words: the number
   * of bytes to advance the window forward without changing its end)
   */
  public void advanceTo(int pos) {
    buf = buf.delete(0, pos - this.pos);
    this.pos = pos;
  }
}
