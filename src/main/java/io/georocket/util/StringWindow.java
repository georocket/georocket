package io.georocket.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnmappableCharacterException;

import io.vertx.core.buffer.Buffer;

/**
 * A dynamically resizable buffer that acts like a window being moved over
 * a larger input stream
 * @author Michel Kraemer
 */
public class StringWindow {
  /**
   * Default size for {@link #charBuf}
   */
  private final int DEFAULT_CHAR_BUFFER_SIZE = 2048;

  /**
   * Decodes incoming byte buffers into strings
   */
  private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

  /**
   * A temporary character buffer used during string decoding
   */
  private CharBuffer charBuf = CharBuffer.allocate(DEFAULT_CHAR_BUFFER_SIZE);

  /**
   * A temporary buffer holding bytes that still need to be decoded
   */
  private Buffer buf = Buffer.buffer();

  /**
   * A buffer holding the decoded string
   */
  private final StringBuilder decodedBuf = new StringBuilder();

  /**
   * The current position in the window (i.e. in the decoded string)
   */
  private long pos = 0;

  /**
   * Ensure the character buffer is large enough to hold a given number of
   * decoded characters
   * @param length the length of the encoded byte buffer
   */
  private void ensureCharBuffer(int length) {
    int maxLength = (int)((double)length * decoder.maxCharsPerByte());
    if (maxLength > charBuf.length()) {
      charBuf = CharBuffer.allocate(maxLength);
    }
  }

  /**
   * Append data to the window (i.e. make it larger)
   * @param buf the data to append
   */
  public void append(Buffer buf) {
    // append new bytes to buffered bytes or use them directly
    if (this.buf.length() > 0) {
      this.buf.appendBuffer(buf);
    } else {
      this.buf = buf;
    }

    // convert Vert.x buffer to ByteBuffer (ugly!)
    ByteBuffer byteBuf = ByteBuffer.wrap(this.buf.getBytes());

    // prepare temporary CharBuffer
    ensureCharBuffer(buf.length());
    charBuf.position(0);
    charBuf.limit(charBuf.capacity());

    // decode ByteBuffer to temporary CharBuffer
    CoderResult result = decoder.decode(byteBuf, charBuf, false);
    if (result.isMalformed()) {
      throw new IllegalStateException(
        new MalformedInputException(result.length()));
    }
    if (result.isUnmappable()) {
      throw new IllegalStateException(
        new UnmappableCharacterException(result.length()));
    }

    // reset CharBuffer and remove decoded bytes from byte buffer
    charBuf.flip();
    this.buf = this.buf.getBuffer(byteBuf.position(), this.buf.length());

    // append to decoded string buffer
    this.decodedBuf.append(charBuf);
  }
  
  /**
   * Return a chunk from the window
   * @param startCharacter the start position of the chunk (in characters and
   * not bytes). This value is absolute to the position in the larger input
   * stream the window is being moved over.
   * @param endCharacter the end position of the chunk (in characters and not
   * bytes). This value is absolute to the position in the larger input stream
   * the window is being moved over.
   * @return the chunk
   */
  public String getChars(long startCharacter, long endCharacter) {
    return decodedBuf.substring((int)(startCharacter - pos), (int)(endCharacter - pos));
  }
  
  /**
   * Remove characters from the beginning of the window (i.e. make it smaller)
   * @param pos the number of characters to remove (or in other words: the number
   * of characters to advance the window forward without changing its end)
   */
  public void advanceTo(long pos) {
    decodedBuf.delete(0, (int)(pos - this.pos));
    this.pos = pos;
  }
}
