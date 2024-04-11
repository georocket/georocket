package io.georocket.util

import io.vertx.core.buffer.Buffer
import java.nio.CharBuffer
import java.lang.StringBuilder
import java.lang.IllegalStateException
import java.nio.ByteBuffer
import java.nio.charset.*

/**
 * A dynamically resizable buffer that acts like a window being moved over
 * a larger input stream
 * @author Michel Kraemer
 */
class StringWindow {

  companion object {

    /**
     * Default size for [.charBuf]
     */
    private const val DEFAULT_CHAR_BUFFER_SIZE = 2048

  }

  /**
   * Decodes incoming byte buffers into strings
   */
  private val decoder = StandardCharsets.UTF_8.newDecoder()

  /**
   * A temporary character buffer used during string decoding
   */
  private var charBuf = CharBuffer.allocate(DEFAULT_CHAR_BUFFER_SIZE)

  /**
   * A temporary buffer holding bytes that still need to be decoded
   */
  private var buf = Buffer.buffer()

  /**
   * A buffer holding the decoded string
   */
  private val decodedBuf = StringBuilder()

  /**
   * The current position in the window (i.e. in the decoded string)
   */
  private var pos: Long = 0

  /**
   * Ensure the character buffer is large enough to hold a given number of
   * decoded characters
   * @param length the length of the encoded byte buffer
   */
  private fun ensureCharBuffer(length: Int) {
    val maxLength = (length.toDouble() * decoder.maxCharsPerByte()).toInt()
    if (maxLength > charBuf.length) {
      charBuf = CharBuffer.allocate(maxLength)
    }
  }

  /**
   * Append data to the window (i.e. make it larger)
   * @param buf the data to append
   */
  fun append(buf: Buffer) {
    // append new bytes to buffered bytes or use them directly
    if (this.buf.length() > 0) {
      this.buf.appendBuffer(buf)
    } else {
      this.buf = buf
    }

    // convert Vert.x buffer to ByteBuffer (ugly!)
    val byteBuf = ByteBuffer.wrap(this.buf.bytes)

    // prepare temporary CharBuffer
    ensureCharBuffer(buf.length())
    charBuf.position(0)
    charBuf.limit(charBuf.capacity())

    // decode ByteBuffer to temporary CharBuffer
    val result = decoder.decode(byteBuf, charBuf, false)
    if (result.isMalformed) {
      throw IllegalStateException(
        MalformedInputException(result.length())
      )
    }
    if (result.isUnmappable) {
      throw IllegalStateException(
        UnmappableCharacterException(result.length())
      )
    }

    // reset CharBuffer and remove decoded bytes from byte buffer
    charBuf.flip()
    this.buf = this.buf.getBuffer(byteBuf.position(), this.buf.length())

    // append to decoded string buffer
    decodedBuf.append(charBuf)
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
  fun getChars(startCharacter: Long, endCharacter: Long): String {
    return decodedBuf.substring((startCharacter - pos).toInt(), (endCharacter - pos).toInt())
  }

  /**
   * Remove characters from the beginning of the window (i.e. make it smaller)
   * @param pos the number of characters to remove (or in other words: the number
   * of characters to advance the window forward without changing its end)
   */
  fun advanceTo(pos: Long) {
    decodedBuf.delete(0, (pos - this.pos).toInt())
    this.pos = pos
  }
}
