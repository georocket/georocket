package io.georocket.input.json

import de.undercouch.actson.JsonEvent
import io.georocket.input.Splitter
import io.georocket.storage.JsonChunkMeta
import io.georocket.util.JsonStreamEvent
import io.georocket.util.StringWindow
import io.vertx.core.buffer.Buffer
import java.util.*

/**
 * Split incoming JSON tokens whenever an object is encountered that is inside
 * an array. Return the whole input stream as one chunk if there is no array
 * containing objects inside the stream (i.e. if the end of the stream has been
 * reached and no chunks were found).
 * @author Michel Kraemer
 */
open class JsonSplitter(
  /**
   * A buffer for incoming data
   */
  private val window: StringWindow
) : Splitter<JsonStreamEvent, JsonChunkMeta> {
  /**
   * Saves whether the parser is currently inside an array or not.
   */
  protected var inArray: Deque<Boolean> = ArrayDeque()

  /**
   * Saves the name of the last field encountered
   */
  protected var lastFieldName: String? = null

  /**
   * A marked position in the input stream
   */
  protected var mark = -1L

  /**
   * The size of [.inArray] when the [.mark] was set. This is
   * used to decide if a chunk has to be created and the mark should be unset.
   */
  protected var markedLevel = -1

  /**
   * Will be increased whenever the parser is currently inside an array. Helps
   * save space because we don't have to push to [.inArray] anymore.
   */
  protected var insideLevel = 0

  /**
   * `true` if the [.makeResult] method was called
   * at least once
   */
  protected var resultsCreated = false

  /**
   * Create splitter
   */
  init {
    inArray.push(false)
  }

  override fun onEvent(event: JsonStreamEvent): Splitter.Result<JsonChunkMeta>? {
    when (event.event) {
      JsonEvent.START_OBJECT -> {
        if (mark == -1L && inArray.peek() == true) {
          // object start was one character before the current event
          mark = event.pos - 1
          markedLevel = inArray.size
        }
        if (mark == -1L) {
          inArray.push(java.lang.Boolean.FALSE)
        } else {
          ++insideLevel
        }
      }
      JsonEvent.FIELD_NAME ->       // Don't save the last field name if we are currently parsing a chunk. We
        // need to keep the field name until the chunk has been parsed completely
        // so #makeResult() can use it to generate the JsonChunkMeta object.
        if (mark == -1L) {
          lastFieldName = event.currentValue.toString()
        }
      JsonEvent.START_ARRAY -> if (mark == -1L) {
        inArray.push(java.lang.Boolean.TRUE)
      } else {
        ++insideLevel
      }
      JsonEvent.END_OBJECT -> {
        if (mark == -1L) {
          inArray.pop()
        } else {
          --insideLevel
        }
        if (mark != -1L && markedLevel == inArray.size + insideLevel) {
          val r = makeResult(event.pos)
          mark = -1
          markedLevel = -1
          return r
        }
      }
      JsonEvent.END_ARRAY -> if (mark == -1L) {
        inArray.pop()
      } else {
        --insideLevel
      }
      JsonEvent.EOF -> if (!resultsCreated) {
        // we haven't found any chunk so far. return the whole input stream
        // as one chunk
        mark = 0
        lastFieldName = null
        val r = makeResult(event.pos)
        mark = -1
        return r
      }
      else -> {}
    }
    return null
  }

  /**
   * Make chunk and meta data
   * @param pos the position of the end of the chunk to create
   * @return the chunk and meta data
   */
  protected open fun makeResult(pos: Long): Splitter.Result<JsonChunkMeta>? {
    resultsCreated = true
    val str = window.getChars(mark, pos)
    window.advanceTo(pos)
    val buf = Buffer.buffer(str)
    val meta = JsonChunkMeta(lastFieldName)
    return Splitter.Result(buf, null, null, meta)
  }
}
