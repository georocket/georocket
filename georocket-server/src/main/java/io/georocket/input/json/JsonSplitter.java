package io.georocket.input.json;

import java.util.ArrayDeque;
import java.util.Deque;

import de.undercouch.actson.JsonEvent;
import io.georocket.input.Splitter;
import io.georocket.storage.ChunkMeta;
import io.georocket.util.JsonStreamEvent;
import io.georocket.util.StringWindow;

/**
 * Split incoming JSON tokens whenever an object is encountered that is inside
 * an array. Return the whole input stream as one chunk if there is no array
 * containing objects inside the stream (i.e. if the end of the stream has been
 * reached and no chunks were found).
 * @author Michel Kraemer
 */
public class JsonSplitter implements Splitter<JsonStreamEvent, ChunkMeta> {
  /**
   * A buffer for incoming data
   */
  private final StringWindow window;
  
  /**
   * Saves whether the parser is currently inside an array or not.
   */
  private Deque<Boolean> inArray = new ArrayDeque<>();
  
  /**
   * A marked position in the input stream
   */
  private int mark = -1;
  
  /**
   * The size of {@link #inArray} when the {@link #mark} was set. This is
   * used to decide if a chunk has to be created and the mark should be unset.
   */
  private int markedLevel = -1;
  
  /**
   * Will be increased whenever the parser is currently inside an array. Helps
   * save space because we don't have to push to {@link #inArray} anymore.
   */
  private int insideLevel = 0;
  
  /**
   * <code>true</code> if the {@link #makeResult(int)} method was called
   * at least once
   */
  private boolean resultsCreated = false;
  
  /**
   * Create splitter
   * @param window a buffer for incoming data
   */
  public JsonSplitter(StringWindow window) {
    this.window = window;
    inArray.push(Boolean.FALSE);
  }
  
  @Override
  public Result<ChunkMeta> onEvent(JsonStreamEvent event) {
    switch (event.getEvent()) {
    case JsonEvent.START_OBJECT:
      if (mark == -1 && inArray.peek() == Boolean.TRUE) {
        // object start was one character before the current event
        mark = event.getPos() - 1;
        markedLevel = inArray.size();
      }
      if (mark == -1) {
        inArray.push(Boolean.FALSE);
      } else {
        ++insideLevel;
      }
      break;
    
    case JsonEvent.START_ARRAY:
      if (mark == -1) {
        inArray.push(Boolean.TRUE);
      } else {
        ++insideLevel;
      }
      break;
    
    case JsonEvent.END_OBJECT:
      if (mark == -1) {
        inArray.pop();
      } else {
        --insideLevel;
      }
      if (mark != -1 && markedLevel == inArray.size() + insideLevel) {
        Result<ChunkMeta> r = makeResult(event.getPos());
        mark = -1;
        markedLevel = -1;
        return r;
      }
      break;
    
    case JsonEvent.END_ARRAY:
      if (mark == -1) {
        inArray.pop();
      } else {
        --insideLevel;
      }
      break;
    
    case JsonEvent.EOF:
      if (!resultsCreated) {
        // we haven't found any chunk so far. return the whole input stream
        // as one chunk
        mark = 0;
        Result<ChunkMeta> r = makeResult(event.getPos());
        mark = -1;
        return r;
      }
      break;
    
    default:
      break;
    }
    return null;
  }

  /**
   * Make chunk and meta data
   * @param pos the position of the end of the chunk to create
   * @return the chunk and meta data
   */
  private Result<ChunkMeta> makeResult(int pos) {
    resultsCreated = true;
    String chunk = window.getChars(mark, pos);
    window.advanceTo(pos);
    ChunkMeta meta = new ChunkMeta(0, chunk.length());
    return new Result<ChunkMeta>(chunk, meta);
  }
}
