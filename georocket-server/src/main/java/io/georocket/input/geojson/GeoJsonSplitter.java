package io.georocket.input.geojson;

import de.undercouch.actson.JsonEvent;
import io.georocket.input.json.JsonSplitter;
import io.georocket.storage.GeoJsonChunkMeta;
import io.georocket.storage.JsonChunkMeta;
import io.georocket.util.JsonStreamEvent;
import io.georocket.util.StringWindow;

/**
 * A JSON splitter that is tailored to GeoJSON files
 * @author Michel Kraemer
 */
public class GeoJsonSplitter extends JsonSplitter {
  /**
   * True if we're currently parsing a 'type' field
   */
  private boolean parsingType = false;
  
  /**
   * The last 'type' parsed
   */
  private String lastType;
  
  /**
   * The level in which {@link #highestType} was found
   */
  private int highestTypeLevel = Integer.MAX_VALUE;
  
  /**
   * The value of the 'type' attribute found at the highest level of the
   * JSON file
   */
  private String highestType;
  
  /**
   * Create splitter
   * @param window a buffer for incoming data
   */
  public GeoJsonSplitter(StringWindow window) {
    super(window);
  }
  
  @Override
  public Result<JsonChunkMeta> onEvent(JsonStreamEvent event) {
    boolean prevResultsCreated = resultsCreated;
    
    Result<JsonChunkMeta> r = super.onEvent(event);
    
    switch (event.getEvent()) {
    case JsonEvent.FIELD_NAME:
      if (mark == -1 || markedLevel == inArray.size() + insideLevel - 1) {
        // we're inside a chunk. try to get its type
        parsingType = "type".equals(event.getCurrentValue());
      }
      break;
      
    case JsonEvent.VALUE_STRING:
      if (parsingType) {
        if (mark == -1 && inArray.size() < highestTypeLevel) {
          // we're not inside a chunk. store the type from the highest level.
          highestTypeLevel = inArray.size();
          highestType = event.getCurrentValue().toString();
        } else {
          // we're inside a chunk
          lastType = event.getCurrentValue().toString();
        }
      }
      break;
    
    case JsonEvent.EOF:
      // We reached the end of the file and no chunk has been created yet. We
      // should set 'lastType' to the type that we found on the highest level
      // in the file so that the chunk that we are going to create will get
      // this type.
      if (!prevResultsCreated) {
        lastType = highestType;
      }
      parsingType = false;
      break;
    
    default:
      parsingType = false;
      break;
    }
    
    if (r != null) {
      r = new Result<JsonChunkMeta>(r.getChunk(),
        new GeoJsonChunkMeta(lastType, r.getMeta()));
    }
    
    return r;
  }
  
  @Override
  protected Result<JsonChunkMeta> makeResult(int pos) {
    if (lastFieldName == null && ("FeatureCollection".equals(highestType) ||
        "GeometryCollection".equals(highestType))) {
      // ignore empty collections
      return null;
    }
    
    // ignore all chunks found in extra attributes not part of the standard
    if (lastFieldName == null || lastFieldName.equals("features") ||
        lastFieldName.equals("geometries")) {
      return super.makeResult(pos);
    }
    
    return null;
  }
}
