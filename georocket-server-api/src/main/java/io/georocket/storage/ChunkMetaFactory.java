package io.georocket.storage;

import java.lang.reflect.Constructor;
import io.vertx.core.json.JsonObject;

/**
 * This factory is used to create a ChunkMeta object determined by the given
 * class name.
 * 
 * @author Yasmina Kammeyer
 *
 */
public class ChunkMetaFactory {

  /**
   * Creates a ChunkMeta object based on the given type.
   * @param metaType The class name of the ChunkMeta to create e.g., {@link io.georocket.storage.ChunkMeta}
   * @param obj The values used to initialize the ChunkMeta with.
   * @return The ChunkMeta object, default is {@link io.georocket.storage.ChunkMeta}, or null if Object can not be created.
   * @throws Exception The exception that is thrown in case the ChunkMeta could not be created.
   */
  @SuppressWarnings("unchecked")
  public static ChunkMeta createChunkMetaFromJson(String metaType, JsonObject obj) throws Exception {
    ChunkMeta meta = null;
    if (metaType != null) {
      Constructor<ChunkMeta> constrc = (Constructor<ChunkMeta>)Class.forName(metaType)
          .getConstructor(JsonObject.class);
      meta = constrc.newInstance(obj);
    } else {
      meta = new ChunkMeta(obj);
    }
    
    return meta;
  }
}
