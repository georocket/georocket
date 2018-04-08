package io.georocket.output.geojson;

import io.georocket.output.Merger;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.GeoJsonChunkMeta;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import rx.Completable;

/**
 * Merges chunks to valid GeoJSON documents
 * @author Michel Kraemer
 */
public class GeoJsonMerger implements Merger<GeoJsonChunkMeta> {
  private static final int NOT_SPECIFIED = 0;
  private static final int GEOMETRY = 1;
  private static final int GEOMETRY_COLLECTION = 2;
  private static final int FEATURE = 3;
  private static final int FEATURE_COLLECTION = 4;
  
  // CHECKSTYLE:OFF
  private static final int[][] TRANSITIONS = {
    /*                          FEATURE            | GEOMETRY            */
    /* NOT_SPECIFIED       */ { FEATURE            , GEOMETRY            },
    /* GEOMETRY            */ { FEATURE_COLLECTION , GEOMETRY_COLLECTION },
    /* GEOMETRY_COLLECTION */ { FEATURE_COLLECTION , GEOMETRY_COLLECTION },
    /* FEATURE             */ { FEATURE_COLLECTION , FEATURE_COLLECTION  },
    /* FEATURE_COLLECTION  */ { FEATURE_COLLECTION , FEATURE_COLLECTION  }
  };
  // CHECKSTYLE:ON
  
  /**
   * {@code true} if {@link #merge(ChunkReadStream, GeoJsonChunkMeta, WriteStream)}
   * has been called at least once
   */
  private boolean mergeStarted = false;

  /**
   * True if the header has already been written in
   * {@link #merge(ChunkReadStream, GeoJsonChunkMeta, WriteStream)}
   */
  private boolean headerWritten = false;
  
  /**
   * The GeoJSON object type the merged result should have
   */
  private int mergedType = NOT_SPECIFIED;
  
  /**
   * Write the header
   * @param out the output stream to write to
   */
  private void writeHeader(WriteStream<Buffer> out) {
    if (mergedType == FEATURE_COLLECTION) {
      out.write(Buffer.buffer("{\"type\":\"FeatureCollection\",\"features\":["));
    } else if (mergedType == GEOMETRY_COLLECTION) {
      out.write(Buffer.buffer("{\"type\":\"GeometryCollection\",\"geometries\":["));
    }
  }

  @Override
  public Completable init(GeoJsonChunkMeta meta) {
    if (mergeStarted) {
      return Completable.error(new IllegalStateException("You cannot "
          + "initialize the merger anymore after merging has begun"));
    }
    
    if (mergedType == FEATURE_COLLECTION) {
      // shortcut: we don't need to analyse the other chunks anymore,
      // we already reached the most generic type
      return Completable.complete();
    }
    
    // calculate the type of the merged document
    if ("Feature".equals(meta.getType())) {
      mergedType = TRANSITIONS[mergedType][0];
    } else {
      mergedType = TRANSITIONS[mergedType][1];
    }
    
    return Completable.complete();
  }

  @Override
  public Completable merge(ChunkReadStream chunk, GeoJsonChunkMeta meta,
      WriteStream<Buffer> out) {
    mergeStarted = true;
    
    if (!headerWritten) {
      writeHeader(out);
      headerWritten = true;
    } else {
      if (mergedType == FEATURE_COLLECTION || mergedType == GEOMETRY_COLLECTION) {
        out.write(Buffer.buffer(","));
      } else {
        return Completable.error(new IllegalStateException(
          "Trying to merge two or more chunks but the merger has only been "
          + "initialized with one chunk."));
      }
    }
    
    // check if we have to wrap a geometry into a feature
    boolean wrap = mergedType == FEATURE_COLLECTION && !"Feature".equals(meta.getType());
    if (wrap) {
      out.write(Buffer.buffer("{\"type\":\"Feature\",\"geometry\":"));
    }
    
    return writeChunk(chunk, meta, out)
      .doOnCompleted(() -> {
        if (wrap) {
          out.write(Buffer.buffer("}"));
        }
      });
  }

  @Override
  public void finish(WriteStream<Buffer> out) {
    if (mergedType == FEATURE_COLLECTION || mergedType == GEOMETRY_COLLECTION) {
      out.write(Buffer.buffer("]}"));
    }
  }
}
