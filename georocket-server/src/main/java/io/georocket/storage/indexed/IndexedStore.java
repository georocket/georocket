package io.georocket.storage.indexed;

import io.georocket.constants.AddressConstants;
import io.georocket.index.IndexableChunkCache;
import io.georocket.storage.AsyncCursor;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.DeleteMeta;
import io.georocket.storage.IndexMeta;
import io.georocket.storage.Store;
import io.georocket.storage.StoreCursor;
import io.georocket.tasks.PurgingTask;
import io.georocket.tasks.TaskError;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * An abstract base class for chunk stores that are backed by an indexer
 * @author Michel Kraemer
 */
public abstract class IndexedStore implements Store {
  private static final AtomicInteger COUNTER =
    new AtomicInteger(new SecureRandom().nextInt());

  private final Vertx vertx;
  
  /**
   * Constructs the chunk store
   * @param vertx the Vert.x instance
   */
  public IndexedStore(Vertx vertx) {
    this.vertx = vertx;
  }
  
  @Override
  public void add(String chunk, ChunkMeta chunkMeta, String path,
      IndexMeta indexMeta, Handler<AsyncResult<Void>> handler) {
    doAddChunk(chunk, path, indexMeta.getCorrelationId(), ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        // start indexing
        JsonObject indexMsg = new JsonObject()
            .put("path", ar.result())
            .put("meta", chunkMeta.toJsonObject());

        if (indexMeta.getCorrelationId() != null) {
          indexMsg.put("correlationId", indexMeta.getCorrelationId());
        }
        if (indexMeta.getFilename() != null) {
          indexMsg.put("filename", indexMeta.getFilename());
        }
        indexMsg.put("timestamp", indexMeta.getTimestamp());
        if (indexMeta.getTags() != null) {
          indexMsg.put("tags", new JsonArray(indexMeta.getTags()));
        }
        if (indexMeta.getFallbackCRSString() != null) {
          indexMsg.put("fallbackCRSString", indexMeta.getFallbackCRSString());
        }
        if (indexMeta.getProperties() != null) {
          indexMsg.put("properties", new JsonObject(indexMeta.getProperties()));
        }

        // save chunk to cache and then let indexer know about it
        IndexableChunkCache.getInstance().put(ar.result(), Buffer.buffer(chunk));
        vertx.eventBus().send(AddressConstants.INDEXER_ADD, indexMsg);
        
        // tell sender that writing was successful
        handler.handle(Future.succeededFuture());
      }
    });
  }

  @Override
  public void delete(String search, String path, Handler<AsyncResult<Void>> handler) {
    delete(search, path, null, handler);
  }

  /**
   * Send a message to the task verticle telling it that we are now starting
   * to delete chunks from the store
   * @param correlationId the correlation ID of the purging task
   * @param totalChunks the total number of chunks to purge
   */
  private void startPurgingTask(String correlationId, int totalChunks) {
    PurgingTask purgingTask = new PurgingTask(correlationId);
    purgingTask.setStartTime(Instant.now());
    purgingTask.setTotalChunks(totalChunks);
    vertx.eventBus().publish(AddressConstants.TASK_INC,
        JsonObject.mapFrom(purgingTask));
  }

  /**
   * Send a message to the task verticle telling it that we have finished
   * deleting chunks from the store
   * @param correlationId the correlation ID of the purging task
   * @param error an error that occurred during the task execution (may be
   * {@code null} if everything is OK
   */
  private void stopPurgingTask(String correlationId, TaskError error) {
    PurgingTask purgingTask = new PurgingTask(correlationId);
    purgingTask.setEndTime(Instant.now());
    if (error != null) {
      purgingTask.addError(error);
    }
    vertx.eventBus().publish(AddressConstants.TASK_INC,
        JsonObject.mapFrom(purgingTask));
  }

  /**
   * Send a message to the task verticle telling it that we just deleted
   * the given number of chunks from the store
   * @param correlationId the correlation ID of the purging task
   */
  private void updatePurgingTask(String correlationId, int purgedChunks) {
    PurgingTask purgingTask = new PurgingTask(correlationId);
    purgingTask.setPurgedChunks(purgedChunks);
    vertx.eventBus().publish(AddressConstants.TASK_INC,
        JsonObject.mapFrom(purgingTask));
  }

  @Override
  public void delete(String search, String path, DeleteMeta deleteMeta,
      Handler<AsyncResult<Void>> handler) {
    String correlationId = deleteMeta != null ? deleteMeta.getCorrelationId() : null;
    if (correlationId != null) {
      startPurgingTask(correlationId, 0);
    }

    get(search, path, ar -> {
      if (ar.failed()) {
        Throwable cause = ar.cause();
        if (cause instanceof ReplyException) {
          ReplyException ex = (ReplyException)cause;
          if (ex.failureCode() == 404) {
            stopPurgingTask(correlationId, null);
            handler.handle(Future.succeededFuture());
            return;
          }
        }
        stopPurgingTask(correlationId, new TaskError(ar.cause()));
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        StoreCursor cursor = ar.result();
        AtomicLong remaining = new AtomicLong(cursor.getInfo().getTotalHits());
        if (correlationId != null) {
          startPurgingTask(correlationId, remaining.intValue());
        }

        Queue<String> paths = new ArrayDeque<>();
        doDelete(cursor, paths, remaining, correlationId, ddar -> {
          if (correlationId != null) {
            if (ddar.failed()) {
              stopPurgingTask(correlationId, new TaskError(ddar.cause()));
            } else {
              stopPurgingTask(correlationId, null);
            }
          }
          handler.handle(ddar);
        });
      }
    });
  }

  @Override
  public void get(String search, String path, Handler<AsyncResult<StoreCursor>> handler) {
    new IndexedStoreCursor(vertx, search, path).start(handler);
  }

  @Override
  public void scroll(String search, String path, int size, Handler<AsyncResult<StoreCursor>> handler) {
    new FrameCursor(vertx, search, path, size).start(handler);
  }

  @Override
  public void scroll(String scrollId, Handler<AsyncResult<StoreCursor>> handler) {
    new FrameCursor(vertx, scrollId).start(handler);
  }

  /**
   * Iterate over a cursor and delete all returned chunks from the index
   * and from the store.
   * @param cursor the cursor to iterate over
   * @param paths an empty queue (used internally for recursion)
   * @param remainingChunks holds the remaining number of chunks to delete
   * (used internally for recursion)
   * @param correlationId the correlation ID of the current purging task
   * @param handler will be called when all chunks have been deleted
   */
  private void doDelete(StoreCursor cursor, Queue<String> paths,
      AtomicLong remainingChunks, String correlationId,
      Handler<AsyncResult<Void>> handler) {
    // handle response of bulk delete operation
    Function<Integer, Handler<AsyncResult<Void>>> handleBulk = size -> bulkAr -> {
      remainingChunks.getAndAdd(-size);
      if (correlationId != null) {
        updatePurgingTask(correlationId, size);
      }
      if (bulkAr.failed()) {
        handler.handle(Future.failedFuture(bulkAr.cause()));
      } else {
        doDelete(cursor, paths, remainingChunks, correlationId, handler);
      }
    };
    
    // while cursor has items ...
    if (cursor.hasNext()) {
      cursor.next(ar -> {
        if (ar.failed()) {
          handler.handle(Future.failedFuture(ar.cause()));
        } else {
          // add item to queue
          paths.add(cursor.getChunkPath());
          int size = cursor.getInfo().getCurrentHits();

          if (paths.size() >= size) {
            // if there are enough items in the queue, bulk delete them
            doDeleteBulk(paths, cursor.getInfo().getTotalHits(),
                remainingChunks.get(), correlationId, handleBulk.apply(size));
          } else {
            // otherwise proceed with next item from cursor
            doDelete(cursor, paths, remainingChunks, correlationId, handler);
          }
        }
      });
    } else {
      // cursor does not return any more items
      if (!paths.isEmpty()) {
        // bulk delete the remaining ones
        doDeleteBulk(paths, cursor.getInfo().getTotalHits(),
            remainingChunks.get(), correlationId, handleBulk.apply(paths.size()));
      } else {
        // end operation
        handler.handle(Future.succeededFuture());
      }
    }
  }
  
  /**
   * Delete all chunks with the given paths from the index and from the store.
   * Remove all items from the given queue.
   * @param paths the queue of paths of chunks to delete (will be empty when
   * the operation has finished)
   * @param totalChunks the total number of paths to delete (including this batch)
   * @param remainingChunks the remaining chunks to delete (including this batch)
   * @param correlationId the correlation ID of the current purging task
   * @param handler will be called when the operation has finished
   */
  private void doDeleteBulk(Queue<String> paths, long totalChunks,
      long remainingChunks, String correlationId, Handler<AsyncResult<Void>> handler) {
    // delete from index first so the chunks cannot be found anymore
    JsonArray jsonPaths = new JsonArray();
    paths.forEach(jsonPaths::add);
    JsonObject indexMsg = new JsonObject()
        .put("correlationId", correlationId)
        .put("paths", jsonPaths)
        .put("totalChunks", totalChunks)
        .put("remainingChunks", remainingChunks);
    vertx.eventBus().send(AddressConstants.INDEXER_DELETE, indexMsg, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        // now delete all chunks from file system and clear the queue
        doDeleteChunks(paths, handler);
      }
    });
  }

  @Override
  public void getAttributeValues(String search, String path, String attribute,
      Handler<AsyncResult<AsyncCursor<String>>> handler) {
    JsonObject template = new JsonObject()
      .put("search", search)
      .put("attribute", attribute);
    if (path != null) {
      template.put("path", path);
    }
    new IndexedAsyncCursor<>(Objects::toString,
      AddressConstants.METADATA_GET_ATTRIBUTE_VALUES, vertx, template)
      .start(handler);
  }

  @Override
  public void getPropertyValues(String search, String path, String property,
      Handler<AsyncResult<AsyncCursor<String>>> handler) {
    JsonObject template = new JsonObject()
      .put("search", search)
      .put("property", property);
    if (path != null) {
      template.put("path", path);
    }
    new IndexedAsyncCursor<>(Objects::toString,
      AddressConstants.METADATA_GET_PROPERTY_VALUES, vertx, template)
      .start(handler);
  }

  @Override
  public void setProperties(String search, String path,
      Map<String, String> properties, Handler<AsyncResult<Void>> handler) {
    JsonObject msg = new JsonObject()
      .put("search", search)
      .put("properties", JsonObject.mapFrom(properties));
    if (path != null) {
      msg.put("path", path);
    }

    send(AddressConstants.METADATA_SET_PROPERTIES, msg, handler);
  }

  @Override
  public void removeProperties(String search, String path,
      List<String> properties, Handler<AsyncResult<Void>> handler) {
    JsonObject msg = new JsonObject()
      .put("search", search)
      .put("properties", new JsonArray(properties));
    if (path != null) {
      msg.put("path", path);
    }

    send(AddressConstants.METADATA_REMOVE_PROPERTIES, msg, handler);
  }

  @Override
  public void appendTags(String search, String path, List<String> tags,
      Handler<AsyncResult<Void>> handler) {
    JsonObject msg = new JsonObject()
      .put("search", search)
      .put("tags", new JsonArray(tags));
    if (path != null) {
      msg.put("path", path);
    }

    send(AddressConstants.METADATA_APPEND_TAGS, msg, handler);
  }

  @Override
  public void removeTags(String search, String path, List<String> tags,
      Handler<AsyncResult<Void>> handler) {
    JsonObject msg = new JsonObject()
      .put("search", search)
      .put("tags", new JsonArray(tags));
    if (path != null) {
      msg.put("path", path);
    }

    send(AddressConstants.METADATA_REMOVE_TAGS, msg, handler);
  }

  /**
   * Send a message to the specified address and pass null to the handler when
   * the verticle responds
   * @param address the address to send it to
   * @param msg the message to send
   * @param handler the handler which is called when the verticle responds
   */
  private void send(String address, Object msg,
      Handler<AsyncResult<Void>> handler) {
    vertx.eventBus().send(address, msg, ar -> handler.handle(ar.map(x -> null)));
  }

  /**
   * Generate or get a unique chunk identifier. The given correlation ID will
   * be used as the new identifier's prefix. The remaining 16 characters will
   * be constructed from the current time and an atomic counter.
   * @param correlationId the correlation ID of the current import process
   * @return chunk identifier
   */
  protected String generateChunkId(String correlationId) {
    int seconds = (int)(System.currentTimeMillis() / 1000);
    String time = StringUtils.leftPad(Integer.toHexString(seconds),
      Integer.SIZE / Byte.SIZE * 2, '0');
    String counter = StringUtils.leftPad(Integer.toHexString(COUNTER.getAndIncrement()),
      Integer.SIZE / Byte.SIZE * 2, '0');
    return correlationId + time + counter;
  }
  
  /**
   * Add a chunk to the store
   * @param chunk the chunk to add
   * @param path the chunk's destination path
   * @param handler will be called when the operation has finished
   */
  protected abstract void doAddChunk(String chunk, String path,
    String correlationId, Handler<AsyncResult<String>> handler);
  
  /**
   * Delete all chunks with the given paths from the store. Remove one item
   * from the given queue, delete the chunk, and then call recursively until
   * all items have been removed from the queue. Finally, call the given handler.
   * @param paths the queue of paths of chunks to delete (will be empty when
   * the operation has finished)
   * @param handler will be called when the operation has finished
   */
  protected abstract void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler);
}
