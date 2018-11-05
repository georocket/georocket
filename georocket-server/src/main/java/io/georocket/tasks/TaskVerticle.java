package io.georocket.tasks;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * A verticle that tracks information about currently running tasks
 * @author Michel Kraemer
 */
public class TaskVerticle extends AbstractVerticle {
  private long retainSeconds;
  private Map<String, Map<Class<? extends Task>, Task>> tasks = new LinkedHashMap<>();
  private TreeSet<Task> finishedTasks = new TreeSet<>(Comparator.comparing(Task::getEndTime)
    .thenComparingInt(System::identityHashCode));

  @Override
  public void start() {
    retainSeconds = config().getLong(ConfigConstants.TASKS_RETAIN_SECONDS,
        ConfigConstants.DEFAULT_TASKS_RETAIN_SECONDS);

    vertx.eventBus().consumer(AddressConstants.TASK_GET_ALL, this::onGetAll);
    vertx.eventBus().consumer(AddressConstants.TASK_GET_BY_CORRELATION_ID,
        this::onGetByCorrelationId);
    vertx.eventBus().consumer(AddressConstants.TASK_INC, this::onInc);
  }

  /**
   * Handle a request to get all tasks
   * @param msg the request
   */
  private void onGetAll(Message<Void> msg) {
    cleanUp();
    JsonObject result = new JsonObject();
    tasks.forEach((c, m) -> result.put(c, makeResponse(m)));
    msg.reply(result);
  }

  /**
   * Handle a request to get all tasks of a given correlation ID
   * @param msg the request
   */
  private void onGetByCorrelationId(Message<String> msg) {
    cleanUp();
    String correlationId = msg.body();
    if (correlationId == null) {
      msg.fail(400, "Correlation ID expected");
      return;
    }

    Map<Class<? extends Task>, Task> m = tasks.get(correlationId);
    if (m == null) {
      msg.fail(404, "Unknown correlation ID");
      return;
    }

    JsonObject result = new JsonObject();
    result.put(correlationId, makeResponse(m));
    msg.reply(result);
  }

  /**
   * Make a response containing information about single correlation ID
   * @param m the information about the correlation ID
   * @return the response
   */
  private JsonArray makeResponse(Map<Class<? extends Task>, Task> m) {
    JsonArray arr = new JsonArray();
    m.forEach((cls, t) -> {
      JsonObject o = JsonObject.mapFrom(t);
      o.remove("correlationId");
      arr.add(o);
    });
    return arr;
  }

  /**
   * Handle a request to increment the values of a task
   * @param msg the request
   */
  private void onInc(Message<JsonObject> msg) {
    JsonObject body = msg.body();
    if (body == null) {
      // ignore
      return;
    }

    Task t = body.mapTo(Task.class);

    Map<Class<? extends Task>, Task> m = tasks.computeIfAbsent(
        t.getCorrelationId(), k -> new LinkedHashMap<>());

    Task existingTask = m.get(t.getClass());
    if (existingTask != null) {
      if (existingTask.getEndTime() != null && t.getEndTime() != null) {
        // End time will be updated. Temporarily remove existing task from finished tasks.
        finishedTasks.remove(existingTask);
      }
      existingTask.inc(t);
      t = existingTask;
    } else {
      m.put(t.getClass(), t);
    }

    if (t.getEndTime() != null) {
      finishedTasks.add(t);
    }

    // help the indexer verticle and finish the indexing task if the importer
    // task has also finished and the number of indexed chunks equals the
    // number of imported chunks
    if (t instanceof IndexingTask || t instanceof ImportingTask) {
      ImportingTask importingTask = (ImportingTask)m.get(ImportingTask.class);
      if (importingTask != null && importingTask.getEndTime() != null) {
        IndexingTask indexingTask = (IndexingTask)m.get(IndexingTask.class);
        if (indexingTask != null && indexingTask.getEndTime() == null &&
            indexingTask.getIndexedChunks() == importingTask.getImportedChunks()) {
          indexingTask.setEndTime(Instant.now());
          finishedTasks.add(indexingTask);
        }
      }
    }

    // help the indexer verticle and finish the removing task if all chunks
    // have been removed
    if (t instanceof RemovingTask && t.getEndTime() == null) {
      RemovingTask rt = (RemovingTask)t;
      if (rt.getTotalChunks() == rt.getRemovedChunks()) {
        rt.setEndTime(Instant.now());
        finishedTasks.add(rt);
      }
    }

    cleanUp();
  }

  /**
   * Remove outdated tasks
   */
  private void cleanUp() {
    Instant threshold = Instant.now().minus(retainSeconds, ChronoUnit.SECONDS);
    while (!finishedTasks.isEmpty() && finishedTasks.first().getEndTime().isBefore(threshold)) {
      Task t = finishedTasks.pollFirst();
      Map<Class<? extends Task>, Task> m = tasks.get(t.getCorrelationId());
      if (m != null) {
        m.remove(t.getClass());
        if (m.isEmpty()) {
          tasks.remove(t.getCorrelationId());
        }
      }
    }
  }
}
