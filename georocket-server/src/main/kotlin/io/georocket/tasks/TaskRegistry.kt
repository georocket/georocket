package io.georocket.tasks

import io.georocket.constants.ConfigConstants
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * A registry that tracks information about currently running tasks
 * @author Michel Kraemer
 */
object TaskRegistry {
  private var retainSeconds: Long = 0
  private val tasks = mutableMapOf<String, Task>()
  private val finishedTasks = sortedSetOf<Task>(
    Comparator.comparing<Task, Instant> { it.endTime }.thenBy { it.id })

  /**
   * Initialize the registry before it is being used
   */
  @Synchronized
  fun init(config: JsonObject) {
    retainSeconds = config.getLong(ConfigConstants.TASKS_RETAIN_SECONDS,
      ConfigConstants.DEFAULT_TASKS_RETAIN_SECONDS)
  }

  /**
   * Get a copy of all tasks
   */
  @Synchronized
  fun getAll(): List<Task> {
    cleanUp()
    return tasks.values.toList()
  }

  /**
   * Insert or update a [task]
   */
  @Synchronized
  fun upsert(task: Task) {
    val existingTask = tasks.put(task.id, task)
    if (existingTask?.endTime != null) {
      finishedTasks.remove(existingTask)
    }
    if (task.endTime != null) {
      finishedTasks.add(task)
    }
    cleanUp()
  }

  /**
   * Remove outdated tasks
   */
  private fun cleanUp() {
    val threshold = Instant.now().minus(retainSeconds, ChronoUnit.SECONDS)
    while (!finishedTasks.isEmpty() && finishedTasks.first().endTime!!.isBefore(threshold)) {
      val t = finishedTasks.pollFirst()!!
      tasks.remove(t.id)
    }
  }
}
