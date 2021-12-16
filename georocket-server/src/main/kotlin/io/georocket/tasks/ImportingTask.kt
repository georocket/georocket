package io.georocket.tasks

import io.georocket.util.UniqueID
import java.time.Instant

/**
 * A task started by the [io.georocket.ImporterVerticle]
 * @author Michel Kraemer
 */
data class ImportingTask(
  override val id: String = UniqueID.next(),
  override val correlationId: String,
  override val startTime: Instant = Instant.now(),
  override val endTime: Instant? = null,
  override val error: TaskError? = null,

  /**
   * The number of chunks already imported by this task
   */
  var importedChunks: Long = 0,
) : Task
