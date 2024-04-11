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
  val importedChunks: Long = 0,

  /**
   * The total size of the file to be imported in bytes
   */
  val bytesTotal: Long,

  /**
   * The number of bytes completely processed from the file to be imported
   */
  val bytesProcessed: Long = 0
) : Task
