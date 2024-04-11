package io.georocket.tasks

import io.georocket.util.UniqueID
import java.time.Instant

/**
 * A task started by the [io.georocket.http.StoreEndpoint] when it
 * receives a file from the client
 * @author Michel Kraemer
 */
data class ReceivingTask(
  override val id: String = UniqueID.next(),
  override val correlationId: String,
  override val startTime: Instant = Instant.now(),
  override val endTime: Instant? = null,
  override val error: TaskError? = null
) : Task
