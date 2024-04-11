package io.georocket.util

import com.google.common.io.BaseEncoding
import org.bson.types.ObjectId
import java.util.Date

/**
 * Generates short unique identifiers
 * @author Michel Kraemer
 */
object UniqueID : IDGenerator {
  private val base32 = BaseEncoding.base32().lowerCase().omitPadding()

  /**
   * Generate a unique ID
   * @return the unique ID
   */
  override fun next(): String {
    // use a different "epoch" to make IDs shorter
    val o2 = ObjectId(Date(System.currentTimeMillis() - 1545829231994L))
    // convert to base32
    return base32.encode(o2.toByteArray())
  }
}
