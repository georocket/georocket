package io.georocket

import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf

/**
 * An exception that will be created if an API exception has happened
 * on the server side
 * @author Tim Hellhake
 * @since 1.1.0
 */
class ServerAPIException(

  /**
   * A unique machine readable type for the API exception
   */
  val type: String,

  /**
   * the reason for the exception
   */
  private val reason: String?
) : NoStackTraceThrowable(reason) {

  /**
   * Serialize exception
   * @return a JSON object with the type and the reason of this error
   */
  fun toJson(): JsonObject =
    Companion.toJson(type, reason)

  companion object {
    private const val serialVersionUID = -4139618811295918617L

    /**
     * The syntax of a property command is not valid
     * @since 1.1.0
     */
    const val INVALID_PROPERTY_SYNTAX_ERROR = "invalid_property_syntax_error"

    /**
     * The server issued an HTTP request which failed
     * @since 1.1.0
     */
    const val HTTP_ERROR = "http_error"

    /**
     * A generic error occurred, see reason for details
     * @since 1.1.0
     */
    const val GENERIC_ERROR = "generic_error"

    /**
     * Create a JSON object from a type and a reason
     * @param type the type
     * @param reason the reason
     * @return a JSON object with the specified type and the specified reason
     */
    fun toJson(type: String, reason: String?): JsonObject {
      return jsonObjectOf(
        "error" to jsonObjectOf(
          "type" to type,
          "reason" to reason
        )
      )
    }
  }
}
