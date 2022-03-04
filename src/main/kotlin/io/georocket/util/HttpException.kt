package io.georocket.util

/**
 * An exception that will be created if an HTTP error has happened
 * @author Michel Kraemer
 * @since 1.2.0
 */
class HttpException
(
  /**
   * the HTTP status code
   */
  val statusCode: Int,

  /**
   * the status message
   */
  val statusMessage: String? = null

) : Exception(if (statusMessage == null) "$statusCode" else "$statusCode $statusMessage") {

  companion object {
    private const val serialVersionUID = -6784905885779199095L
  }
}
