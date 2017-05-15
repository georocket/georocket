package io.georocket.util;

import static io.georocket.util.ThrowableHelper.throwableToCode;
import static io.georocket.util.ThrowableHelper.throwableToMessage;

import io.georocket.ServerAPIException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import java.nio.charset.StandardCharsets;
import rx.Single;

/**
 * Utils to handle http depended operations.
 * 
 * @author Andrej Sajenko
 */
public final class HttpUtils {
  
  private HttpUtils() {
  }
  
  /**
   * Let the request fail by setting the correct http error code and an error
   * description in the body
   * @param response the response object
   * @param throwable the cause of the error
   */
  public static void fail(HttpServerResponse response, Throwable throwable) {
    response
      .setStatusCode(throwableToCode(throwable))
      .end(errorResponse(throwable));
  }

  /**
   * Generate the json error response for a failed request
   * @param throwable the cause of the error
   * @return the json string
   */
  private static String errorResponse(Throwable throwable) {
    String msg = throwableToMessage(throwable, "");

    try {
      return new JsonObject(msg).toString();
    } catch (Exception e) {
      if (throwable instanceof ReplyException) {
        return ServerAPIException.toJson(ServerAPIException.GENERIC_ERROR, msg)
          .toString();
      }

      if (throwable instanceof HttpException) {
        return ServerAPIException.toJson(ServerAPIException.HTTP_ERROR, msg)
          .toString();
      }

      return ServerAPIException.toJson(ServerAPIException.GENERIC_ERROR, msg)
        .toString();
    }
  }

  /**
   * Get request body as JSON object.
   * @param request the request to extract to body from
   * @return Single holding the JSON as soon as the request is processed
   */
  public static Single<JsonObject> bodyAsJsonObject(HttpServerRequest request) {
    return Single.create(singleSubscriber -> {
      Buffer buffer = Buffer.buffer();
      request.handler(buffer::appendBuffer);
      request.exceptionHandler(singleSubscriber::onError);
      request.endHandler(v -> singleSubscriber.onSuccess(
          new JsonObject(buffer.toString(StandardCharsets.UTF_8))));
    });
  }
}
