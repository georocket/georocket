package io.georocket.query

import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Recognizer
import org.antlr.v4.runtime.misc.ParseCancellationException

/**
 * An ANTLR error listener that throws an exception on syntax error
 * @author Michel Kraemer
 */
class ThrowingErrorListener : BaseErrorListener() {
  override fun syntaxError(recognizer: Recognizer<*, *>, offendingSymbol: Any?,
      line: Int, charPositionInLine: Int, msg: String, e: RecognitionException?) {
    throw ParseCancellationException("line $line:$charPositionInLine $msg")
  }
}
