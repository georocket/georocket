package io.georocket.query

private val ESCAPE_REGEX = """[.*+?$^{}()|\[\]\\]""".toRegex()

/**
 * Escapes a string so it can be used as a literal in a regular expression
 */
fun String.escapeRegex(): String = replace(ESCAPE_REGEX, "\\\\$0")
