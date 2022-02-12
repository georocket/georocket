package io.georocket.index

/**
 * Normalizes the layer name.
 *
 * Normalized layer names have the following properties:
 *
 *  - No leading slash, but a trailing slash.
 *    Example: 'foo/bar/'
 *
 *    The root path is an empty string ('').
 *
 *    The trailing slash allows matching a normalized layer to a path prefix using String.startsWith().
 *    Example: layer.startsWith("foo/") matches for layer="foo/" and layer="foo/bar/", but not layer="foo123/".
 *    Without the trailing slashes, the last example would match too, even though it is not a sub folder of 'foo'.
 *
 *  - no white spaces between path segments and no empty path segments
 *    Example: ' foo / bar baz / ' -> 'foo/bar baz/'
 *    Example: 'foo//bar/' -> 'foo/bar/'
 */
fun normalizeLayer(layer: String): String {

  return layer.trim()

    // split path segments
    .split("/")

    // remove whitespaces between path segments
    .map { it.trim() }

    // remove empty path segments.
    // this especially gets rid of leading slashes  ( '/foo' -> 'foo )
    .filter { it.isNotEmpty() }

    // re-join to path
    .joinToString("") { "$it/" }
}

/**
 * Validates the given layer name.
 *
 * Valid layer names are only allowed to contain:
 *  - alphanumeric characters (a-z, A-Z, 0-9)
 *  - path separators (/)
 *  - the special characters '_' and '-'
 *  - spaces
 *
 * This is to make sure, that:
 *  - Maliciously crafted layer names cannot be used to access resources outside the storage path.
 *    Most importantly, the usage of relative paths (../../folder) or the shortcut
 *    to the (linux) home folder (~/Documents), is forbidden.
 *  - The ogc-api-features endpoint uses '.' as an alternative path separator. The same holds for
 *    windows and its '\' path separator (when using the FileStore storage implementation).
 *    Replacing the path separators '/' with '\' or '.' does not lead to ambiguities.
 */
fun isLayerValid(layer: String): Boolean {
  return layer.matches(Regex("[a-zA-Z0-9-_ /]*"))
}
