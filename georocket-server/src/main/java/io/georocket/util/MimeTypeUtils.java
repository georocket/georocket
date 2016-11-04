package io.georocket.util;

/**
 * Utility methods for mime types
 * @author Andrej Sajenko
 * @author Michel Kraemer
 */
public class MimeTypeUtils {
  /**
   * <p>Check if the given mime type belongs to another one.</p>
   * <p>Examples:</p>
   * <ul>
   *   <li>belongsTo("application/gml+xml", "application", "xml") == true</li>
   *   <li>belongsTo("application/exp+xml", "application", "xml") == true</li>
   *   <li>belongsTo("application/xml", "application", "xml") == true</li>
   *   <li>belongsTo("application/exp+xml", "text", "xml") == false</li>
   *   <li>belongsTo("application/exp+xml", "application", "json") == false</li>
   * </ul>
   * @param mimeType the mime type
   * @param otherType the general type of the other mime type
   * @param otherStructuredSyntaxSuffix the structured syntax suffix of the
   * other subtype (subtype = example+structuredSyntaxSuffix)
   * @return true if the mime type belongs to the other one
   */
  public static boolean belongsTo(String mimeType, String otherType,
      String otherStructuredSyntaxSuffix) {
    String mediaParts[] = mimeType.split("/");
    if (mediaParts.length != 2) {
      return false;
    }

    String type = mediaParts[0];
    String subtype = mediaParts[1];

    if (!type.equals(otherType)) {
      return false;
    }

    if (subtype.equals(otherStructuredSyntaxSuffix)) {
      return true;
    }

    String subtypeParts[] = subtype.split("\\+");
    if (subtypeParts.length != 2) {
      return false;
    }

    String structuredSyntaxSuffix = subtypeParts[1];
    return structuredSyntaxSuffix.equals(otherStructuredSyntaxSuffix);
  }
}
