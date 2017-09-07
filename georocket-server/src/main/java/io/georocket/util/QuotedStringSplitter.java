package io.georocket.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.text.StringEscapeUtils;

/**
 * Splits strings around whitespace characters. Takes care of double-quotes
 * and single-quotes. Trims and unescapes the results.
 * @author Michel Kraemer
 */
public class QuotedStringSplitter {
  private static final Pattern pattern =
      Pattern.compile("\"((\\\\\"|[^\"])*)\"|'((\\\\'|[^'])*)'|(\\S+)");
  
  /**
   * <p>Splits strings around whitespace characters.</p>
   * <p>Example:</p>
   * <pre>
   * input string: "Hello World"
   * output:       ["Hello", "World"]
   * </pre>
   * 
   * <p>Takes care of double-quotes and single-quotes.</p>
   * <p>Example:</p>
   * <pre>
   * input string: "\"Hello World\" 'cool'"
   * output:       ["Hello World", "cool"]
   * </pre>
   * 
   * <p>Trims and unescapes the results.</p>
   * <p>Example:</p>
   * <pre>
   * input string: "  Hello   \"Wo\\\"rld\"  "
   * output:       ["Hello", "Wo\"rld"]
   * </pre>
   * 
   * @param str the string to split
   * @return the parts
   */
  public static List<String> split(String str) {
    Matcher m = pattern.matcher(str);
    List<String> result = new ArrayList<>();
    while (m.find()) {
      String r;
      if (m.group(1) != null) {
        r = m.group(1);
      } else if (m.group(3) != null) {
        r = m.group(3);
      } else {
        r = m.group();
      }
      result.add(StringEscapeUtils.unescapeJava(r));
    }
    return result;
  }
}
