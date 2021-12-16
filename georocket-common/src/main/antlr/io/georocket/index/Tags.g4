grammar Tags;

@header {
package io.georocket.index;
import org.apache.commons.text.StringEscapeUtils;
}

@members {
  private static class ThrowingErrorListener extends BaseErrorListener {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
        int line, int charPositionInLine, String msg, RecognitionException e)
        throws ParseCancellationException {
      throw new ParseCancellationException("line " + line + ":" + charPositionInLine + " " + msg);
    }
  }

  public static java.util.List<String> parse(String tags) {
    TagsLexer lexer = new TagsLexer(new ANTLRInputStream(tags.trim()));
    lexer.removeErrorListeners();
    lexer.addErrorListener(new ThrowingErrorListener());
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    TagsParser parser = new TagsParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new ThrowingErrorListener());

    return parser.tags().result;
  }
}

tags returns [java.util.List<String> result]
  @init {
    $result = new java.util.ArrayList<String>();
  }
  : a=string { $result.add($a.result); }
    ( ',' b=string { $result.add($b.result); } )*
    EOF
  ;

string returns [String result]
  : QUOTED_STRING { $result = $QUOTED_STRING.text; }
  | STRING { $result = $STRING.text; }
  ;

QUOTED_STRING
  : (
    '"' ( '\\"' | ~('\n'|'\r') )*? '"'
    | '\'' ( '\\\'' | ~('\n'|'\r') )*? '\''
  ) {
    String s = getText();
    s = s.substring(1, s.length() - 1);
    s = StringEscapeUtils.unescapeJava(s);
    setText(s);
  }
  ;
STRING : ~[ ,:\n\r\"\']+ ;
