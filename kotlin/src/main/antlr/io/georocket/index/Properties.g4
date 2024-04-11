grammar Properties;

@header {
package io.georocket.index;
import org.apache.commons.text.StringEscapeUtils;
import io.georocket.query.ThrowingErrorListener;
}

@members {
  public static java.util.Map<String, Object> parse(String properties) {
    PropertiesLexer lexer = new PropertiesLexer(CharStreams.fromString(properties.trim()));
    lexer.removeErrorListeners();
    lexer.addErrorListener(new ThrowingErrorListener());
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    PropertiesParser parser = new PropertiesParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new ThrowingErrorListener());

    return parser.properties().result;
  }
}

properties returns [java.util.Map<String, Object> result]
  @init {
    $result = new java.util.LinkedHashMap<String, Object>();
  }
  : a=keyvalue { $result.put($a.result.getFirst(), $a.result.getSecond()); }
    ( ',' b=keyvalue { $result.put($b.result.getFirst(), $b.result.getSecond()); } )*
    EOF
  ;

keyvalue returns [kotlin.Pair<String, Object> result]
 : string ':' value { $result = new kotlin.Pair<>($string.result, $value.result); }
 ;

value returns [Object result]
  : number { $result = $number.result; }
  | string { $result = $string.result; }
  ;

number returns [Number result]
  : NUMBER {
    try {
      $result = Long.parseLong($NUMBER.text);
    } catch (NumberFormatException e) {
      $result = Double.parseDouble($NUMBER.text);
    }
  }
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
NUMBER : [+-]? ( [0-9]+ ( '.' [0-9]* )? ( [eE] [+-]? [0-9]+ )? | '.' [0-9]+ ( [eE] [+-]? [0-9]+ )? ) ;
STRING : ~[ ,:\n\r"']+ ;
