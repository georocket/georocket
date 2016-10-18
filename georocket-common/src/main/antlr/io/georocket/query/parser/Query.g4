grammar Query;

@header {
package io.georocket.query.parser;
import org.apache.commons.lang3.StringEscapeUtils;
}

query
  : expr ( WS+ expr )*
  ;

expr
  : or
  | and
  | not
  | string
  ;

or
 : 'OR' WS* '(' WS* query WS* ')'
 ;

and
 : 'AND' WS* '(' WS* query WS* ')'
 ;

not
 : 'NOT' WS* '(' WS* query WS* ')'
 ;

string
  : QUOTED_STRING
  | STRING
  ;

WS : [ \n\r] ;
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
STRING : ~[ \n\r\"\'()]+ ;
