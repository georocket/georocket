grammar Query;

@header {
package io.georocket.query.parser;
import org.apache.commons.text.StringEscapeUtils;
}

query
  : expr ( WS+ expr )*
  ;

expr
  : or
  | and
  | not
  | eq
  | gt
  | gte
  | lt
  | lte
  | number
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

eq
 : 'EQ' WS* '(' WS* keyvalue WS* ')'
 ;

gt
 : 'GT' WS* '(' WS* keyvalue WS* ')'
 ;

gte
 : 'GTE' WS* '(' WS* keyvalue WS* ')'
 ;

lt
 : 'LT' WS* '(' WS* keyvalue WS* ')'
 ;

lte
 : 'LTE' WS* '(' WS* keyvalue WS* ')'
 ;

keyvalue
 : string WS+ value
 ;

value
  : number
  | string
  ;

number
  : NUMBER
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
NUMBER : [+-]? ( [0-9]+ ( '.' [0-9]* )? ( [eE] [+-]? [0-9]+ )? | '.' [0-9]+ ( [eE] [+-]? [0-9]+ )? ) ;
STRING : ~[ \n\r"'()]+ ;
