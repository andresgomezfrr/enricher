grammar EnricherQL;

query: SELECT (STAR | (id (',' id)*)) FROM type (id (',' id)*) (query_join)* query_output;

query_join: JOIN SELECT (STAR|(id (',' id)*)) FROM type id USING className;

query_output: INSERT INTO type id;

type
    : STREAM
    | TABLE
    ;

className: ID ('.' ID)*;

id: ID;

STAR: '*';

// Keywords
SELECT: S E L E C T;
FROM: F R O M;
JOIN: J O I N;
TABLE: T A B L E;
STREAM: S T R E A M;
USING: U S I N G;
INSERT: I N S E R T;
INTO: I N T O;

ID : [a-zA-Z_] [a-zA-Z_0-9]*;

WS: [ \r\n\t] -> skip;

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];