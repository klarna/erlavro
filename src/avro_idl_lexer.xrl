%% @doc Avro IDL lexer
%% https://avro.apache.org/docs/current/idl.html

Definitions.

Rules.

[\s\t\n\r]+ : skip_token.

%% TODO: escaped double quotes inside strings
"[^\"]+" : {token, {string_v, TokenLine, unescape(TokenChars, $\")}}.

`[^\`]+` : {token, {id, TokenLine, unescape(TokenChars, $`)}}.

//[^\r\n]* : {token, {comment_v, TokenLine, unescape_line_comment(TokenChars)}}.

\{ : {token, {'{', TokenLine}}.
\} : {token, {'}', TokenLine}}.
\( : {token, {'(', TokenLine}}.
\) : {token, {')', TokenLine}}.
\[ : {token, {'[', TokenLine}}.
\] : {token, {']', TokenLine}}.
<  : {token, {'<', TokenLine}}.
>  : {token, {'>', TokenLine}}.
;  : {token, {';', TokenLine}}.
\, : {token, {',', TokenLine}}.

%% Null can be in both values and primitive types
null : {token, {null, TokenLine}}.

%% Default values (json)
= : {token, {'=', TokenLine}}.
%% TODO: better float regexp;
%% XXX: is it safe to use list_to_float? seems float syntax is used for decimal defaults as well
[+-]?[0-9]+\.[0-9]+ : {token, {float_v, TokenLine, list_to_float(TokenChars)}}.
[+-]?[0-9]+         : {token, {integer_v, TokenLine, list_to_integer(TokenChars)}}.
true|false          : {token, {bool_v, TokenLine, list_to_atom(TokenChars)}}.
\:                  : {token, {':', TokenLine}}.

%% === Datatype IDs ===

%% primitive
int|long|string|boolean|float|double|bytes : {token, {primitive_t, TokenLine, list_to_atom(TokenChars)}}.

%% complex
record|enum|array|map|fixed|union : {token, {list_to_atom(TokenChars ++ "_t"), TokenLine}}.

%% Logical
date|time_ms|timestamp_ms : {token, {logical_t, TokenLine, list_to_atom(TokenChars)}}.
decimal : {token, {decimal_t, TokenLine}}.
%% keywords
error|throws|oneway|void|import|idl|protocol|schema : {token, {list_to_atom(TokenChars ++ "_k"), TokenLine}}.

%% === Constructs ===

@[a-zA-Z0-9_-]+ : {token, {annotation_v, TokenLine, unescape_annotation(TokenChars)}}.

[A-Za-z_][A-Za-z0-9_]* : {token, {id, TokenLine, TokenChars}}.
%% namespaced will only be allowed in data type spec
[A-Za-z_][A-Za-z0-9_]+(\.[A-Za-z_][A-Za-z0-9_]+)+ : {token, {ns_id, TokenLine, TokenChars}}.

%% https://blog.ostermiller.org/finding-comments-in-source-code-using-regular-expressions/
%% `/** .. */` is a docstring for the following object
(/\*\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)  : {token, {doc_v, TokenLine, unescape_multiline_comment(TokenChars)}}.
%% `/* .. */` is just a comment
(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/) : {token, {comment_v, TokenLine, unescape_multiline_comment(TokenChars)}}.


Erlang code.
-export([preprocess/2]).

%% Api helpers

-spec preprocess(Tokens, [drop_comments | trim_doc]) -> Tokens when
      Tokens :: [tuple()].
preprocess(Tokens, Actions) ->
    lists:foldl(fun do_preprocess/2, Tokens, Actions).

do_preprocess(drop_comments, T) ->
    lists:filter(
      fun({comment_v, _, _}) -> false;
         (_) -> true
      end, T);
do_preprocess(trim_doc, T) ->
    lists:map(
      fun({doc_v, Loc, Val}) ->
              {doc_v, Loc, trim_doc(Val)};
         (Tok) -> Tok
      end, T).

trim_doc(Doc) ->
    re:replace(Doc, "^[\\s\\*]*((?U).*)[\\s]*$", "\\1",
               [global, multiline, {return, list}]).

%% Lexer internal helpers

unescape(Token, Char) ->
    string:strip(Token, both, Char).

unescape_line_comment("//" ++ Comment) ->
    Comment.

%% TODO: cleanup
unescape_multiline_comment("/**" ++ Comment0) ->
    %% Drop closing "*/"
    Len = length(Comment0),
    lists:sublist(Comment0, Len - 2);
unescape_multiline_comment("/*" ++ Comment0) ->
    Len = length(Comment0),
    lists:sublist(Comment0, Len - 2).

unescape_annotation("@" ++ Annotation) ->
    Annotation.
