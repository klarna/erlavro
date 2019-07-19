%% @doc Avro IDL lexer
%% https://avro.apache.org/docs/current/idl.html

Definitions.

Rules.

[\s\t\n\r]+ : skip_token.

"[^\"]+" : {token, {string_v, TokenLine, unescape(TokenChars, $\")}}.

`[^\`]+` : {token, {id, TokenLine, unescape(TokenChars, $`)}}.

//[^\r\n]* : {token, {comment_v, TokenLine, unescape_line_comment(TokenChars)}}.

/\*(.|[\r\n])*\*/ : {token, {comment_v, TokenLine, unescape_multiline_comment(TokenChars)}}.

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


%% Default values (json)
= : {token, {'=', TokenLine}}.
%% TODO: better float regexp
[+-]?[0-9]+\.[0-9]+ : {token, {float_v, TokenLine, list_to_float(TokenChars)}}.
[+-]?[0-9]+         : {token, {integer_v, TokenLine, list_to_integer(TokenChars)}}.
true|false          : {token, {bool_v, TokenLine, list_to_atom(TokenChars)}}.
%% TODO: null?/:(for maps)/???...

%% === Datatype IDs ===

%% primitive; FIXME: 'null' can be used in both primitive and data!
int|long|string|boolean|float|double|bytes|null : {token, {primitive_t, TokenLine, list_to_atom(TokenChars)}}.

%% complex
record|enum|array|map|fixed|union : {token, {list_to_atom(TokenChars ++ "_t"), TokenLine}}.

%% Logical
decimal|date|time_ms|timestamp_ms : {token, {logical_t, TokenLine, list_to_atom(TokenChars)}}.

%% keywords
error|throws|oneway|void|import|idl|protocol|schema : {token, {list_to_atom(TokenChars ++ "_k"), TokenLine}}.

%% === Constructs ===

@[a-zA-Z0-9_-]+ : {token, {annotation_v, TokenLine, unescape_annotation(TokenChars)}}.

[A-Za-z_][A-Za-z_0-9]* : {token, {id, TokenLine, TokenChars}}.

Erlang code.


unescape(Token, Char) ->
    string:trim(Token, both, [Char]).

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
