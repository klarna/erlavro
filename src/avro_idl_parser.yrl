%% @doc Avro IDL parser
%% https://avro.apache.org/docs/current/idl.html

Header "%% Hello".

Terminals id string_v comment_v float_v integer_v bool_v annotation_v
    primitive_t logical_t
    '{' '}' '(' ')' '[' ']' '<' '>' ';' ',' '='
    record_t enum_t array_t map_t fixed_t union_t
    protocol_k error_k throws_k oneway_k void_k import_k idl_k schema_k.

Nonterminals
    protocol typedefs
    decorator decorator_value string array_of_strings array_of_strings_tail
    typedef_tail typedef
    import import_file_type
    primitive
    enum enum_variants
    union
    record
    fixed
    array
    map
    error
    data
    array_of_data array_of_data_tail.

Rootsymbol protocol.


protocol ->
    protocol_k id '{' '}' :
        {protocol, value_of('$2'), []}.
protocol ->
    protocol_k id '{' typedef typedef_tail :
        {protocol, value_of('$2'), ['$4' | '$5']}.
protocol ->
    decorator protocol :
        {decorated, '$1', '$2'}.  % todo: embed into protocol?


%% == Decorator ==
decorator ->
    annotation_v '(' decorator_value ')' :
        {decorator, value_of('$1'), '$3'}.

%% Maybe can just use `data` instead of `decorator_value`?
decorator_value ->
    string :
        '$1'.
decorator_value ->
    array_of_strings :
        '$1'.

string ->
    string_v :
        value_of('$1').

array_of_strings ->
    '[' ']' :
        [].
array_of_strings ->
    '[' string array_of_strings_tail :
        ['$2' | '$3'].

array_of_strings_tail ->
    ']' :
        [].
array_of_strings_tail ->
    ',' string array_of_strings_tail :
        ['$2' | '$3'].


%% == Type definitions (inside protocol or record) ==

typedef_tail ->
    '}' :
        [].
typedef_tail ->
    typedef typedef_tail :
        ['$1' | '$2'].

%% TODO: generalize to 'type' name (= value)(;)
typedef -> import : '$1'.
typedef -> primitive : '$1'.
typedef -> enum : '$1'.
typedef -> union : '$1'.
typedef -> record : '$1'.
typedef -> fixed : '$1'.
typedef -> array : '$1'.
typedef -> map : '$1'.
typedef -> error : '$1'.
%% typedef -> function : '$1'.  % TODO

%% -- Import def

import ->
    import_k import_file_type string_v ';' :
        {import, '$2', value_of('$3')}.

import_file_type -> idl_k : idl.
import_file_type -> protocol_k : protocol.
import_file_type -> schema_k : schema.

%% -- Primitive typedef
primitive ->
    primitive_t id ';' :
        {primitive, value_of('$2'), value_of('$1'), undefined}.
primitive ->
    primitive_t id '=' data ';' :
        {primitive, value_of('$2'), value_of('$1'), '$4'}.

%% -- Enum typedef
enum ->
    enum_t id '{' id enum_variants :
        {enum, value_of('$2'), [value_of('$4') | '$5']}.

enum_variants ->
    '}' :
        [].
enum_variants ->
    ',' id enum_variants : [value_of('$2') | '$3'].


union -> union_t : '$1'.                        %TODO
record -> record_t : '$1'.                      %TODO

%% -- Fixed typedef
fixed ->
    fixed_t id '(' integer_v ')' ';':
        {fixed, '$2', value_of('$4'), undefined}.
fixed ->
    fixed_t id '(' integer_v ')' '=' data ';' :
        {fixed, '$2', value_of('$4'), '$6'}.

%% -- Array typedef
array ->
    array_t '<' primitive_t '>' id ';' :
        {array, value_of('$5'), value_of('$3'), undefined}.         %FIXME: not just primitives!
array ->
    array_t '<' primitive_t '>' id '=' data ';' :
        {array, value_of('$5'), value_of('$3'), '$7'}.

%% -- Map typedef
map ->
    map_t '<' primitive_t '>' id ';' :
        {map, '$5', value_of('$3'), undefined}.           %FIXME: not just primitives!; defaults!

%% -- Error typedef
error ->
    error_k : '$1'.                             %TODO

%% == Data (JSON) for default values
data -> string_v : value_of('$1').
data -> integer_v : value_of('$1').
data -> float_v : value_of('$1').
data -> bool_v : value_of('$1').
data -> array_of_data : '$1'.

array_of_data ->
    '[' ']' :
        [].
array_of_data ->
    '[' data array_of_data_tail :
        ['$2' | '$3'].

array_of_data_tail ->
    ']' :
        [].
array_of_data_tail ->
    ',' data array_of_data_tail :
        ['$2' | '$3'].

Erlang code.

value_of(Token) ->
    try element(3, Token)
    catch error:badarg ->
            error({badarg, Token})
    end.
