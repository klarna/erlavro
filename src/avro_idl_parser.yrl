Header "%%% @doc Avro IDL parser
%%%
%%% XXX: all `comment_v' tokens should be filtered-out before parsing!
%%% @end
%%% @reference See [https://avro.apache.org/docs/current/idl.html]
%%% @author Sergey Prokhhorov <me@seriyps.ru>".

Terminals id ns_id null string_v doc_v float_v integer_v bool_v annotation_v
    primitive_t logical_t decimal_t
    '{' '}' '(' ')' '[' ']' '<' '>' ';' ',' '=' ':'
    record_t enum_t array_t map_t fixed_t union_t
    protocol_k error_k throws_k oneway_k void_k import_k idl_k schema_k.

Nonterminals
    protocol
    meta meta_item annotation annotation_value string array_of_strings array_of_strings_tail
    declaration declaration_tail
    import import_file_type
    record record_field record_field_name record_tail
    type error
    decimal
    enum enum_variants
    union union_tail
    fixed
    array
    map
    function fun_return fun_arguments fun_argument fun_extra throws
    data array_of_data array_of_data_tail map_of_data map_of_data_tail.

Rootsymbol protocol.

%% There are 2 shift/reduce conflicts expected due to ambiguity in
%% meta / meta_item that is automatically correctly resolved with shift.
%% See https://www.gnu.org/software/bison/manual/html_node/Shift_002fReduce.html
Expect 2.

protocol ->
    protocol_k id '{' '}' :
        #protocol{name = value_of('$2')}.
protocol ->
    protocol_k id '{' declaration declaration_tail :
        #protocol{name = value_of('$2'), definitions = ['$4' | '$5']}.
protocol ->
    meta protocol :
        ('$2')#protocol{meta = '$1'}.


%% == Annotation ==
meta ->
    meta_item :
        ['$1'].
meta ->
    meta_item meta :
        ['$1' | '$2'].

meta_item ->
    annotation :
        '$1'.
meta_item ->
    doc_v :
        {doc, value_of('$1')}.

annotation ->
    annotation_v '(' annotation_value ')' :
        #annotation{name = value_of('$1'), value = '$3'}.

%% Maybe can just use `data` instead of `decorator_value`?
annotation_value ->
    string :
        '$1'.
annotation_value ->
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


%% == Protocol definitions ==

declaration_tail ->
    '}' :
        [].
declaration_tail ->
    declaration declaration_tail :
        ['$1' | '$2'].

declaration -> import : '$1'.
declaration -> enum : '$1'.
declaration -> fixed : '$1'.
declaration -> error : '$1'.
declaration -> record : '$1'.
declaration -> function : '$1'.


%% -- Import def

import ->
    import_k import_file_type string_v ';' :
        #import{type = '$2', file_path = value_of('$3')}.

import_file_type -> idl_k : idl.
import_file_type -> protocol_k : protocol.
import_file_type -> schema_k : schema.

%% -- Enum typedef
enum ->
    enum_t id '{' id enum_variants :
        #enum{name = value_of('$2'), variants = [value_of('$4') | '$5']}.
enum ->
    meta enum :
        ('$2')#enum{meta = '$1'}.

enum_variants ->
    '}' :
        [].
enum_variants ->
    ',' id enum_variants : [value_of('$2') | '$3'].

%% -- Fixed typedef
fixed ->
    fixed_t id '(' integer_v ')' ';':
        #fixed{name = value_of('$2'), size = value_of('$4')}.
fixed ->
    meta fixed :
        ('$2')#fixed{meta = '$1'}.

%% -- Error typedef
error ->
    error_k id '{' record_field record_tail :
        #error{name = value_of('$2'), fields = ['$4' | '$5']}.
error ->
    meta error :
        ('$2')#error{meta = '$1'}.

%% -- Record

record ->
    record_t id '{' record_field record_tail :
        #record{name = value_of('$2'), fields = ['$4' | '$5']}.
record ->
    meta record :
        ('$2')#record{meta = '$1'}.

record_tail ->
    '}' :
        [].
record_tail ->
    record_field record_tail :
        ['$1' | '$2'].

record_field ->
    type record_field_name ';' :
        #field{name = element(1, '$2'), meta = element(2, '$2'), type = '$1'}.
record_field ->
    type record_field_name '=' data ';' :
        #field{name = element(1, '$2'), meta = element(2, '$2'),
               type = '$1', default = '$4'}.
record_field ->
    meta record_field :
        ('$2')#field{meta = '$1' ++ ('$2')#field.meta}.

record_field_name ->
    id :
        {value_of('$1'), []}.
record_field_name ->
    meta id :
        {value_of('$2'), '$1'}.

type -> primitive_t : value_of('$1').
type -> logical_t : value_of('$1').
type -> null : null.
type -> id : {custom, value_of('$1')}.
type -> ns_id : {custom, value_of('$1')}.
type -> decimal : '$1'.
type -> union : '$1'.
type -> array : '$1'.
type -> map : '$1'.

%% -- Decimal
decimal ->
    decimal_t '(' integer_v ',' integer_v ')' :    %decimal(precision, scale)
        {decimal, value_of('$3'), value_of('$5')}. %

%% -- Union
union ->
    union_t '{' type union_tail :
        {union, ['$3' | '$4']}.

union_tail ->
    '}' :
        [].
union_tail ->
    ',' type union_tail :
        ['$2' | '$3'].

%% -- Array typedef
array ->
    array_t '<' type '>' :
        {array, '$3'}.

%% -- Map typedef
map ->
    map_t '<' type '>' :
        {map, '$3'}.

%% == Function (message) definitions

function ->
    fun_return id '(' fun_arguments ')' fun_extra ';' :
        #function{name = value_of('$2'), arguments = '$4', return = '$1', extra = '$6'}.
function ->
    doc_v function :
        ('$2')#function{meta = [{doc, value_of('$1')}]}.

fun_return -> type : '$1'.
fun_return -> void_k : void.

fun_arguments ->
    '$empty' :
        [].
fun_arguments ->
    fun_argument :
        ['$1'].
fun_arguments ->
    fun_argument ',' fun_arguments :
        ['$1' | '$3'].

fun_argument ->
    type id :
        {arg, value_of('$2'), '$1', undefined}.
fun_argument ->
    type id '=' data :
        {arg, value_of('$2'), '$1', '$4'}.

fun_extra ->
    '$empty' : undefined.
fun_extra ->
    throws_k id throws :
        {throws, [value_of('$2') | '$3']}.
fun_extra ->
    oneway_k :
        oneway.

throws ->
    '$empty' :
        [].
throws ->
    ',' id throws:
        [value_of('$2') | '$3'].

%% == Data (JSON) for default values
data -> string_v : value_of('$1').
data -> integer_v : value_of('$1').
data -> float_v : value_of('$1').
data -> bool_v : value_of('$1').
data -> array_of_data : '$1'.
data -> null : null.
data -> map_of_data : '$1'.

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

map_of_data ->
    '{' '}' :
        #{}.
map_of_data ->
    '{' string_v ':' data map_of_data_tail :
        ('$5')#{value_of('$2') => '$4'}.

map_of_data_tail ->
    '}' :
        #{}.
map_of_data_tail ->
    ',' string_v ':' data map_of_data_tail:
        ('$5')#{value_of('$2') => '$4'}.

Erlang code.
-include("idl.hrl").

value_of(Token) ->
    try element(3, Token)
    catch error:badarg ->
            error({badarg, Token})
    end.
