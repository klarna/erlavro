%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%% The module manages Avro types.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_schema).

%% API
-export([get_name/1]).
-export([get_fullname/2]).

-export([null/0]).
-export([boolean/0]).
-export([int/0]).
-export([long/0]).
-export([float/0]).
-export([double/0]).
-export([bytes/0]).
-export([string/0]).

-export([is_primitive_type/1]).

-export([is_null/1]).
-export([is_boolean/1]).
-export([is_int/1]).
-export([is_long/1]).
-export([is_float/1]).
-export([is_double/1]).
-export([is_bytes/1]).
-export([is_string/1]).
-export([is_union/1]).

-export([verify/1]).

%% TODO: -include("erlavro.hrl").
-include_lib("erlavro/include/erlavro.hrl").

%% Names of primitive types
-define(AVRO_NULL,    "null").
-define(AVRO_BOOLEAN, "boolean").
-define(AVRO_INT,     "int").
-define(AVRO_LONG,    "long").
-define(AVRO_FLOAT,   "float").
-define(AVRO_DOUBLE,  "double").
-define(AVRO_BYTES,   "bytes").
-define(AVRO_STRING,  "string").

%% Other reserved types names
-define(AVRO_ARRAY,   "array").
-define(AVRO_MAP,     "map").
-define(AVRO_UNION,   "union").


%%%===================================================================
%%% API: Type information extraction
%%%===================================================================

is_named_type(#avro_record_type{}) -> true;
is_named_type(#avro_enum_type{})   -> true;
is_named_type(#avro_fixed_type{})  -> true;
is_named_type(_)              -> false.

get_name(#avro_primitive_type{name = Name})              -> {Name,        ""};
get_name(#avro_record_type{name = Name, namespace = Ns}) -> {Name,        Ns};
get_name(#avro_enum_type{name = Name, namespace = Ns})   -> {Name,        Ns};
get_name(#avro_array_type{})                             -> {?AVRO_ARRAY, ""};
get_name(#avro_map_type{})                               -> {?AVRO_MAP,   ""};
get_name(#avro_union_type{})                             -> {?AVRO_UNION, ""};
get_name(#avro_fixed_type{name = Name, namespace = Ns})  -> {Name,        Ns}.

get_fullname(Type, EnclosingNamespace) ->
    {Name, Ns} = get_name(Type),
    case is_named_type(Type) of
        true  ->
            {ProperName, ProperNs} =
                canonicalize_name(Name, Ns, EnclosingNamespace),
            make_fullname(ProperName, ProperNs);
        false ->
            %% Primitive and unnamed types don't belong to any namespace
            Name
    end.

%%%===================================================================
%%% API: Schema definition
%%%===================================================================

null()    -> #avro_primitive_type{name = ?AVRO_NULL}.

boolean() -> #avro_primitive_type{name = ?AVRO_BOOLEAN}.

int()     -> #avro_primitive_type{name = ?AVRO_INT}.

long()    -> #avro_primitive_type{name = ?AVRO_LONG}.

float()   -> #avro_primitive_type{name = ?AVRO_FLOAT}.

double()  -> #avro_primitive_type{name = ?AVRO_DOUBLE}.

bytes()   -> #avro_primitive_type{name = ?AVRO_BYTES}.

string()  -> #avro_primitive_type{name = ?AVRO_STRING}.

%%%===================================================================
%%% API: Schema verification
%%%===================================================================

is_primitive_type(#avro_primitive_type{}) -> true;
is_primitive_type(_)                      -> false.

is_null(Type)    -> is_primitive_type(Type, ?AVRO_NULL).

is_boolean(Type) -> is_primitive_type(Type, ?AVRO_BOOLEAN).

is_int(Type)     -> is_primitive_type(Type, ?AVRO_INT).

is_long(Type)    -> is_primitive_type(Type, ?AVRO_LONG).

is_float(Type)   -> is_primitive_type(Type, ?AVRO_FLOAT).

is_double(Type)  -> is_primitive_type(Type, ?AVRO_DOUBLE).

is_bytes(Type)   -> is_primitive_type(Type, ?AVRO_BYTES).

is_string(Type)  -> is_primitive_type(Type, ?AVRO_STRING).

is_union(#avro_union_type{}) -> true;
is_union(_)                  -> false.

verify(Type) ->
    case is_named_type(Type) of
        true  -> verify_type_name(Type);
        false -> ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

reserved_type_names() ->
    [?AVRO_NULL, ?AVRO_BOOLEAN, ?AVRO_INT, ?AVRO_LONG, ?AVRO_FLOAT,
     ?AVRO_DOUBLE, ?AVRO_BYTES, ?AVRO_STRING, ?AVRO_ARRAY, ?AVRO_MAP,
     ?AVRO_UNION].

canonicalize_name(Name, Namespace, EnclosingNamespace) ->
    case split_fullname(Name) of
        {_, _} = N -> N;
        false ->
            ProperNs = if Namespace =:= "" -> EnclosingNamespace;
                          true             -> Namespace
                       end,
            {Name, ProperNs}
    end.

is_correct_name(Name) ->
    Re = "^([A-Za-z_][A-Za-z0-9_]*\.)*[A-Za-z_][A-Za-z0-9_]*$",
    re:run(Name, Re) =/= nomatch.

verify_type_name(Type) ->
    {Name, Ns} = get_name(Type),
    error_if_false(is_correct_name(Name),
                   {error, {invalid_name, Name}}),
    error_if_false(Ns =:= "" orelse is_correct_name(Ns),
                   {error, {invalid_name, Ns}}),
    %% It is important to call canonicalize_name after basic checks,
    %% because it assumes that all names are correct.
    %% We are not interested in the namespace here, so we can ignore
    %% EnclosingExtension value.
    {CanonicalName, _} = canonicalize_name(Name, Ns, ""),
    error_if_false(not lists:member(CanonicalName, reserved_type_names()),
                   {error, reserved_name_is_used_for_type_name}).

is_primitive_type(#avro_primitive_type{} = Type, TypeName) ->
    Type#avro_primitive_type.name =:= TypeName;
is_primitive_type(_, _) ->
    false.

%% Splits FullName to {Name, Namespace} or returns false
%% if FullName is not a full name.
%% The function can fail if it is called on badly formatted names.
-spec split_fullname(string()) -> {string(), string()} | false.
split_fullname(FullName) ->
    case string:rchr(FullName, $.) of
        0 ->
            %% Dot not found
            false;
        DotPos ->
            {string:substr(FullName, 1, DotPos-1),
             string:substr(FullName, DotPos + 1)}
    end.

make_fullname(Name, Namespace) ->
    Namespace ++ "." ++ Name.

error_if_false(true, _Err) -> ok;
error_if_false(false, Err) -> erlang:error(Err).
