%%%-------------------------------------------------------------------
%%% Copyright (c) 2013-2016 Klarna AB
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%
%%% @doc General Avro handling code.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro).

-export([ is_named_type/1
        , get_type_name/1
        , get_type_namespace/1
        , get_type_fullname/1
        , get_aliases/1
        ]).

-export([ split_type_name/2
        , split_type_name/3
        , build_type_fullname/2
        , build_type_fullname/3
        ]).

-export([read_schema/1]).

-export([cast/2]).

-export([to_term/1]).

-export_type([ enum_symbol/0
             , fullname/0
             , name/0
             , namespace/0
             , typedoc/0
             , union_index/0
             ]).

-type name() :: string().
-type namespace() :: string().
-type fullname() :: string().
-type typedoc() :: string().
-type enum_symbol() :: string().
-type union_index() :: non_neg_integer().

-include("erlavro.hrl").

%%%===================================================================
%%% API: Accessing types properties
%%%===================================================================

%% Returns true if the type can have its own name defined in schema.
-spec is_named_type(avro_value() | avro_type()) -> boolean().
is_named_type(#avro_value{type = T}) -> is_named_type(T);
is_named_type(#avro_record_type{})   -> true;
is_named_type(#avro_enum_type{})     -> true;
is_named_type(#avro_fixed_type{})    -> true;
is_named_type(_)                     -> false.

%% Returns the type's name. If the type is named then content of
%% its name field is returned which can be short name or full name,
%% depending on how the type was specified. If the type is unnamed
%% then Avro name of the type is returned.
-spec get_type_name(avro_value() | avro_type()) -> string().
get_type_name(#avro_value{type = T})             -> get_type_name(T);
get_type_name(#avro_primitive_type{name = Name}) -> Name;
get_type_name(#avro_record_type{name = Name})    -> Name;
get_type_name(#avro_enum_type{name = Name})      -> Name;
get_type_name(#avro_array_type{})                -> ?AVRO_ARRAY;
get_type_name(#avro_map_type{})                  -> ?AVRO_MAP;
get_type_name(#avro_union_type{})                -> ?AVRO_UNION;
get_type_name(#avro_fixed_type{name = Name})     -> Name.

%% Returns the type's namespace exactly as it is set in the type.
%% Depending on how the type was specified it could the namespace
%% or just an empty string if the name contains namespace in it.
%% If the type can't have namespace then empty string is returned.
-spec get_type_namespace(avro_value() | avro_type()) -> string().
get_type_namespace(#avro_value{type = T})             -> get_type_namespace(T);
get_type_namespace(#avro_primitive_type{})            -> "";
get_type_namespace(#avro_record_type{namespace = Ns}) -> Ns;
get_type_namespace(#avro_enum_type{namespace = Ns})   -> Ns;
get_type_namespace(#avro_array_type{})                -> "";
get_type_namespace(#avro_map_type{})                  -> "";
get_type_namespace(#avro_union_type{})                -> "";
get_type_namespace(#avro_fixed_type{namespace = Ns})  -> Ns.

%% Returns fullname stored inside the type. For unnamed types
%% their Avro name is returned.
-spec get_type_fullname(avro_value() | avro_type()) -> string().
get_type_fullname(#avro_value{type = T})              -> get_type_fullname(T);
get_type_fullname(#avro_primitive_type{name = Name})  -> Name;
get_type_fullname(#avro_record_type{fullname = Name}) -> Name;
get_type_fullname(#avro_enum_type{fullname = Name})   -> Name;
get_type_fullname(#avro_array_type{})                 -> ?AVRO_ARRAY;
get_type_fullname(#avro_map_type{})                   -> ?AVRO_MAP;
get_type_fullname(#avro_union_type{})                 -> ?AVRO_UNION;
get_type_fullname(#avro_fixed_type{fullname = Name})  -> Name.


%% Returns aliases for the type (types without aliases are considered to
%% have empty alias list).
-spec get_aliases(avro_type()) -> [string()].

get_aliases(#avro_primitive_type{})               -> [];
get_aliases(#avro_record_type{aliases = Aliases}) -> Aliases;
get_aliases(#avro_enum_type{aliases = Aliases})   -> Aliases;
get_aliases(#avro_array_type{})                   -> [];
get_aliases(#avro_map_type{})                     -> [];
get_aliases(#avro_union_type{})                   -> [];
get_aliases(#avro_fixed_type{aliases = Aliases})  -> Aliases.

%%%===================================================================
%%% API: Reading schema from json file
%%%===================================================================
-spec read_schema(file:filename()) ->
        {ok, avro_type()} | {error, any()}.
read_schema(File) ->
  case file:read_file(File) of
    {ok, Data} ->
      {ok, avro_json_decoder:decode_schema(Data)};
    Error ->
      Error
  end.

%%%===================================================================
%%% API: Calculating of canonical short and full names of types
%%%===================================================================

%% Splits type's name parts to its canonical short name and namespace.
-spec split_type_name(string(), string(), string()) -> {string(), string()}.

split_type_name(TypeName, Namespace, EnclosingNamespace) ->
  case split_fullname(TypeName) of
    {_, _} = N ->
      %% TypeName contains name and namespace
      N;
    false ->
      %% TypeName is a name without namespace, choose proper namespace
      ProperNs = if Namespace =:= "" -> EnclosingNamespace;
                    true             -> Namespace
                 end,
      {TypeName, ProperNs}
  end.

%% Same thing as before, but uses name and namespace from the specified type.
-spec split_type_name(avro_type(), string()) -> {string(), string()}.

split_type_name(Type, EnclosingNamespace) ->
  split_type_name(get_type_name(Type),
                  get_type_namespace(Type),
                  EnclosingNamespace).

%% Constructs the type's full name from provided name and namespace
-spec build_type_fullname(string(), string(), string()) -> string().

build_type_fullname(TypeName, Namespace, EnclosingNamespace) ->
  {ShortName, ProperNs} =
    split_type_name(TypeName, Namespace, EnclosingNamespace),
  make_fullname(ShortName, ProperNs).

%% Same thing as before but uses name and namespace from the specified type.
-spec build_type_fullname(avro_type(), string()) -> string().

build_type_fullname(Type, EnclosingNamespace) ->
  build_type_fullname(get_type_name(Type),
                      get_type_namespace(Type),
                      EnclosingNamespace).

%%%===================================================================
%%% API: Checking correctness of names and types specifications
%%%===================================================================

%% Tries to cast a value (which can be another Avro value or some erlang term)
%% to the specified Avro type performing conversion if required.
-spec cast(avro_type_or_name(), term()) -> {ok, avro_value()} | {error, term()}.

cast(TypeName, Value) when is_list(TypeName) ->
  case type_from_name(TypeName) of
    undefined ->
      %% If the type specified by its name then in most cases
      %% we don't know which module should handle it. The only
      %% thing which we can do here is to compare full name of
      %% Type and typeof(Value) and return Value if they are equal.
      %% This assumes also that plain erlang values can't be casted
      %% to types specified only by their names.
      case ?IS_AVRO_VALUE(Value) of
        true ->
          ValueType = ?AVRO_VALUE_TYPE(Value),
          case has_fullname(ValueType, TypeName) of
            true  -> {ok, Value};
            false -> {error, type_name_mismatch}
          end;
        false ->
          %% Erlang terms can't be casted to names
          {error, cast_erlang_term_to_name}
      end;
    Type ->
      %% The only exception are primitive types names because we know
      %% corresponding types and can cast to them.
      cast(Type, Value)
  end;
%% When casting to same type just return the original value
cast(T, ?AVRO_VALUE(T, _) = V)      -> {ok, V};
%% For all other combinations call corresponding cast functions
%% in type modules
cast(#avro_primitive_type{} = T, V) -> avro_primitive:cast(T, V);
cast(#avro_record_type{} = T,    V) -> avro_record:cast(T, V);
cast(#avro_enum_type{} = T,      V) -> avro_enum:cast(T, V);
cast(#avro_array_type{} = T,     V) -> avro_array:cast(T, V);
cast(#avro_map_type{} = T,       V) -> avro_map:cast(T, V);
cast(#avro_union_type{} = T,     V) -> avro_union:cast(T, V);
cast(#avro_fixed_type{} = T,     V) -> avro_fixed:cast(T, V);
cast(Type, _)                       -> {error, {unknown_type, Type}}.


%% @doc Convert avro values to erlang term.
-spec to_term(avro_value()) -> term().
to_term(#avro_value{type = T} = V) -> to_term(T, V).

to_term(#avro_primitive_type{}, V) -> avro_primitive:get_value(V);
to_term(#avro_record_type{}, V)    -> avro_record:to_term(V);
to_term(#avro_enum_type{}, V)      -> avro_enum:get_value(V);
to_term(#avro_array_type{}, V)     -> avro_array:to_term(V);
to_term(#avro_map_type{}, V)       -> avro_map:to_term(V);
to_term(#avro_union_type{}, V)     -> avro_union:to_term(V);
to_term(#avro_fixed_type{}, V)     -> avro_fixed:get_value(V);
to_term(T, _)                      -> erlang:error({unknown_type, T}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Splits FullName to {Name, Namespace} or returns false
%% if FullName is not a full name.
-spec split_fullname(string()) -> {string(), string()} | false.
split_fullname(FullName) ->
    case string:rchr(FullName, $.) of
        0 ->
            %% Dot not found
            false;
        DotPos ->
            { string:substr(FullName, DotPos + 1)
            , string:substr(FullName, 1, DotPos-1)
            }
    end.

make_fullname(Name, "") ->
  Name;
make_fullname(Name, Namespace) ->
  Namespace ++ "." ++ Name.

%%%===================================================================
%%% Internal functions: casting
%%%===================================================================

%% Checks if the type has specified full name
has_fullname(Type, FullName) ->
  is_named_type(Type) andalso get_type_fullname(Type) =:= FullName.

type_from_name(?AVRO_NULL)   -> avro_primitive:null_type();
type_from_name(?AVRO_INT)    -> avro_primitive:int_type();
type_from_name(?AVRO_LONG)   -> avro_primitive:long_type();
type_from_name(?AVRO_FLOAT)  -> avro_primitive:float_type();
type_from_name(?AVRO_DOUBLE) -> avro_primitive:double_type();
type_from_name(?AVRO_BYTES)  -> avro_primitive:bytes_type();
type_from_name(?AVRO_STRING) -> avro_primitive:string_type();
type_from_name(_)            -> undefined.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

-define(PRIMITIVE_VALUE(Name, Value),
        #avro_value{ type = #avro_primitive_type{name = Name}
                   , data = Value}).

get_test_type(Name, Namespace) ->
  avro_fixed:type(Name, 16, [{namespace, Namespace}]).

split_type_name_test() ->
  ?assertEqual({"tname", ""},
               split_type_name("tname", "", "")),
  ?assertEqual({"tname", "name.space"},
               split_type_name("tname", "name.space", "enc.losing")),
  ?assertEqual({"tname", "name.space"},
               split_type_name("name.space.tname", "", "name1.space1")),
  ?assertEqual({"tname", "enc.losing"},
               split_type_name("tname", "", "enc.losing")).

get_type_fullname_test() ->
  ?assertEqual("name.space.tname",
               get_type_fullname(get_test_type("tname", "name.space"))),
  ?assertEqual("int",
               get_type_fullname(avro_primitive:int_type())).

cast_primitive_test() ->
  ?assertEqual({ok, ?PRIMITIVE_VALUE(?AVRO_STRING, "abc")},
               cast(avro_primitive:string_type(), "abc")),
  ?assertEqual({ok, ?PRIMITIVE_VALUE(?AVRO_INT, 1)}, cast("int", 1)),
  ?assertEqual({ok, ?PRIMITIVE_VALUE(?AVRO_LONG, 1)}, cast("long", 1)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
