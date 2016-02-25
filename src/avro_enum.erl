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
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc Avro enum type implementation.
%%%
%%% All symbols in an enum must be unique; duplicates are prohibited.
%%%
%%% Internal data for an enum is the symbol string itself.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_enum).

%% API
-export([type/2]).
-export([type/3]).
-export([new/2]).
-export([get_value/1]).
-export([cast/2]).

-include("erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type(Name, Symbols) ->
  type(Name, Symbols, []).

%% Options:
%%   namespace    :: string()
%%   doc          :: string()
%%   aliases      :: [string()]
%%   enclosing_ns :: string()
type(Name, Symbols, Opts) ->
  check_symbols(Symbols),
  Ns          = avro_util:get_opt(namespace, Opts, ""),
  Doc         = avro_util:get_opt(doc, Opts, ""),
  Aliases     = avro_util:get_opt(aliases, Opts, []),
  EnclosingNs = avro_util:get_opt(enclosing_ns, Opts, ""),
  avro_util:verify_aliases(Aliases),
  Type = #avro_enum_type
         { name      = Name
         , namespace = Ns
         , aliases   = avro_util:canonicalize_aliases(
                         Aliases, Name, Ns, EnclosingNs)
         , doc       = Doc
         , symbols   = Symbols
         , fullname  = avro:build_type_fullname(Name, Ns, EnclosingNs)
         },
  avro_util:verify_type(Type),
  Type.

-spec new(#avro_enum_type{}, term()) -> avro_value().

new(Type, Value) when ?AVRO_IS_ENUM_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Rec}    -> Rec;
    {error, Err} -> erlang:error(Err)
  end.

get_value(Value) when ?AVRO_IS_ENUM_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

%% Enums can be casted from other enums or strings
-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.

cast(Type, Value) when ?AVRO_IS_ENUM_TYPE(Type) ->
  do_cast(Type, Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_symbols(Symbols) ->
  SymLen = length(Symbols),
  ?ERROR_IF(SymLen =:= 0,
            empty_symbols),
  ?ERROR_IF(length(lists:usort(Symbols)) =/= SymLen,
            non_unique_symbols),
  lists:foreach(fun(S) -> avro_util:verify_name(S) end, Symbols).

do_cast(Type, Value) when ?AVRO_IS_ENUM_VALUE(Value) ->
  %% When casting from other enums only equality of type names is checked
  TargetTypeName = Type#avro_enum_type.fullname,
  SourceTypeName = (?AVRO_VALUE_TYPE(Value))#avro_enum_type.fullname,
  if TargetTypeName =:= SourceTypeName -> {ok, Value};
     true                              -> {error, type_name_mismatch}
  end;
do_cast(Type, Value) when is_list(Value) ->
  case lists:member(Value, Type#avro_enum_type.symbols) of
    true  -> {ok, ?AVRO_VALUE(Type, Value)};
    false -> {error, {cast_error, Type, Value}}
  end;
do_cast(Type, Value) ->
  {error, {cast_error, Type, Value}}.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

empty_symbols_test() ->
  ?assertError(empty_symbols, type("FooBar", [])).

non_unique_symbols_test() ->
  ?assertError(non_unique_symbols, type("FooBar", ["a", "c", "d", "c", "e"])).

incorrect_name_test() ->
  ?assertError({invalid_name, "c-1"},
               type("FooBar", ["a", "b", "c-1", "d", "c", "e"])).

correct_cast_from_enum_test() ->
  SourceType = type("MyEnum", ["a", "b", "c", "d"]),
  SourceValue = new(SourceType, "b"),
  TargetType = SourceType,
  ?assertEqual({ok, SourceValue}, cast(TargetType, SourceValue)).

incorrect_cast_from_enum_test() ->
  SourceType = type("MyEnum", ["a", "b", "c", "d"]),
  SourceValue = new(SourceType, "b"),
  TargetType = type("MyEnum2", ["a", "b", "c", "d"]),
  ?assertEqual({error, type_name_mismatch}, cast(TargetType, SourceValue)).


correct_cast_from_string_test() ->
  Type = type("MyEnum", ["a", "b", "c", "d"]),
  {ok, Enum} = cast(Type, "b"),
  ?assertEqual(Type, ?AVRO_VALUE_TYPE(Enum)),
  ?assertEqual("b", get_value(Enum)).

incorrect_cast_from_string_test() ->
  Type = type("MyEnum", ["a", "b", "c", "d"]),
  ?assertEqual({error, {cast_error, Type, "e"}}, cast(Type, "e")).

get_value_test() ->
  Type = type("MyEnum", ["a", "b", "c", "d"]),
  Value = new(Type, "b"),
  ?assertEqual("b", get_value(Value)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
