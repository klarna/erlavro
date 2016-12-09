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
%%% @doc Avro Json decoder
%%% @end
%%%-------------------------------------------------------------------
-module(avro_enum_tests).
-author("tihon").

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").

empty_symbols_test() ->
  ?assertError(empty_symbols, avro_enum:type("FooBar", [])).

non_unique_symbols_test() ->
  ?assertError(non_unique_symbols, avro_enum:type("FooBar", ["a", "c", "d", "c", "e"])).

incorrect_name_test() ->
  ?assertError({invalid_name, "c-1"},
    avro_enum:type("FooBar", ["a", "b", "c-1", "d", "c", "e"])).

correct_cast_from_enum_test() ->
  SourceType = avro_enum:type("MyEnum", ["a", "b", "c", "d"]),
  SourceValue = avro_enum:new(SourceType, "b"),
  TargetType = SourceType,
  ?assertEqual({ok, SourceValue}, avro_enum:cast(TargetType, SourceValue)).

incorrect_cast_from_enum_test() ->
  SourceType = avro_enum:type("MyEnum", ["a", "b", "c", "d"]),
  SourceValue = avro_enum:new(SourceType, "b"),
  TargetType = avro_enum:type("MyEnum2", ["a", "b", "c", "d"]),
  ?assertEqual({error, type_name_mismatch}, avro_enum:cast(TargetType, SourceValue)).


correct_cast_from_string_test() ->
  Type = avro_enum:type("MyEnum", ["a", "b", "c", "d"]),
  {ok, Enum} = avro_enum:cast(Type, "b"),
  ?assertEqual(Type, ?AVRO_VALUE_TYPE(Enum)),
  ?assertEqual("b", avro_enum:get_value(Enum)).

incorrect_cast_from_string_test() ->
  Type = avro_enum:type("MyEnum", ["a", "b", "c", "d"]),
  ?assertEqual({error, {cast_error, Type, "e"}}, avro_enum:cast(Type, "e")).

get_value_test() ->
  Type = avro_enum:type("MyEnum", ["a", "b", "c", "d"]),
  Value = avro_enum:new(Type, "b"),
  ?assertEqual("b", avro_enum:get_value(Value)).
