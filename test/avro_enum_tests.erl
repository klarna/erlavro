%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Dec 2016 10:43 AM
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
