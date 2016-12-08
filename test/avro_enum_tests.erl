%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, Klarna AB
%%% @doc
%%%
%%% @end
%%% Created : 07. Dec 2016 10:43 AM
%%%-------------------------------------------------------------------
-module(avro_enum_tests).
-author("tihon").

-import(avro_enum, [ cast/2
                   , get_value/1
                   , new/2
                   , type/2
                   ]).

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").

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

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
