%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, Klarna AB
%%% @doc
%%%
%%% @end
%%% Created : 07. Dec 2016 10:44 AM
%%%-------------------------------------------------------------------
-module(avro_fixed_tests).
-author("tihon").

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").

neg_size_test() ->
  ?assertError(invalid_size, avro_fixed:type("FooBar", -1)).

short_create_test() ->
  Type = avro_fixed:type("FooBar", 16),
  ?assertEqual("FooBar", avro:get_type_fullname(Type)),
  ?assertEqual(16, avro_fixed:get_size(Type)).

full_create_test() ->
  Type = avro_fixed:type("FooBar", 16,
    [ {namespace, "name.space"}
      , {aliases, ["Zoo", "Bee"]}
      , {enclosing_ns, "enc.losing"}
    ]),
  ?assertEqual("name.space.FooBar", avro:get_type_fullname(Type)),
  ?assertEqual(16, avro_fixed:get_size(Type)).

incorrect_cast_from_fixed_test() ->
  SourceType = avro_fixed:type("FooBar", 2),
  SourceValue = avro_fixed:new(SourceType, <<1,2>>),
  TargetType = avro_fixed:type("BarFoo", 2),
  ?assertEqual({error, type_name_mismatch},
               avro_fixed:cast(TargetType, SourceValue)).

correct_cast_from_fixed_test() ->
  SourceType = avro_fixed:type("FooBar", 2),
  SourceValue = avro_fixed:new(SourceType, <<1,2>>),
  TargetType = avro_fixed:type("FooBar", 2),
  ?assertEqual({ok, SourceValue}, avro_fixed:cast(TargetType, SourceValue)).

incorrect_cast_from_binary_test() ->
  Type = avro_fixed:type("FooBar", 2),
  ?assertEqual({error, wrong_binary_size}, avro_fixed:cast(Type, <<1,2,3>>)),
  ?assertEqual({error, wrong_binary_size}, avro_fixed:cast(Type, <<1>>)).

correct_cast_from_binary_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Bin = <<1,2>>,
  ?assertEqual({ok, ?AVRO_VALUE(Type, Bin)}, avro_fixed:cast(Type, Bin)).

integer_cast_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Value1 = avro_fixed:new(Type, 67),   %% 1 byte
  Value2 = avro_fixed:new(Type, 1017), %% 2 bytes
  ?assertEqual(67, avro_fixed:to_integer(Value1)),
  ?assertEqual(1017, avro_fixed:to_integer(Value2)).

get_value_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Value = avro_fixed:new(Type, <<1,2>>),
  ?assertEqual(<<1,2>>, avro_fixed:get_value(Value)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
