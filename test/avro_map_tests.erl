%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Dec 2016 10:53 AM
%%%-------------------------------------------------------------------
-module(avro_map_tests).
-author("tihon").

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").

cast_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  Value = avro_map:cast(Type, [{"v1", 1}, {"v2", 2}, {"v3", 3}]),
  Expected = ?AVRO_VALUE(Type, dict:from_list(
    [{"v1", avro_primitive:int(1)}
      ,{"v2", avro_primitive:int(2)}
      ,{"v3", avro_primitive:int(3)}])),
  ?assertEqual({ok, Expected}, Value).

to_dict_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  Value = avro_map:new(Type, [{"v1", 1}, {"v2", 2}, {"v3", 3}]),
  Expected = dict:from_list(
    [{"v1", avro_primitive:int(1)}
      ,{"v2", avro_primitive:int(2)}
      ,{"v3", avro_primitive:int(3)}]),
  ?assertEqual(Expected,
    avro_map:to_dict(Value)).

to_term_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  ExpectedMappings = [{"v1", 1}, {"v2", 2}, {"v3", 3}],
  Value = avro_map:new(Type, ExpectedMappings),
  Mappings = avro:to_term(Value),
  ?assertEqual(ExpectedMappings, lists:keysort(1, Mappings)).