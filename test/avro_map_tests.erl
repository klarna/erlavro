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
%%%-------------------------------------------------------------------
-module(avro_map_tests).

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