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
%%%-------------------------------------------------------------------
-module(avro_array_tests).
-author("tihon").

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").

to_term_test() ->
  ArrayType = avro_array:type(avro_primitive:int_type()),
  {ok, Array} = avro_array:cast(ArrayType, [1, 2]),
  ?assertEqual([1, 2], avro:to_term(Array)).

cast_test() ->
  ArrayType = avro_array:type(avro_primitive:string_type()),
  {ok, Array} = avro_array:cast(ArrayType, ["a", "b"]),
  ?assertEqual(ArrayType, ?AVRO_VALUE_TYPE(Array)),
  ?assertEqual([avro_primitive:string("a"), avro_primitive:string("b")],
    ?AVRO_VALUE_DATA(Array)).

prepend_test() ->
  ArrayType = avro_array:type(avro_primitive:string_type()),
  {ok, Array} = avro_array:cast(ArrayType, ["b", "a"]),
  NewArray = avro_array:prepend(["d", "c"], Array),
  ExpectedValues = [avro_primitive:string(S) || S <- ["d", "c", "b", "a"]],
  ?assertEqual(ExpectedValues, ?AVRO_VALUE_DATA(NewArray)).

new_direct_test() ->
  Type = avro_array:type(avro_primitive:int_type()),
  NewVersion = avro_array:new(Type, [1,2,3]),
  DirectVersion = avro_array:new_direct(Type,
    [ avro_primitive:int(1)
      , avro_primitive:int(2)
      , avro_primitive:int(3)
    ]),
  ?assertEqual(NewVersion, DirectVersion).