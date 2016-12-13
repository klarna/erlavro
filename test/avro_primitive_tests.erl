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
-module(avro_primitive_tests).

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").

boolean_cast_uncast_test() ->
  {ok, AvroBoolF = #avro_value{type = #avro_primitive_type{name = ?AVRO_BOOLEAN}, data = false}} =
    avro:cast(?AVRO_BOOLEAN, false),
  {ok, BoolF} = avro:uncast(AvroBoolF),
  ?assertEqual(BoolF, false),
  {ok, AvroBoolT = #avro_value{type = #avro_primitive_type{name = ?AVRO_BOOLEAN}, data = true}} =
    avro:cast(?AVRO_BOOLEAN, true),
  {ok, BoolT} = avro:uncast(AvroBoolT),
  ?assertEqual(BoolT, true).

null_cast_uncast_test() ->
  {ok, AvroValue = #avro_value{type = #avro_primitive_type{name = ?AVRO_NULL}, data = null}} =
    avro:cast(?AVRO_NULL, null),
  {ok, Null} = avro:uncast(AvroValue),
  ?assertEqual(Null, null).

int_cast_uncast_test() ->
  {ok, AvroValue = #avro_value{type = #avro_primitive_type{name = ?AVRO_INT}, data = 17}} =
    avro:cast(?AVRO_INT, 17),
  {ok, Value} = avro:uncast(AvroValue),
  ?assertEqual(Value, 17).

long_cast_uncast_test() ->
  {ok, AvroValue = #avro_value{type = #avro_primitive_type{name = ?AVRO_LONG}, data = 9023372036854775807}} =
    avro:cast(?AVRO_LONG, 9023372036854775807),
  {ok, Value} = avro:uncast(AvroValue),
  ?assertEqual(Value, 9023372036854775807).

float_cast_uncast_test() ->
  {ok, AvroValue = #avro_value{type = #avro_primitive_type{name = ?AVRO_FLOAT}, data = 17.17}} =
    avro:cast(?AVRO_FLOAT, 17.17),
  {ok, Value} = avro:uncast(AvroValue),
  ?assertEqual(Value, 17.17).

double_cast_uncast_test() ->
  {ok, AvroValue = #avro_value{type = #avro_primitive_type{name = ?AVRO_DOUBLE}, data = 17.17}} =
    avro:cast(?AVRO_DOUBLE, 17.17),
  {ok, Value} = avro:uncast(AvroValue),
  ?assertEqual(Value, 17.17).

bytes_cast_uncast_test() ->
  {ok, AvroValue = #avro_value{type = #avro_primitive_type{name = ?AVRO_BYTES}, data = <<"b">>}} =
    avro:cast(?AVRO_BYTES, <<"b">>),
  {ok, Value} = avro:uncast(AvroValue),
  ?assertEqual(Value, <<"b">>).

string_cast_uncast_test() ->
  {ok, AvroValue = #avro_value{type = #avro_primitive_type{name = ?AVRO_STRING}, data = "s"}} =
    avro:cast(?AVRO_STRING, "s"),
  {ok, Value} = avro:uncast(AvroValue),
  ?assertEqual(Value, "s").