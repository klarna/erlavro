%% coding: latin-1
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
-module(avro_union_tests).

-include_lib("eunit/include/eunit.hrl").
-include("avro_internal.hrl").

get_record(N) ->
  Name = "R" ++ integer_to_list(N),
  avro_record:type(Name,
                   [avro_record:define_field("F", avro_primitive:int_type())],
                   [{namespace, "com.klarna.test"}]).
make_name(N) ->
  "com.klarna.test.R" ++ integer_to_list(N).

tiny_union() ->
  avro_union:type([get_record(N) || N <- lists:seq(1,5)]).

big_union() ->
  avro_union:type([get_record(N) || N <- lists:seq(1,200)]).

new_direct_test() ->
  Type = avro_union:type([avro_primitive:int_type(),
                          avro_primitive:string_type()]),
  NewVersion = avro_union:new(Type, "Foo"),
  DirectVersion = avro_union:new_direct(Type, avro_primitive:string("Foo")),
  ?assertEqual(NewVersion, DirectVersion).

lookup_child_type_from_tiny_union_test() ->
  Type = tiny_union(),
  ExpectedRec1 = get_record(1),
  ?assertEqual({ok, ExpectedRec1},
               avro_union:lookup_child_type(Type, "com.klarna.test.R1")),
  ?assertEqual({ok, ExpectedRec1},
               avro_union:lookup_child_type(Type, 0)),
  ExpectedRec2 = get_record(2),
  ?assertEqual({ok, ExpectedRec2},
               avro_union:lookup_child_type(Type, "com.klarna.test.R2")),
  ?assertEqual({ok, ExpectedRec2},
               avro_union:lookup_child_type(Type, 1)).


lookup_child_type_from_big_union_test() ->
  Type = big_union(),
  ExpectedRec = get_record(100),
  ?assertEqual({ok, ExpectedRec},
               avro_union:lookup_child_type(Type, "com.klarna.test.R100")),
  ?assertEqual({ok, ExpectedRec},
               avro_union:lookup_child_type(Type, 99)).

to_term_test() ->
  Type = avro_union:type([avro_primitive:null_type(),
                          avro_primitive:int_type()]),
  Value1 = avro_union:new(Type, null),
  Value2 = avro_union:new(Type, 1),
  ?assertEqual(null, avro:to_term(Value1)),
  ?assertEqual(1,    avro:to_term(Value2)).

empty_unon_not_allowed_test() ->
  ?assertException(error, <<"union should have at least one member type">>,
                   avro_union:type([])).

cast_test() ->
  Type = avro_union:type([avro_primitive:null_type(),
                          avro_primitive:long_type()]),
  ?assertMatch({ok, #avro_value{}}, avro_union:cast(Type, {long, 1})),
  ?assertMatch({ok, #avro_value{}}, avro_union:cast(Type, null)),
  ?assertEqual({error, type_mismatch}, avro_union:cast(Type, "str")),
  ?assertException(error, type_mismatch, avro_union:new(Type, "str")).

unknown_tag_cast_test() ->
  Type = avro_union:type([avro_primitive:null_type(),
                          avro_primitive:long_type()]),
  ?assertException(error, {unknown_tag, Type, 2},
                   avro_union:cast(Type, {2, "s"})).

unknown_tag_encode_test() ->
  Type = avro_union:type([avro_primitive:null_type(),
                          avro_primitive:long_type()]),
  EncodeFun = fun(_MemberType, InputValue, MemberIdInteger) ->
                  {encoded, MemberIdInteger, InputValue}
              end,
  ?assertEqual({encoded, 0, null},
               avro_union:encode(Type, {0, null}, EncodeFun)),
  ?assertEqual({encoded, 0, null},
               avro_union:encode(Type, {null, null}, EncodeFun)),
  ?assertEqual({encoded, 1, 42},
               avro_union:encode(Type, {long, 42}, EncodeFun)),
  ?assertException(error, {unknown_tag, Type, 2},
                   avro_union:encode(Type, {2, "s"}, EncodeFun)),
  ?assertException(error, {unknown_tag, Type, "int"},
                   avro_union:encode(Type, {"int", 2}, EncodeFun)).

loop_over_encode_test() ->
  Type = avro_union:type([avro_primitive:null_type(),
                          avro_primitive:long_type()]),
  EncodeFun = fun(_MemberType, InputValue, MemberIdInteger) ->
                  case MemberIdInteger of
                    0 -> null = InputValue;
                    1 -> 42   = InputValue
                  end,
                  {encoded, MemberIdInteger, InputValue}
              end,
  ?assertEqual({encoded, 0, null},
               avro_union:encode(Type, null, EncodeFun)),
  ?assertEqual({encoded, 1, 42},
               avro_union:encode(Type, 42, EncodeFun)),
  ?assertException(error, {failed_to_encode_union, Type, "s"},
                   avro_union:encode(Type, "s", EncodeFun)).

big_union_of_names_test() ->
  Type = avro_union:type([make_name(N) || N <- lists:seq(1,200)]),
  EncodeFun = fun(_MemberType, InputValue, MemberIdInteger) ->
                  {encoded, MemberIdInteger, InputValue}
              end,
  ?assertEqual({encoded, 0, value},
               avro_union:encode(Type, {<<"com.klarna.test.R1">>, value},
                                 EncodeFun)),
  ?assertEqual({encoded, 199, value},
               avro_union:encode(Type, {<<"com.klarna.test.R200">>, value},
                                 EncodeFun)),
  ?assertException(error, {unknown_tag, Type, <<"com.klarna.test.x">>},
                   avro_union:encode(Type, {<<"com.klarna.test.x">>, value},
                                     EncodeFun)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
