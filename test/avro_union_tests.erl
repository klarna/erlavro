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

get_record(N) ->
  Name = "R" ++ integer_to_list(N),
  avro_record:type(Name,
    [avro_record:define_field("F", avro_primitive:int_type())],
    [{namespace, "com.klarna.test.bix"}]).
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
    avro_union:lookup_child_type(Type, "com.klarna.test.bix.R1")),
  ?assertEqual({ok, ExpectedRec1},
    avro_union:lookup_child_type(Type, 0)),
  ExpectedRec2 = get_record(2),
  ?assertEqual({ok, ExpectedRec2},
    avro_union:lookup_child_type(Type, "com.klarna.test.bix.R2")),
  ?assertEqual({ok, ExpectedRec2},
    avro_union:lookup_child_type(Type, 1)).


lookup_child_type_from_big_union_test() ->
  Type = big_union(),
  ExpectedRec = get_record(100),
  ?assertEqual({ok, ExpectedRec},
    avro_union:lookup_child_type(Type, "com.klarna.test.bix.R100")),
  ?assertEqual({ok, ExpectedRec},
    avro_union:lookup_child_type(Type, 99)).

get_child_type_index_test() ->
  Type1 = tiny_union(),
  Type2 = big_union(),
  ?assertEqual({ok, 2},
    avro_union:get_child_type_index(Type1, "com.klarna.test.bix.R3")),
  ?assertEqual({ok, 42},
    avro_union:get_child_type_index(Type2, "com.klarna.test.bix.R43")).

to_term_test() ->
  Type = avro_union:type([avro_primitive:null_type(),
                          avro_primitive:int_type()]),
  Value1 = avro_union:new(Type, null),
  Value2 = avro_union:new(Type, 1),
  ?assertEqual(null, avro:to_term(Value1)),
  ?assertEqual(1,    avro:to_term(Value2)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
