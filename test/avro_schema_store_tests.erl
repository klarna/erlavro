%% coding: latin-1
%%%-------------------------------------------------------------------
%%% Copyright (c) 2013-2018 Klarna AB
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
-module(avro_schema_store_tests).

-include_lib("eunit/include/eunit.hrl").

get_all_types_test() ->
  TestFun =
    fun(Store) ->
      Store1 =
        avro_schema_store:add_type("assigned.name", test_record(), Store),
      ?assertEqual(
        [ avro_fixed:type("MyFixed", 16, [{namespace, "com.klarna.test.bix"}])
        , avro_enum:type("MyEnum", ["A"], [{namespace, "another.name"}])
        , flat_test_record()
        , flat_sub_record()
        ], avro_schema_store:get_all_types(Store1)),
      ok = avro_schema_store:close(Store)
    end,
  ok = TestFun(avro_schema_store:new([{name, ?MODULE}])),
  ok = TestFun(avro_schema_store:new([dict])),
  ok = TestFun(avro_schema_store:new([map])).

is_store_test() ->
  ?assertNot(avro_schema_store:is_store(<<"json">>)),
  ?assert(avro_schema_store:is_store(avro_schema_store:new([dict]))),
  ?assert(avro_schema_store:is_store(avro_schema_store:new([map]))),
  ?assert(avro_schema_store:is_store(avro_schema_store:new([]))).

ensure_store_test() ->
  Pass = try
          avro_schema_store:ensure_store("not a store")
        catch
            _:_  -> throw
        end,
  ?assertEqual(throw, Pass),
  ?assertEqual(1, avro_schema_store:ensure_store(1)),
  ?assertEqual(atom, avro_schema_store:ensure_store(atom)),
  ?assertEqual({dict, dict:new()},
               avro_schema_store:ensure_store({dict, dict:new()})),
  ?assertEqual(#{}, avro_schema_store:ensure_store(#{})).

flatten_type_test() ->
  Type = avro_array:type(test_record()),
  Expected =
    { avro_array:type("com.klarna.test.bix.TestRecord")
    , [ flat_test_record()
      , flat_sub_record()
      , avro_enum:type("MyEnum", ["A"], [{namespace, "another.name"}])
      , avro_fixed:type("MyFixed", 16, [{namespace, "com.klarna.test.bix"}])
      ]
    },
  ?assertEqual(Expected, avro:flatten_type(Type)).

add_type_test() ->
  TestFun =
    fun(Store) ->
        Store1 = avro_schema_store:add_type(test_record(), Store),
        ?assertEqual({ok, flat_test_record()},
                     lookup("com.klarna.test.bix.TestRecord", Store1)),
        ?assertEqual({ok, flat_test_record()},
                     lookup("com.klarna.test.bix.TestRecordAlias1", Store1)),
        ?assertEqual({ok, flat_sub_record()},
                     lookup("com.klarna.test.bix.TestSubRecord", Store1)),
        ?assertEqual({ok, flat_sub_record()},
                     lookup("com.klarna.test.bix.TestSubRecordAlias", Store1)),
        ok = avro_schema_store:close(Store1)
    end,
  ok = TestFun(avro_schema_store:new()),
  ok = TestFun(avro_schema_store:new([dict])),
  ok = TestFun(avro_schema_store:new([map])).

lookup(Name, Store) ->
  avro_schema_store:lookup_type(Name, Store).

import_test() ->
  AvscFile = test_data("interop.avsc"),
  Store = avro_schema_store:new([{name, ?MODULE}], [AvscFile]),
  ?assertEqual(?MODULE, Store),
  ets:delete(Store),
  ok.

import_unnamed_test() ->
  UnionName = "com.klarna.test.union",
  AvscFile = test_data(UnionName ++ ".avsc"),
  UnionType = avro_union:type([null, long]),
  UnionJSON = avro_json_encoder:encode_type(UnionType),
  ok = file:write_file(AvscFile, UnionJSON),
  try
    Store = avro_schema_store:new([], [AvscFile]),
    ?assertException(error, {unnamed_type, UnionType},
                     avro_schema_store:import_schema_json(UnionJSON, Store)),
    ?assertEqual({ok, UnionType},
                 avro_schema_store:lookup_type(UnionName, Store))
  after
    file:delete(AvscFile)
  end.

name_clash_test() ->
  Name = <<"com.klarna.test.union">>,
  Type = avro_union:type([null, long]),
  AnotherType = avro_primitive:string_type(),
  Store = avro_schema_store:new([]),
  %% ok to add the type
  Store = avro_schema_store:add_type(Name, Type, Store),
  %% ok to add the exact type again
  Store = avro_schema_store:add_type(Name, Type, Store),
  ?assertException(error, {name_clash, Name, AnotherType, Type},
                   avro_schema_store:add_type(Name, AnotherType, Store)).

import_failure_test() ->
  Filename = "no-such-file",
  ?assertException(error, {failed_to_read_schema_file, Filename, enoent},
                   avro_schema_store:import_file(Filename, ignore)).

expand_type_test() ->
  AvscFile = test_data("interop.avsc"),
  Store = avro_schema_store:new([], [AvscFile]),
  {ok, FlatType} =
    avro_schema_store:lookup_type("org.apache.avro.Interop", Store),
  {ok, TruthJSON} = file:read_file(AvscFile),
  TruthType = avro_json_decoder:decode_schema(TruthJSON),
  Type = avro:expand_type("org.apache.avro.Interop", Store),
  %% compare decoded type instead of JSON schema because
  %% the order of JSON object fields lacks deterministic
  ?assertEqual(TruthType, Type),
  %% also try to expand a flattened wrapper type, which should
  %% have the exact same effect as expanding from its fullname
  ?assertEqual(TruthType, avro:expand_type(FlatType, Store)),
  ok.

%% @private
sub_record() ->
  avro_record:type(
    "TestSubRecord",
    [ define_field("sub_field1", boolean)
    , define_field("sub_field2", avro_enum:type("MyEnum", ["A"],
                                                [{namespace, "another.name"}]))
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestSubRecordAlias"]}
    ]).

%% @private
flat_sub_record() ->
  avro_record:type(
    "TestSubRecord",
    [ define_field("sub_field1", boolean),
      define_field("sub_field2", "another.name.MyEnum") ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestSubRecordAlias"]}
    ]).

%% @private
test_record() ->
  avro_record:type(
    "TestRecord",
    [ %% simple type
      define_field("field1", int)
      %% huge nested type
    , define_field("field2",
                   avro_array:type(
                     avro_union:type(
                       [ string
                       , sub_record()
                       , avro_fixed:type("MyFixed", 16,
                                         [{namespace, "com.klarna.test.bix"}])
                       ])))
      %% named type without explicit namespace
    , define_field("field3", "com.klarna.test.bix.SomeType")
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestRecordAlias1", "TestRecordAlias2"]}
    ]
  ).

%% @private
flat_test_record() ->
  avro_record:type(
    "TestRecord",
    [ %% simple type
      define_field("field1", int)
      %% huge nested type
    , define_field("field2", avro_array:type(
                               avro_union:type(
                                 [ string
                                 , "com.klarna.test.bix.TestSubRecord"
                                 , "com.klarna.test.bix.MyFixed"
                                 ])))
      %% named type without explicit namespace
    , define_field("field3", "com.klarna.test.bix.SomeType")
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestRecordAlias1", "TestRecordAlias2"]}
    ]).

test_data(FileName) ->
  filename:join([code:lib_dir(erlavro, test), "data", FileName]).

define_field(Name, Type) -> avro_record:define_field(Name, Type).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
