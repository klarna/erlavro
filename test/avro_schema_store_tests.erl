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
-module(avro_schema_store_tests).

-include_lib("eunit/include/eunit.hrl").

extract_from_primitive_type_test() ->
  Type = avro_primitive:int_type(),
  ?assertEqual({Type, []}, avro_schema_store:extract_children_types(Type)).

extract_from_nested_primitive_type_test() ->
  Type = avro_array:type(avro_primitive:int_type()),
  ?assertEqual({Type, []}, avro_schema_store:extract_children_types(Type)).

extract_from_named_type_test() ->
  Type = avro_array:type("com.klarna.test.bix.SomeType"),
  ?assertEqual({Type, []}, avro_schema_store:extract_children_types(Type)).

extract_from_extractable_type_test() ->
  Type = avro_array:type(test_record()),
  Expected = { avro_array:type("com.klarna.test.bix.TestRecord")
    , [ extracted_test_record()
      , extracted_sub_record()
      , avro_enum:type("MyEnum", ["A"],
        [{namespace, "another.name"}])
      , avro_fixed:type("MyFixed", 16,
        [{namespace, "com.klarna.test.bix"}])
    ]
  },
  ?assertEqual(Expected, avro_schema_store:extract_children_types(Type)).

add_type_test() ->
  Store = avro_schema_store:new(),
  Store1 = avro_schema_store:add_type(test_record(), Store),
  ?assertEqual({ok, extracted_test_record()},
    lookup("com.klarna.test.bix.TestRecord", Store1)),
  ?assertEqual({ok, extracted_test_record()},
    lookup("com.klarna.test.bix.TestRecordAlias1", Store1)),
  ?assertEqual({ok, extracted_sub_record()},
    lookup("com.klarna.test.bix.TestSubRecord", Store1)),
  ?assertEqual({ok, extracted_sub_record()},
    lookup("com.klarna.test.bix.TestSubRecordAlias", Store1)).

lookup(Name, Store) ->
  avro_schema_store:lookup_type(Name, Store).

import_test() ->
  PrivDir = priv_dir(),
  AvscFile = filename:join([PrivDir, "interop.avsc"]),
  Store = avro_schema_store:new([], [AvscFile]),
  ets:delete(Store),
  ok.

priv_dir() ->
  case filelib:is_dir(filename:join(["..", priv])) of
    true -> filename:join(["..", priv]);
    _    -> "./priv"
  end.

expand_type_test() ->
  PrivDir = priv_dir(),
  AvscFile = filename:join([PrivDir, "interop.avsc"]),
  Store = avro_schema_store:new([], [AvscFile]),
  {ok, TruthJSON} = file:read_file(AvscFile),
  TruthType = avro_json_decoder:decode_schema(TruthJSON),
  Type = avro_schema_store:expand_type("org.apache.avro.Interop", Store),
  %% compare decoded type instead of JSON schema because
  %% the order of JSON object fields lacks deterministic
  ?assertEqual(TruthType, Type),
  ok.


%% @private
sub_record() ->
  avro_record:type(
    "TestSubRecord",
    [ avro_record:define_field("sub_field1", avro_primitive:boolean_type())
      , avro_record:define_field("sub_field2",
      avro_enum:type("MyEnum", ["A"],
        [{namespace, "another.name"}]))
    ],
    [ {namespace, "com.klarna.test.bix"}
      , {doc, "Some doc"}
      , {aliases, ["TestSubRecordAlias"]}
    ]).

%% @private
extracted_sub_record() ->
  avro_record:type(
    "TestSubRecord",
    [ avro_record:define_field(
      "sub_field1", avro_primitive:boolean_type())
      , avro_record:define_field(
      "sub_field2", "another.name.MyEnum")
    ],
    [ {namespace, "com.klarna.test.bix"}
      , {doc, "Some doc"}
      , {aliases, ["TestSubRecordAlias"]}
    ]).

%% @private
test_record() ->
  avro_record:type(
    "TestRecord",
    [ %% simple type
      avro_record:define_field("field1", avro_primitive:int_type())
      %% huge nested type
      , avro_record:define_field(
      "field2",
      avro_array:type(
        avro_union:type(
          [ avro_primitive:string_type()
            , sub_record()
            , avro_fixed:type("MyFixed", 16,
            [{namespace, "com.klarna.test.bix"}])
          ])))
      %% named type without explicit namespace
      , avro_record:define_field("field3", "com.klarna.test.bix.SomeType")
    ],
    [ {namespace, "com.klarna.test.bix"}
      , {doc, "Some doc"}
      , {aliases, ["TestRecordAlias1", "TestRecordAlias2"]}
    ]
  ).

%% @private
extracted_test_record() ->
  avro_record:type(
    "TestRecord",
    [ %% simple type
      avro_record:define_field(
        "field1", avro_primitive:int_type())
      %% huge nested type
      , avro_record:define_field(
      "field2", avro_array:type(
        avro_union:type(
          [ avro_primitive:string_type()
            , "com.klarna.test.bix.TestSubRecord"
            , "com.klarna.test.bix.MyFixed"
          ])))
      %% named type without explicit namespace
      , avro_record:define_field(
      "field3", "com.klarna.test.bix.SomeType")
    ],
    [ {namespace, "com.klarna.test.bix"}
      , {doc, "Some doc"}
      , {aliases, ["TestRecordAlias1", "TestRecordAlias2"]}
    ]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
