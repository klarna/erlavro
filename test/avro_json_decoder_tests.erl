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
-module(avro_json_decoder_tests).

-import(avro_json_decoder, [ parse/5
                           , parse_schema/3
                           , parse_value/3
                           ]).

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(JSON_OBJ(__FIELDS__), {__FIELDS__}).

parse_primitive_type_name_test() ->
  %% Check that primitive types specified by their names are parsed correctly
  ?assertEqual(avro_primitive:int_type(),
               parse_schema(<<"int">>, "foobar", none)).

parse_primitive_type_object_test() ->
  %% Check that primitive types specified by type objects are parsed correctly
  Schema = ?JSON_OBJ([{<<"type">>, <<"int">>}]),
  ?assertEqual(avro_primitive:int_type(),
               parse_schema(Schema, "foobar", none)).

parse_record_type_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>,      <<"record">>}
    , {<<"name">>,      <<"TestRecord">>}
    , {<<"namespace">>, <<"name.space">>}
    , {<<"fields">>,    []}
    ]),
  Record = parse_schema(Schema, "", none),
  ?assertEqual(avro_record:type("TestRecord", [], [{namespace, "name.space"}]),
               Record).

parse_record_type_with_default_values_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>, <<"record">>}
    , {<<"name">>, <<"TestRecord">>}
    , {<<"namespace">>, <<"name.space">>}
    , {<<"fields">>, [ ?JSON_OBJ([ {<<"name">>, <<"string_field">>}
                                 , {<<"type">>, <<"string">>}
                                 , {<<"default">>, <<"FOOBAR">>}
                                 ])
                     , ?JSON_OBJ([ {<<"name">>, <<"union_field">>}
                                 , {<<"type">>, [<<"boolean">>, <<"int">>]}
                                 , {<<"default">>, true}
                                 ])
                     ]}
    ]),
  Record = parse_schema(Schema, "", none),
  ExpectedUnion = avro_union:type([ avro_primitive:boolean_type()
                                  , avro_primitive:int_type()
                                  ]),
  Expected = avro_record:type(
    "TestRecord",
    [ avro_record:define_field(
        "string_field",
        avro_primitive:string_type(),
        [{default, avro_primitive:string("FOOBAR")}])
    , avro_record:define_field(
        "union_field", ExpectedUnion,
        [{default, avro_union:new(ExpectedUnion,
          avro_primitive:boolean(true))}])
    ],
    [{namespace, "name.space"}]),
  ?assertEqual(Expected, Record).

parse_record_type_with_enclosing_namespace_test() ->
  Schema= ?JSON_OBJ(
    [ {<<"type">>,   <<"record">>}
    , {<<"name">>,   <<"TestRecord">>}
    , {<<"fields">>, []}
    ]),
  Record = parse_schema(Schema, "name.space", none),
  ?assertEqual("name.space.TestRecord", avro:get_type_fullname(Record)).

parse_union_type_test() ->
  Schema = [ <<"int">>
           , <<"string">>
           , <<"typename">>
           ],
  Union = parse_schema(Schema, "name.space", none),
  ?assertEqual(avro_union:type([avro_primitive:int_type(),
                                avro_primitive:string_type(),
                                "name.space.typename"]), Union).

parse_enum_type_full_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>,      <<"enum">>}
    , {<<"name">>,      <<"TestEnum">>}
    , {<<"namespace">>, <<"name.space">>}
    , {<<"symbols">>,   [<<"A">>, <<"B">>, <<"C">>]}
    , {<<"doc">>,       <<"descr">>}
    , {<<"aliases">>,   [<<"EnumAlias">>, <<"EnumAlias2">>]}
    ]),
  Enum = parse_schema(Schema, "enc.losing", none),
  ExpectedType = avro_enum:type(
    "TestEnum",
    ["A", "B", "C"],
    [ {namespace,    "name.space"}
    , {doc,          "descr"}
    , {aliases,      ["EnumAlias", "EnumAlias2"]}
    , {enclosing_ns, "enc.losing"}
    ]),
  ?assertEqual(ExpectedType, Enum).

parse_enum_type_short_test() ->
  %% Only required fields are present
  Schema = ?JSON_OBJ(
    [ {<<"type">>,    <<"enum">>}
    , {<<"name">>,    <<"TestEnum">>}
    , {<<"symbols">>, [<<"A">>, <<"B">>, <<"C">>]}
    ]),
  Enum = parse_schema(Schema, "enc.losing", none),
  ExpectedType = avro_enum:type(
    "TestEnum",
    ["A", "B", "C"],
    [ {namespace,    ""}
    , {doc,          ""}
    , {aliases,      []}
    , {enclosing_ns, "enc.losing"}
    ]),
  ?assertEqual(ExpectedType, Enum).

parse_map_type_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>,   <<"map">>}
    , {<<"values">>, <<"int">>}
    ]),
  Map = parse_schema(Schema, "enc.losing", none),
  ExpectedType = avro_map:type(avro_primitive:int_type()),
  ?assertEqual(ExpectedType, Map).

parse_fixed_type_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>,      <<"fixed">>}
    , {<<"size">>,      2}
    , {<<"name">>,      <<"FooBar">>}
    , {<<"aliases">>,   [<<"Alias1">>, <<"Alias2">>]}
    , {<<"namespace">>, <<"name.space">>}
    ]),
  Fixed = parse_schema(Schema, "enc.losing", none),
  ExpectedType = avro_fixed:type("FooBar", 2,
                                 [ {namespace, "name.space"}
                                 , {aliases, ["Alias1", "Alias2"]}
                                 , {enclosing_ns, "enc.losing"}]),
  ?assertEqual(ExpectedType, Fixed).

parse_bytes_value_test() ->
  Json = <<"\\u0010\\u0000\\u00FF">>,
  Value = parse_value(Json, avro_primitive:bytes_type(), none),
  ?assertEqual(avro_primitive:bytes(<<16,0,255>>), Value).

parse_record_value_test() ->
  %% This test also tests parsing other types inside the record
  TestRecord = get_test_record(),
  Json = ?JSON_OBJ(
    [ {<<"invno">>, 100}
    , {<<"array">>, [<<"ACTIVE">>, <<"CLOSED">>]}
    , {<<"union">>, ?JSON_OBJ([{<<"boolean">>, true}])}
    ]),
  Value = parse_value(Json, TestRecord, none),
  ?assertEqual(avro_primitive:long(100), avro_record:get_value("invno", Value)),
  ?assertEqual(avro_array:new(avro_record:get_field_type("array", TestRecord),
                              [ avro_primitive:string("ACTIVE")
                              , avro_primitive:string("CLOSED")]),
               avro_record:get_value("array", Value)),
  ?assertEqual(avro_primitive:boolean(true),
               avro_union:get_value(avro_record:get_value("union", Value))).

parse_record_value_missing_field_test() ->
  %% This test also tests parsing other types inside the record
  TestRecord = get_test_record(),
  Json = ?JSON_OBJ([ {<<"invno">>, 100}
                   , {<<"union">>, ?JSON_OBJ([{<<"boolean">>, true}])}
                   ]),
  %% parse_value(Json, TestRecord, none),
  %% ok.
  ?assertError({required_field_missed, "array"},
               parse_value(Json, TestRecord, none)).

parse_record_value_unknown_field_test() ->
  %% This test also tests parsing other types inside the record
  TestRecord = get_test_record(),
  Json = ?JSON_OBJ([ {<<"invno">>, 100}
                   , {<<"array">>, [<<"ACTIVE">>, <<"CLOSED">>]}
                   , {<<"union">>, ?JSON_OBJ([{<<"boolean">>, true}])}
                   , {<<"unknown_field">>, 1}
                   ]),
  ?assertError({unknown_field, "unknown_field"},
               parse_value(Json, TestRecord, none)).

parse_union_value_primitive_test() ->
  Type = avro_union:type([ avro_primitive:null_type()
                         , avro_primitive:string_type()]),
  Json = ?JSON_OBJ([{<<"string">>, <<"str">>}]),
  Value = parse_value(Json, Type, none),
  ?assertEqual(avro_primitive:string("str"), avro_union:get_value(Value)).

parse_union_value_null_test() ->
  Type = avro_union:type([ avro_primitive:null_type()
                         , avro_primitive:string_type()]),
  Json = null,
  Value = parse_value(Json, Type, none),
  ?assertEqual(avro_primitive:null(), avro_union:get_value(Value)).

parse_union_value_fail_test() ->
  Type = avro_union:type([ avro_primitive:null_type()
                         , avro_primitive:string_type()]),
  Json = ?JSON_OBJ([{<<"boolean">>, true}]),
  ?assertError(unknown_type_of_union_value, parse_value(Json, Type, none)).

parse_enum_value_test() ->
  Type = avro_enum:type("MyEnum", ["A", "B", "C"]),
  Json = <<"B">>,
  Expected = avro_enum:new(Type, "B"),
  ?assertEqual(Expected, parse_value(Json, Type, none)).

parse_map_value_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  Json = ?JSON_OBJ([ {<<"v1">>, 1}, {<<"v2">>, 2} ]),
  Expected = avro_map:new(Type, [{"v1", 1}, {"v2", 2}]),
  ?assertEqual(Expected, parse_value(Json, Type, none)).

parse_fixed_value_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Json = <<"\\u0001\\u007f">>,
  Expected = avro_fixed:new(Type, <<1,127>>),
  ?assertEqual(Expected, parse_value(Json, Type, none)).

parse_value_with_extract_type_fun_test() ->
  Hook = avro_decoder_hooks:pretty_print_hist(),
  ExtractTypeFun = fun("name.space.Test") -> get_test_record() end,
  Schema = ?JSON_OBJ([ {<<"type">>, <<"array">>}
                     , {<<"items">>, <<"Test">>}
                     ]),
  ValueJson = [?JSON_OBJ([ {<<"invno">>, 100}
                         , {<<"array">>, [<<"ACTIVE">>, <<"CLOSED">>]}
                         , {<<"union">>, ?JSON_OBJ([{<<"boolean">>, true}])}
                         ])],
  Type = parse_schema(Schema, "name.space", ExtractTypeFun),
  ExpectedType = avro_array:type("name.space.Test"),
  ?assertEqual(ExpectedType, Type),
  Value = parse(ValueJson, Type, ExtractTypeFun, true, Hook),
  [Rec] = avro_array:get(Value),
  ?assertEqual("name.space.Test",
               avro:get_type_fullname(?AVRO_VALUE_TYPE(Rec))),
  ?assertEqual(avro_primitive:long(100), avro_record:get_value("invno", Rec)).

%% @private
get_test_record() ->
  F = fun(Name, Type) -> avro_record:define_field(Name, Type) end,
  Fields = [ F("invno", avro_primitive:long_type())
           , F("array", avro_array:type(avro_primitive:string_type()))
           , F("union", avro_union:type([ avro_primitive:null_type()
                                        , avro_primitive:int_type()
                                        , avro_primitive:boolean_type()
                                        ]))
           ],
  avro_record:type("Test", Fields, [{namespace, "name.space"}]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
