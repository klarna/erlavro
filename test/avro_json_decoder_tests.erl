%% coding: latin-1
%%%-------------------------------------------------------------------
%%% Copyright (c) 2013-2024 Klarna AB
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

-import(avro_json_decoder, [ parse/4
                           , parse_schema/1
                           ]).

-include_lib("erlavro/include/avro_internal.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(JSON_OBJ(__FIELDS__), {__FIELDS__}).

parse_primitive_type_name_test() ->
  %% Check that primitive types specified by their names are parsed correctly
  ?assertEqual(avro_primitive:int_type(),
               parse_schema(<<"int">>)).

parse_primitive_type_object_test() ->
  %% Check that primitive types specified by type objects are parsed correctly
  Type = ?JSON_OBJ([{<<"type">>, <<"int">>}]),
  ?assertEqual(avro_primitive:int_type(),
               parse_schema(Type)).

parse_custom_prop_test() ->
  Type = ?JSON_OBJ([ {<<"type">>, <<"int">>}
                   , {<<"logicalType">>, <<"my-type">>}
                   , {<<"foo">>, 42}
                   , {<<"bar">>, 3.14}
                   ]),
  ?assertEqual(avro_primitive:type(int, [ {<<"logicalType">>, <<"my-type">>}
                                        , {<<"foo">>, 42}
                                        , {<<"bar">>, 3.14}
                                        ]),
               parse_schema(Type)).

parse_record_type_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>,      <<"record">>}
    , {<<"name">>,      <<"TestRecord">>}
    , {<<"namespace">>, <<"name.space">>}
    , {<<"fields">>,    []}
    ]),
  Record = parse_schema(Schema),
  Expected = avro_record:type("TestRecord", [], [{namespace, "name.space"}]),
  ?assertEqual(Expected, Record).

allow_null_as_string_for_default_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>, <<"record">>}
    , {<<"name">>, <<"TestRecord">>}
    , {<<"namespace">>, <<"name.space">>}
    , {<<"fields">>, [ ?JSON_OBJ([ {<<"name">>, <<"union_null_field">>}
                                 , {<<"type">>, [<<"null">>, <<"int">>]}
                                 , {<<"default">>, <<"null">>}
                                 , {<<"order">>, <<"ignore">>}
                                 ])

                     ]}
    ]),
  JSON = iolist_to_binary(jsone:encode(Schema, [native_utf8])),
  ExpectedUnion = avro_union:type([ avro_primitive:null_type()
                                  , avro_primitive:int_type()
                                  ]),
  Expected = avro_record:type(
    "TestRecord",
    [ avro_record:define_field(
        "union_null_field", ExpectedUnion,
        [{default, null},
         {order, ignore}
        ])
    ],
    [{namespace, "name.space"}]),
  ?assertEqual(Expected, avro:decode_schema(JSON, [])).

decode_with_default_values_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>, <<"record">>}
    , {<<"name">>, <<"TestRecord">>}
    , {<<"namespace">>, <<"name.space">>}
    , {<<"fields">>, [ ?JSON_OBJ([ {<<"name">>, <<"string_field">>}
                                 , {<<"type">>, <<"string">>}
                                 , {<<"default">>, <<"FOOBAR">>}
                                 , {<<"order">>, <<"descending">>}
                                 ])
                     , ?JSON_OBJ([ {<<"name">>, <<"union_field">>}
                                 , {<<"type">>, [<<"boolean">>, <<"int">>]}
                                 , {<<"default">>, true}
                                 , {<<"order">>, <<"ignore">>}
                                 ])
                     , ?JSON_OBJ([ {<<"name">>, <<"long_field1">>}
                                 , {<<"type">>, <<"long">>}
                                 ])
                     , ?JSON_OBJ([ {<<"name">>, <<"long_field2">>}
                                 , {<<"type">>, <<"long">>}
                                 , {<<"default">>, null}
                                 , {<<"order">>, <<"ignore">>}
                                 ])

                     ]}
    ]),
  JSON = iolist_to_binary(jsone:encode(Schema, [native_utf8])),
  ExpectedUnion = avro_union:type([ avro_primitive:boolean_type()
                                  , avro_primitive:int_type()
                                  ]),
  Expected = avro_record:type(
    "TestRecord",
    [ avro_record:define_field(
        "string_field",
        avro_primitive:string_type(),
        [{default, <<"FOOBAR">>},
         {order, descending}
        ])
    , avro_record:define_field(
        "union_field", ExpectedUnion,
        [{default, true},
         {order, ignore}
        ])
    , avro_record:define_field(
        "long_field1",
        avro_primitive:long_type(),
        [])
    , avro_record:define_field(
        "long_field2",
        avro_primitive:long_type(),
        [{default, null},
         {order, ignore}
        ])
    ],
    [{namespace, "name.space"}]),
  ?assertEqual(Expected, avro:decode_schema(JSON, [ignore_bad_default_values])),
  ?assertError({bad_default, [ {record, <<"name.space.TestRecord">>}
                             , {field, <<"long_field2">>}
                             , {reason, _}]},
               avro:decode_schema(JSON, [])).

parse_union_type_test() ->
  Schema = [ <<"int">>
           , <<"string">>
           , <<"typename">>
           ],
  Union = parse_schema(Schema),
  ?assertEqual(avro_union:type([avro_primitive:int_type(),
                                avro_primitive:string_type(),
                                "typename"]), Union).

parse_enum_type_full_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>,      <<"enum">>}
    , {<<"name">>,      <<"TestEnum">>}
    , {<<"namespace">>, <<"name.space">>}
    , {<<"symbols">>,   [<<"A">>, <<"B">>, <<"C">>]}
    , {<<"doc">>,       <<"descr">>}
    , {<<"aliases">>,   [<<"EnumAlias">>, <<"EnumAlias2">>]}
    ]),
  Enum = parse_schema(Schema),
  ExpectedType = avro_enum:type(
    "TestEnum",
    ["A", "B", "C"],
    [ {namespace,    "name.space"}
    , {doc,          "descr"}
    , {aliases,      ["EnumAlias", "EnumAlias2"]}
    ]),
  ?assertEqual(ExpectedType, Enum).

parse_enum_type_short_test() ->
  %% Only required fields are present
  Schema = ?JSON_OBJ(
    [ {<<"type">>,    <<"enum">>}
    , {<<"name">>,    <<"TestEnum">>}
    , {<<"symbols">>, [<<"A">>, <<"B">>, <<"C">>]}
    ]),
  Enum = parse_schema(Schema),
  ExpectedType = avro_enum:type(
    "TestEnum",
    ["A", "B", "C"],
    [ {namespace,    ?NS_GLOBAL}
    , {doc,          ""}
    , {aliases,      []}
    ]),
  ?assertEqual(ExpectedType, Enum).

parse_enum_unwrapped_test() ->
  Type = avro_enum:type(
    "TestEnum",
    ["A", "B", "C"],
    [ {namespace,    ""}
    , {doc,          ""}
    , {aliases,      []}
    ]),
  LkupFun = fun(_) -> Type end,
  Options = avro:make_decoder_options([{is_wrapped, false}]),
  ?assertEqual(<<"A">>,
               avro_json_decoder:parse(<<"A">>, "TestEnum", LkupFun, Options)).

parse_map_type_test() ->
  Schema = ?JSON_OBJ(
    [ {<<"type">>,   <<"map">>}
    , {<<"values">>, <<"int">>}
    ]),
  Map = parse_schema(Schema),
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
  Fixed = parse_schema(Schema),
  ExpectedType = avro_fixed:type("FooBar", 2,
                                 [ {namespace, "name.space"}
                                 , {aliases, ["Alias1", "Alias2"]}
                                 ]),
  ?assertEqual(ExpectedType, Fixed).

parse_bytes_value_test() ->
  RawJson = <<"{\"a\":\"\\u0010\\u0000\\u00FF\"}">>,
  #{<<"a">> := Bytes} = jsone:decode(RawJson),
  ?assertEqual([16,0,255], unicode:characters_to_list(Bytes, utf8)),
  Value = parse_value(Bytes, avro_primitive:bytes_type(), none),
  ?assertEqual(avro_primitive:bytes(<<16,0,255>>), Value).

bytes_value_encode_decode_test() ->
  Fields = [avro_record:define_field("a", bytes)],
  Schema = avro_record:type("Test", Fields, [{namespace, "name.space"}]),
  Bytes = iolist_to_binary(lists:seq(0, 255)),
  Record = avro_record:new(Schema, [{"a", Bytes}]),
  Json = avro_json_encoder:encode_value(Record),
  Lkup = fun(_) -> Schema end,
  Opts = avro:make_decoder_options([{is_wrapped, false}]),
  Decoded = avro_json_decoder:decode_value(Json, Schema, Lkup, Opts),
  ?assertEqual([{<<"a">>, Bytes}], Decoded),
  ok.

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
               avro_union:get_value(avro_record:get_value("union", Value))),

  ExpectedWithMapType = #{<<"array">> => [<<"ACTIVE">>, <<"CLOSED">>],
                          <<"invno">> => 100,
                          <<"union">> => true},
  ?assertEqual(ExpectedWithMapType,
               parse_value_with_map_type(Json, TestRecord, none)).


parse_record_value_missing_field_test() ->
  %% This test also tests parsing other types inside the record
  TestRecord = get_test_record(),
  Json = ?JSON_OBJ([ {<<"invno">>, 100}
                   , {<<"union">>, ?JSON_OBJ([{<<"boolean">>, true}])}
                   ]),
  %% parse_value(Json, TestRecord, none),
  %% ok.
  ?assertError({required_field_missed, <<"array">>},
               parse_value(Json, TestRecord, none)).

parse_record_value_unknown_field_test() ->
  %% This test also tests parsing other types inside the record
  TestRecord = get_test_record(),
  Json = ?JSON_OBJ([ {<<"invno">>, 100}
                   , {<<"array">>, [<<"ACTIVE">>, <<"CLOSED">>]}
                   , {<<"union">>, ?JSON_OBJ([{<<"boolean">>, true}])}
                   , {<<"unknown_field">>, 1}
                   ]),
  ?assertError({unknown_field, <<"unknown_field">>},
               parse_value(Json, TestRecord, none)).

parse_union_value_primitive_test() ->
  Type = avro_union:type([ avro_primitive:null_type()
                         , avro_primitive:string_type()
                         , avro_primitive:float_type()
                         , avro_primitive:double_type()
                         ]),
  Json1 = ?JSON_OBJ([{<<"string">>, <<"str">>}]),
  Value1 = parse_value(Json1, Type, none),
  ?assertEqual(avro_primitive:string("str"),
               avro_union:get_value(Value1)),
  Json2 = ?JSON_OBJ([{<<"double">>, 5.6}]),
  Value2 = parse_value(Json2, Type, none),
  ?assertEqual(avro_primitive:double(5.6),
               avro_union:get_value(Value2)),
  Json3 = ?JSON_OBJ([{<<"float">>, 4.2}]),
  Value3 = parse_value(Json3, Type, none),
  ?assertEqual(avro_primitive:float(4.2),
               avro_union:get_value(Value3)).


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
  ?assertError({unknown_union_member, _},
               parse_value(Json, Type, none)).

parse_enum_value_test() ->
  Type = avro_enum:type("MyEnum", ["A", "B", "C"]),
  Json = <<"B">>,
  Expected = avro_enum:new(Type, "B"),
  ?assertEqual(Expected, parse_value(Json, Type, none)).

parse_map_value_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  Json = ?JSON_OBJ([ {<<"v1">>, 1}, {<<"v2">>, 2} ]),
  Expected = avro_map:new(Type, [{"v1", 1}, {"v2", 2}]),
  ?assertEqual(Expected, parse_value(Json, Type, none)),

  ExpectedWithMapType = #{<<"v1">> => 1, <<"v2">> => 2},
  ?assertEqual(ExpectedWithMapType,
               parse_value_with_map_type(Json, Type, none)).

parse_fixed_value_test() ->
  Type = avro_fixed:type("FooBar", 2),
  RawJson = <<"{\"a\":\"\\u0001\\u007f\"}">>,
  #{<<"a">> := Bytes} = jsone:decode(RawJson),
  ?assertEqual([1,127], unicode:characters_to_list(Bytes, utf8)),
  ExpectedValue = avro_fixed:new(Type, <<1,127>>),
  ?assertEqual(ExpectedValue, parse_value(Bytes, Type, none)),
  ?assertEqual(<<1,127>>,
               parse(Bytes, Type, none,
                     avro:make_decoder_options([{is_wrapped, false}]))).

parse_value_with_lkup_fun_test() ->
  Hook = avro_decoder_hooks:pretty_print_hist(),
  ExtractTypeFun = fun(<<"name.space.Test">>) -> get_test_record() end,
  Schema = ?JSON_OBJ([ {<<"type">>, <<"array">>}
                     , {<<"items">>, <<"name.space.Test">>}
                     ]),
  ValueJson = [?JSON_OBJ([ {<<"invno">>, 100}
                         , {<<"array">>, [<<"ACTIVE">>, <<"CLOSED">>]}
                         , {<<"union">>, ?JSON_OBJ([{<<"boolean">>, true}])}
                         ])],
  Type = parse_schema(Schema),
  ExpectedType = avro_array:type("name.space.Test"),
  ?assertEqual(ExpectedType, Type),
  Value = parse(ValueJson, Type, ExtractTypeFun,
                avro:make_decoder_options([{hook, Hook}])),
  [Rec] = avro_array:get_items(Value),
  ?assertEqual(<<"name.space.Test">>,
               avro:get_type_fullname(?AVRO_VALUE_TYPE(Rec))),
  ?assertEqual(avro_primitive:long(100), avro_record:get_value("invno", Rec)),

  ExpectedWithMapType = [#{<<"array">> => [<<"ACTIVE">>, <<"CLOSED">>],
                           <<"invno">> => 100,
                           <<"union">> => true}],
  ?assertEqual(ExpectedWithMapType,
               parse_value_with_map_type(ValueJson, Type, ExtractTypeFun)).

decode_schema_test() ->
  _ = avro:decode_schema(<<"{\"type\":\"int\"}">>),
  _ = avro:decode_schema(<<"{\"type\":\"int\"}">>, []),
  ok.

type_ref_validate_test() ->
  Fixed = "{\"type\": \"fixed\", \"name\": \"MD5\", \"size\": 16}",
  FixedRef = "\"MD5\"",
  RecF = fun(Fixed1, Fixed2) ->
             iolist_to_binary(
               ["{\"type\": \"record\", \"name\":\"rec\","
                 "\"fields\": ["
                    "{\"name\": \"f1\", \"type\":", Fixed1, "},"
                    "{\"name\": \"f2\", \"type\":", Fixed2, "}"
                  "]}"])
         end,
  %% MD5 redefined
  ?assertException(throw, {type_redefined, <<"MD5">>},
                   avro:decode_schema(RecF(Fixed, Fixed))),
  %% MD5 redefined, but allowed
  _ = avro:decode_schema(RecF(Fixed, Fixed), [allow_type_redefine]),
  %% First MD5 is forward-referencing to the second
  %% according to spec, forward-referencing is not allowed
  ?assertException(throw, {ref_to_unknown_type, <<"MD5">>},
                   avro:decode_schema(RecF(FixedRef, Fixed))),
  %% Bad reference allowed
  _ = avro:decode_schema(RecF(FixedRef, Fixed), [allow_bad_references]),
  %% perfect schema
  _ = avro:decode_schema(RecF(Fixed, FixedRef), []),
  ok.

decode_wrapped_value_test() ->
  Type = avro_primitive:int_type(),
  Lkup = fun(<<"name">>) -> Type end,
  Value = avro_json_decoder:decode_value(<<"1">>, name, Lkup),
  ?assertEqual(?AVRO_VALUE(Type, 1), Value).

%% @private
get_test_record() ->
  F = fun(Name, Type) -> avro_record:define_field(Name, Type) end,
  Fields = [ F("invno", long)
           , F("array", avro_array:type(string))
           , F("union", avro_union:type([null, int, boolean]))
           ],
  avro_record:type("Test", Fields, [{namespace, "name.space"}]).

%% @private
parse_value(Value, Type, Lkup) ->
  parse(Value, Type, Lkup, avro:make_decoder_options([])).

parse_value_with_map_type(Value, Type, Lkup) ->
  Options = avro:make_decoder_options([{map_type, map},
                                       {record_type, map},
                                       {is_wrapped, false}]),
  parse(Value, Type, Lkup, Options).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
