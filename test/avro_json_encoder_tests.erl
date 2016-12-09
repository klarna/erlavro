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
-module(avro_json_encoder_tests).

-import(avro_json_encoder, [ encode_type/1
                           , encode_value/1
                           , encode_value/2
                           ]).

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TO_STRING(S), binary_to_list(iolist_to_binary(S))).

encode_null_type_test() ->
  Json = encode_type(avro_primitive:null_type()),
  ?assertEqual("\"null\"", ?TO_STRING(Json)).

encode_null_test() ->
  Json = encode_value(avro_primitive:null()),
  ?assertEqual("null", ?TO_STRING(Json)).

encode_boolean_type_test() ->
  Json = encode_type(avro_primitive:boolean_type()),
  ?assertEqual("\"boolean\"", ?TO_STRING(Json)).

encode_boolean_test() ->
  JsonTrue = encode_value(avro_primitive:boolean(true)),
  JsonFalse = encode_value(avro_primitive:boolean(false)),
  ?assertEqual("true", ?TO_STRING(JsonTrue)),
  ?assertEqual("false", ?TO_STRING(JsonFalse)).

encode_int_type_test() ->
  Json = encode_type(avro_primitive:int_type()),
  ?assertEqual("\"int\"", ?TO_STRING(Json)).

encode_int_test() ->
  Json = encode_value(avro_primitive:int(1)),
  ?assertEqual("1", ?TO_STRING(Json)).

encode_long_type_test() ->
  Json = encode_type(avro_primitive:long_type()),
  ?assertEqual("\"long\"", ?TO_STRING(Json)).

encode_long_test() ->
  Json = encode_value(avro_primitive:long(12345678901)),
  ?assertEqual("12345678901", ?TO_STRING(Json)).

encode_float_type_test() ->
  Json = encode_type(avro_primitive:float_type()),
  ?assertEqual("\"float\"", ?TO_STRING(Json)).

encode_float_test() ->
  Json = encode_value(avro_primitive:float(3.14159265358)),
  ?assertEqual("3.14159265358", ?TO_STRING(Json)).

encode_float_precision_lost_test() ->
  %% Warning: implementation of doubles in erlang loses
  %% precision on such numbers.
  ?assertEqual(<<"1e+16">>,
    encode_value(avro_primitive:float(10000000000000001), jsonx)),
  ?assertEqual("1.0e+16",
    encode_value(avro_primitive:float(10000000000000001), mochijson3)).

encode_integer_float_test() ->
  ?assertEqual(<<"314159265358">>,
    encode_value(avro_primitive:float(314159265358), jsonx)),
  ?assertEqual("314159265358.0",
    encode_value(avro_primitive:float(314159265358), mochijson3)).

encode_double_type_test() ->
  Json = encode_type(avro_primitive:double_type()),
  ?assertEqual("\"double\"", ?TO_STRING(Json)).

encode_double_test() ->
  Json = encode_value(avro_primitive:double(3.14159265358)),
  ?assertEqual("3.14159265358", ?TO_STRING(Json)).

encode_integer_double_test() ->
  ?assertEqual(<<"314159265358">>,
    encode_value(avro_primitive:double(314159265358), jsonx)),
  ?assertEqual("314159265358.0",
    encode_value(avro_primitive:double(314159265358), mochijson3)).

encode_bytes_type_test() ->
  Json = encode_type(avro_primitive:bytes_type()),
  ?assertEqual("\"bytes\"", ?TO_STRING(Json)).

encode_empty_bytes_test() ->
  Json = encode_value(avro_primitive:bytes(<<>>)),
  ?assertEqual("\"\"", ?TO_STRING(Json)).

encode_bytes_test() ->
  Json = encode_value(avro_primitive:bytes(<<0,1,100,255>>)),
  ?assertEqual("\"\\u0000\\u0001\\u0064\\u00ff\"", ?TO_STRING(Json)).

encode_string_type_test() ->
  Json = encode_type(avro_primitive:string_type()),
  ?assertEqual("\"string\"", ?TO_STRING(Json)).

encode_string_test() ->
  Json = encode_value(avro_primitive:string("Hello, Avro!")),
  ?assertEqual("\"Hello, Avro!\"", ?TO_STRING(Json)).

encode_string_with_quoting_test() ->
  Json = encode_value(avro_primitive:string("\"\\")),
  ?assertEqual("\"\\\"\\\\\"", ?TO_STRING(Json)).

encode_utf8_string_test() ->
  S = unicode:characters_to_binary("Avro är populär", latin1, utf8),
  Json = encode_value(avro_primitive:string(binary_to_list(S))),
  ?assertEqual("\"Avro " ++ [195,164] ++ "r popul"++ [195,164] ++ "r\"",
    ?TO_STRING(Json)).

encode_record_type_test() ->
  Json = encode_type(sample_record_type()),
  ?assertEqual("{"
  "\"namespace\":\"com.klarna.test.bix\","
  "\"type\":\"record\","
  "\"name\":\"SampleRecord\","
  "\"doc\":\"Record documentation\","
  "\"fields\":["
  "{\"name\":\"bool\","
  "\"type\":\"boolean\","
  "\"default\":false,"
  "\"doc\":\"bool f\"},"
  "{\"name\":\"int\","
  "\"type\":\"int\","
  "\"default\":0,"
  "\"doc\":\"int f\"},"
  "{\"name\":\"long\","
  "\"type\":\"long\","
  "\"default\":42,"
  "\"doc\":\"long f\"},"
  "{\"name\":\"float\","
  "\"type\":\"float\","
  "\"default\":3.14,"
  "\"doc\":\"float f\"},"
  "{\"name\":\"double\","
  "\"type\":\"double\","
  "\"default\":6.67221937,"
  "\"doc\":\"double f\"},"
  "{\"name\":\"bytes\","
  "\"type\":\"bytes\","
  "\"doc\":\"bytes f\"},"
  "{\"name\":\"string\","
  "\"type\":\"string\","
  "\"default\":\"string value\","
  "\"doc\":\"string f\"}]"
  "}",
    ?TO_STRING(Json)).

encode_record_test() ->
  Json = encode_value(sample_record()),
  Expected = "{"
  "\"bool\":true,"
  "\"int\":100,"
  "\"long\":123456789123456789,"
  "\"float\":2.718281828,"
  "\"double\":3.14159265358,"
  "\"bytes\":\"\\u0062\\u0079\\u0074\\u0065\\u0073\\u0020\\u0076"
  "\\u0061\\u006c\\u0075\\u0065\","
  "\"string\":\"string value\""
  "}",
  ?assertEqual(Expected, ?TO_STRING(Json)).

encode_enum_type_test() ->
  EnumType =
    avro_enum:type("Enum",
      ["A", "B", "C"],
      [{namespace, "com.klarna.test.bix"}]),
  EnumTypeJson = encode_type(EnumType),
  ?assertEqual("{"
  "\"namespace\":\"com.klarna.test.bix\","
  "\"type\":\"enum\","
  "\"name\":\"Enum\","
  "\"symbols\":[\"A\",\"B\",\"C\"]"
  "}",
    ?TO_STRING(EnumTypeJson)).

encode_enum_test() ->
  EnumType =
    avro_enum:type("Enum",
      ["A", "B", "C"],
      [{namespace, "com.klarna.test.bix"}]),
  EnumValue = ?AVRO_VALUE(EnumType, "B"),
  EnumValueJson = encode_value(EnumValue),
  ?assertEqual("\"B\"", ?TO_STRING(EnumValueJson)).

encode_union_type_test() ->
  UnionType = avro_union:type([ avro_primitive:string_type()
                              , avro_primitive:int_type()]),
  Json = encode_type(UnionType),
  ?assertEqual("[\"string\",\"int\"]", ?TO_STRING(Json)).

encode_union_test() ->
  UnionType = avro_union:type([ avro_primitive:string_type()
    , avro_primitive:int_type()]),
  Value = avro_union:new(UnionType, avro_primitive:int(10)),
  Json = encode_value(Value),
  ?assertEqual("{\"int\":10}", ?TO_STRING(Json)).

encode_union_with_null_test() ->
  UnionType = avro_union:type([ avro_primitive:string_type()
    , avro_primitive:null_type()]),
  Value = avro_union:new(UnionType, avro_primitive:null()),
  Json = encode_value(Value),
  ?assertEqual("null", ?TO_STRING(Json)).

encode_array_type_test() ->
  Type = avro_array:type(avro_primitive:string_type()),
  Json = encode_type(Type),
  ?assertEqual("{\"type\":\"array\",\"items\":\"string\"}", ?TO_STRING(Json)).

encode_array_test() ->
  Type = avro_array:type(avro_primitive:string_type()),
  Value = avro_array:new(Type,
    [ avro_primitive:string("a")
      , avro_primitive:string("b")]),
  Json = encode_value(Value),
  ?assertEqual("[\"a\",\"b\"]", ?TO_STRING(Json)).

encode_map_type_test() ->
  MapType = avro_map:type(avro_union:type(
    [avro_primitive:int_type(),
      avro_primitive:null_type()])),
  Json = encode_type(MapType),
  ?assertEqual("{\"type\":\"map\",\"values\":[\"int\",\"null\"]}",
    ?TO_STRING(Json)).

encode_map_test() ->
  MapType = avro_map:type(avro_union:type(
    [avro_primitive:int_type(),
      avro_primitive:null_type()])),
  MapValue = avro_map:new(MapType,
    [{"v1", 1}, {"v2", null}, {"v3", 2}]),
  Json = encode_value(MapValue),
  ?assertEqual("{\"v3\":{\"int\":2},\"v1\":{\"int\":1},\"v2\":null}",
    ?TO_STRING(Json)).

encode_fixed_type_test() ->
  Type = avro_fixed:type("FooBar", 2,
    [ {namespace, "name.space"}
      , {aliases, ["Alias1", "Alias2"]}
    ]),
  Json = encode_type(Type),
  ?assertEqual("{"
  "\"namespace\":\"name.space\","
  "\"type\":\"fixed\","
  "\"name\":\"FooBar\","
  "\"size\":2,"
  "\"aliases\":[\"name.space.Alias1\",\"name.space.Alias2\"]}",
    ?TO_STRING(Json)).

encode_fixed_value_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Value = avro_fixed:new(Type, <<1,127>>),
  Json = encode_value(Value),
  ?assertEqual("\"\\u0001\\u007f\"", ?TO_STRING(Json)).

check_json_encode_record_properly_test() ->
  MyRecordType = avro_record:type("MyRecord",
    [avro_record:define_field("f1", avro_primitive:int_type()),
      avro_record:define_field("f2", avro_primitive:string_type())],
    [{namespace, "my.com"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Term = [{"f1", 1},{"f2","my string"}],
  {ok, AvroValue} = avro:cast(MyRecordType, Term),
  ExpectedJSON = encode_value(AvroValue),
  JSON = encode(Store, "my.com.MyRecord", Term),
  ?assertEqual(ExpectedJSON, JSON),
  ?assertEqual(Term,
    avro_json_decoder:decode_value(JSON, "my.com.MyRecord", Store,
                                   [{is_wrapped, false}])).

check_json_encode_enum_properly_test() ->
  EnumType =
    avro_enum:type("Enum",
      ["A", "B", "C"],
      [{namespace, "com.klarna.test.bix"}]),
  Store = avro_schema_store:add_type(EnumType, avro_schema_store:new([])),
  EnumValue = ?AVRO_VALUE(EnumType, "B"),
  EnumValueJson = encode_value(EnumValue),
  Encoded = encode(Store, "com.klarna.test.bix.Enum", "B"),
  ?assertEqual(EnumValueJson, Encoded).

check_json_encode_array_properly_test() ->
  Type = avro_array:type(avro_primitive:string_type()),
  Value = avro_array:new(Type,
    [ avro_primitive:string("a")
    , avro_primitive:string("b")]),
  ExpectedJSON = encode_value(Value),
  JSON = encode(fun(_) -> Type end, "some_array", ["a", "b"]),
  ?assertEqual(ExpectedJSON, JSON).

check_json_encode_map_properly_test() ->
  MapType = avro_map:type(avro_union:type(
    [avro_primitive:int_type(),
      avro_primitive:null_type()])),
  Value = [{"v1", 1}, {"v2", null}, {"v3", 2}],
  MapValue = avro_map:new(MapType, Value),
  JSON1 = encode_value(MapValue),
  JSON2 = encode(fun(_) -> MapType end, "some_map", Value),
  DecodeF = fun(JSON) ->
              avro_json_decoder:decode_value(JSON, MapType, none,
                                             [{is_wrapped, false}])
            end,
  ?assertEqual(Value, lists:keysort(1, DecodeF(JSON1))),
  ?assertEqual(Value, lists:keysort(1, DecodeF(JSON2))).

check_json_encode_union_properly_test() ->
  UnionType = avro_union:type([ avro_primitive:string_type()
                              , avro_primitive:null_type()]),
  Value1 = avro_union:new(UnionType, avro_primitive:null()),
  Value2 = avro_union:new(UnionType, avro_primitive:string("bar")),
  Json1 = encode_value(Value1),
  Json2 = encode_value(Value2),
  Encoded1 = encode(none, UnionType, null),
  Encoded2 = encode(none, UnionType, "bar"),
  ?assertEqual(Json1, Encoded1),
  ?assertEqual(Json2, Encoded2).

check_json_encode_fixed_properly_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Value = avro_fixed:new(Type, <<1,127>>),
  Json = encode_value(Value),
  Encoded = encode(fun(_) -> Type end, "some_fixed", <<1,127>>),
  ?assertEqual(?TO_STRING(Json), ?TO_STRING(Encoded)).


%% @private
sample_record_type() ->
  avro_record:type(
    "SampleRecord",
    [ avro_record:define_field("bool", avro_primitive:boolean_type(),
      [ {doc, "bool f"}
      , {default, avro_primitive:boolean(false)}
      ])
    , avro_record:define_field("int", avro_primitive:int_type(),
      [ {doc, "int f"}
      , {default, avro_primitive:int(0)}
      ])
    , avro_record:define_field("long", avro_primitive:long_type(),
      [ {doc, "long f"}
      , {default, avro_primitive:long(42)}
      ])
    , avro_record:define_field("float", avro_primitive:float_type(),
      [ {doc, "float f"}
      , {default, avro_primitive:float(3.14)}
      ])
    , avro_record:define_field("double", avro_primitive:double_type(),
      [ {doc, "double f"}
      , {default, avro_primitive:double(6.67221937)}
      ])
    , avro_record:define_field("bytes", avro_primitive:bytes_type(),
      [ {doc, "bytes f"}
      ])
    , avro_record:define_field("string", avro_primitive:string_type(),
      [ {doc, "string f"}
      , {default,
        avro_primitive:string("string value")}
      ])
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Record documentation"}]).

%% @private
sample_record() ->
  avro_record:new(sample_record_type(),
    [ {"string", "string value"}
    , {"double", 3.14159265358}
    , {"long",   123456789123456789}
    , {"bool",   true}
    , {"int",    100}
    , {"float",  2.718281828}
    , {"bytes",  <<"bytes value">>}
    ]).

%% @private
encode(StoreOrLkupFun, TypeOrName, Value) ->
  iolist_to_binary(
    avro_json_encoder:encode(StoreOrLkupFun, TypeOrName, Value)).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
