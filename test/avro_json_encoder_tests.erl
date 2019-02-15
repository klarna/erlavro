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
-module(avro_json_encoder_tests).

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").

encode_null_type_test() ->
  Json = encode_type(avro_primitive:null_type()),
  ?assertEqual(<<"\"null\"">>, Json).

encode_null_test() ->
  Json = encode_value(avro_primitive:null()),
  ?assertEqual(<<"null">>, Json).

encode_boolean_type_test() ->
  Json = encode_type(avro_primitive:boolean_type()),
  ?assertEqual(<<"\"boolean\"">>, Json).

encode_type_with_custom_prop_test() ->
  Props = [{<<"logicalType">>, <<"x">>}],
  Json = encode_type(avro_primitive:type(bytes, Props)),
  ?assertEqual(<<"{\"type\":\"bytes\",\"logicalType\":\"x\"}">>, Json).

encode_boolean_test() ->
  JsonTrue = encode_value(avro_primitive:boolean(true)),
  JsonFalse = encode_value(avro_primitive:boolean(false)),
  ?assertEqual(<<"true">>, JsonTrue),
  ?assertEqual(<<"false">>, JsonFalse).

encode_int_type_test() ->
  Json = encode_type(avro_primitive:int_type()),
  ?assertEqual(<<"\"int\"">>, Json).

encode_int_test() ->
  Json = encode_value(avro_primitive:int(1)),
  ?assertEqual(<<"1">>, Json).

encode_long_type_test() ->
  Json = encode_type(avro_primitive:long_type()),
  ?assertEqual(<<"\"long\"">>, Json).

encode_long_test() ->
  Json = encode_value(avro_primitive:long(12345678901)),
  ?assertEqual(<<"12345678901">>, Json).

encode_float_type_test() ->
  Json = encode_type(avro_primitive:float_type()),
  ?assertEqual(<<"\"float\"">>, Json).

encode_float_test() ->
  Json = encode_value(avro_primitive:float(3.14159265358)),
  ?assertEqual(<<"3.14159265358">>, Json).

encode_float_precision_lost_test() ->
  ?assertEqual(<<"1.0e16">>,
    encode_value(avro_primitive:float(10000000000000001))).

encode_integer_float_test() ->
  ?assertEqual(<<"314159265358.0">>,
    encode_value(avro_primitive:float(314159265358))).

encode_double_type_test() ->
  Json = encode_type(avro_primitive:double_type()),
  ?assertEqual(<<"\"double\"">>, Json).

encode_double_test() ->
  Json = encode_value(avro_primitive:double(3.14159265358)),
  ?assertEqual(<<"3.14159265358">>, Json).

encode_integer_double_test() ->
  ?assertEqual(<<"314159265358.0">>,
    encode_value(avro_primitive:double(314159265358))).

encode_bytes_type_test() ->
  Json = encode_type(avro_primitive:bytes_type()),
  ?assertEqual(<<"\"bytes\"">>, Json).

encode_empty_bytes_test() ->
  Json = encode_value(avro_primitive:bytes(<<>>)),
  ?assertEqual(<<"\"\"">>, Json).

encode_bytes_test() ->
  Json = encode_value(avro_primitive:bytes(<<0,1,100,255>>)),
  ?assertEqual(<<"\"\\u0000\\u0001\\u0064\\u00ff\"">>, Json).

encode_string_type_test() ->
  Json = encode_type(avro_primitive:string_type()),
  ?assertEqual(<<"\"string\"">>, Json).

encode_string_test() ->
  Json = encode_value(avro_primitive:string("Hello, Avro!")),
  ?assertEqual(<<"\"Hello, Avro!\"">>, Json).

encode_string_with_quoting_test() ->
  Json = encode_value(avro_primitive:string("\"\\")),
  ?assertEqual(<<"\"\\\"\\\\\"">>, Json).

encode_utf8_string_test() ->
  %% This file is in latin-1 encoding.
  %% To test utf8, we need to perform a latin1 to utf8 translation
  S = unicode:characters_to_binary("Avro är populär", latin1, utf8),
  Json = encode_value(avro_primitive:string(S)),
  %% And because it's in latin-1 encoding, we need to hard-code
  %% the expected utf8 string with byte-lists inline
  Expected0 = ["\"Avro " ++ [195,164] ++ "r popul"++ [195,164] ++ "r\""],
  Expected = iolist_to_binary(Expected0),
  ?assertEqual(Expected, Json).

encode_record_type_test() ->
  Json = encode_type(sample_record_type()),
  ?assertEqual(<<"{"
  "\"namespace\":\"com.klarna.test.bix\","
  "\"name\":\"SampleRecord\","
  "\"type\":\"record\","
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
  "}">>, Json).

encode_record_test() ->
  Json = encode_value(sample_record()),
  Expected = <<"{"
  "\"bool\":true,"
  "\"int\":100,"
  "\"long\":123456789123456789,"
  "\"float\":2.718281828,"
  "\"double\":3.14159265358,"
  "\"bytes\":\"\\u0062\\u0079\\u0074\\u0065\\u0073\\u0020\\u0076"
  "\\u0061\\u006c\\u0075\\u0065\","
  "\"string\":\"string value\""
  "}">>,
  ?assertEqual(Expected, Json).

encode_enum_type_test() ->
  EnumType = avro_enum:type("Enum", ["A", "B", "C"],
                            [{namespace, "com.klarna.test.bix"}]),
  EnumTypeJson = encode_type(EnumType),
  ?assertEqual(<<"{"
  "\"namespace\":\"com.klarna.test.bix\","
  "\"name\":\"Enum\","
  "\"type\":\"enum\","
  "\"symbols\":[\"A\",\"B\",\"C\"]"
  "}">>, EnumTypeJson).

encode_enum_test_() ->
  EnumType = avro_enum:type("Enum", ["A", "B", "C"],
                            [{namespace, "com.klarna.test.bix"}]),
  [{"wrapped value encoding",
    fun() ->
        EnumValue = ?AVRO_VALUE(EnumType, "B"),
        JSON = encode_value(EnumValue),
        ?assertEqual(<<"\"B\"">>, JSON)
    end},
   {"schema value side by side encoding",
    fun() ->
        Input = 'A',
        JSON = encode(ignore, EnumType, Input),
        ?assertEqual(<<"\"A\"">>, JSON)
    end
   }].

encode_union_type_test() ->
  UnionType = avro_union:type([string, int]),
  Json = encode_type(UnionType),
  ?assertEqual(<<"[\"string\",\"int\"]">>, Json).

encode_union_test() ->
  UnionType = avro_union:type([string, int]),
  Value = avro_union:new(UnionType, avro_primitive:int(10)),
  Json = encode_value(Value),
  ?assertEqual(<<"{\"int\":10}">>, Json).

encode_union_with_null_test() ->
  UnionType = avro_union:type([string, null]),
  Value = avro_union:new(UnionType, avro_primitive:null()),
  Json = encode_value(Value),
  ?assertEqual(<<"null">>, Json).

encode_array_type_test() ->
  Type = avro_array:type(string),
  Json = encode_type(Type),
  ?assertEqual(<<"{\"type\":\"array\",\"items\":\"string\"}">>, Json).

encode_array_test() ->
  Type = avro_array:type(string),
  Value = avro_array:new(Type, [avro_primitive:string("a"),
                                avro_primitive:string("b")]),
  Json = encode_value(Value),
  ?assertEqual(<<"[\"a\",\"b\"]">>, Json).

encode_map_type_test() ->
  MapType = avro_map:type(avro_union:type([int, null])),
  Json = encode_type(MapType),
  ?assertEqual(<<"{\"type\":\"map\",\"values\":[\"int\",\"null\"]}">>, Json).

encode_map_test() ->
  MapType = avro_map:type(avro_union:type([int, null])),
  MapValue0 = avro_map:new(MapType, [{v1, 1}, {"v2", null}, {<<"v3">>, 2}]),
  Json0 = encode_value(MapValue0),
  ?assertEqual(<<"{\"v1\":{\"int\":1},\"v2\":null,\"v3\":{\"int\":2}}">>, Json0),

  MapValue1 = avro_map:new(MapType, #{v1 => 1, "v2" => null, <<"v3">> => 2}),
  Json1 = encode_value(MapValue1),
  ?assertEqual(<<"{\"v1\":{\"int\":1},\"v2\":null,\"v3\":{\"int\":2}}">>, Json1).

encode_fixed_type_test() ->
  Type = avro_fixed:type("FooBar", 2,
                         [ {namespace, "name.space"}
                         , {aliases, ["Alias1", "Alias2"]}
                         ]),
  Json = encode_type(Type),
  ?assertEqual(<<"{"
  "\"namespace\":\"name.space\","
  "\"name\":\"FooBar\","
  "\"type\":\"fixed\","
  "\"size\":2,"
  "\"aliases\":[\"name.space.Alias1\",\"name.space.Alias2\"]}">>, Json).

encode_fixed_value_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Value = avro_fixed:new(Type, <<1,127>>),
  Json = encode_value(Value),
  ?assertEqual(<<"\"\\u0001\\u007f\"">>, Json).

check_json_encode_record_properly_test() ->
  MyRecordType =
    avro_record:type("MyRecord",
                     [ define_field(f1, int)
                     , define_field("f2", string)],
                     [{namespace, "my.com"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Decoder = avro:make_decoder(Store, [{encoding, avro_json}]),
  Term = [{f1, 1}, {<<"f2">>, <<"my string">>}],
  {ok, AvroValue} = avro:cast(MyRecordType, Term),
  ExpectedJSON = encode_value(AvroValue),
  JSON = encode(Store, "my.com.MyRecord", Term),
  ?assertEqual(ExpectedJSON, JSON),
  ?assertEqual([{<<"f1">>, 1}, {<<"f2">>, <<"my string">>}],
               Decoder("my.com.MyRecord", JSON)).

check_json_encode_enum_properly_test() ->
  EnumType =
    avro_enum:type("Enum", ["A", "B", "C"],
                   [{namespace, "com.klarna.test.bix"}]),
  Store = avro_schema_store:add_type(EnumType, avro_schema_store:new([])),
  EnumValue = ?AVRO_VALUE(EnumType, "B"),
  EnumValueJson = encode_value(EnumValue),
  Encoded = encode(Store, "com.klarna.test.bix.Enum", "B"),
  ?assertEqual(EnumValueJson, Encoded).

check_json_encode_array_properly_test() ->
  Type = avro_array:type(string),
  Value = avro_array:new(Type, [ avro_primitive:string("a")
                               , avro_primitive:string("b")]),
  ExpectedJSON = encode_value(Value),
  JSON = encode(fun(_) -> Type end, "some_array", ["a", "b"]),
  ?assertEqual(ExpectedJSON, JSON).

check_json_encode_map_properly_test() ->
  MapType = avro_map:type(avro_union:type([int, null])),
  Value = [{<<"v1">>, 1}, {<<"v2">>, null}, {<<"v3">>, 2}],
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
  UnionType = avro_union:type([string, null]),
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
  ?assertEqual(Json, Encoded).

encode_type_fullname_ref_test() ->
  Type = avro_map:type("com.example.type"),
  Encoded = encode_type(Type),
  JSON = <<"{\"type\":\"map\",\"values\":\"com.example.type\"}">>,
  ?assertEqual(JSON, iolist_to_binary(Encoded)).

encode_type_shortname_ref_test() ->
  Field = avro_record:define_field("f1", "com.example.mytype", []),
  Type = avro_record:type("rec", [Field], [{namespace, "com.example"}]),
  Encoded = encode_type(Type),
  Expected =
    <<"{\"namespace\":\"com.example\","
       "\"name\":\"rec\","
       "\"type\":\"record\","
       "\"fields\":[{\"name\":\"f1\",\"type\":\"mytype\"}]}">>,
  ?assertEqual(Expected, iolist_to_binary(Encoded)).

encode_type_no_redundant_ns_test() ->
  SubField = avro_record:define_field("subf", int, []),
  SubType = avro_record:type("subrec", [SubField],
                             [{namespace, "com.example"}]),
  Field = avro_record:define_field("f", SubType, []),
  Type = avro_record:type("rec", [Field], [{namespace, "com.example"}]),
  Encoded = encode_type(Type),
  SubJSON = ["{\"name\":\"subrec\","
              "\"type\":\"record\","
              "\"fields\":[{\"name\":\"subf\",\"type\":\"int\"}]}"],
  Expected =
    ["{\"namespace\":\"com.example\","
      "\"name\":\"rec\","
      "\"type\":\"record\","
      "\"fields\":"
          "[{\"name\":\"f\","
            "\"type\":", SubJSON,
           "}]}"],
  ?assertEqual(iolist_to_binary(Expected), iolist_to_binary(Encoded)).

encode_field_order_test_() ->
  [ fun() ->
        Field = avro_record:define_field("f", "mytype", [{order, Order}]),
        Type = avro_record:type("rec", [Field], [{namespace, "com.example"}]),
        Encoded = encode_type(Type),
        EncodedOrder =
          case Order of
            ascending -> ""; %% default is ignored
            _         -> [",\"order\":\"", atom_to_list(Order), "\""]
          end,
        Expected =
           ["{\"namespace\":\"com.example\","
             "\"name\":\"rec\","
             "\"type\":\"record\","
             "\"fields\":[{\"name\":\"f\","
                          "\"type\":\"mytype\"",
                          EncodedOrder,
                          "}]}"],
        ?assertEqual(iolist_to_binary(Expected),
                     iolist_to_binary(Encoded))
    end || Order <- [ascending, descending, ignore]
  ].

encode_dup_ref_test() ->
  Enum = avro_enum:type("Enum", ["a", "b"]),
  Field = avro_record:define_field("f", int, []),
  SubRec = avro_record:type("sub", [Field], []),
  Field1 = avro_record:define_field("a", SubRec, []),
  Field2 = avro_record:define_field("b", SubRec, []),
  Field3 = avro_record:define_field("c", Enum, []),
  Field4 = avro_record:define_field("d", Enum, []),
  Type = avro_record:type("root", [Field1, Field2, Field3, Field4], []),
  JSON = avro_json_encoder:encode_type(Type),
  DecodedType = avro:decode_schema(JSON, []),
  Store = avro_schema_store:new(),
  Store = avro_schema_store:add_type(DecodedType, Store),
  ExpandedType = avro:expand_type_bloated("root", Store),
  ?assertEqual(ExpandedType, Type),
  avro_schema_store:close(Store).

%% @private
sample_record_type() ->
  avro_record:type(
    "SampleRecord",
    [ define_field("bool", boolean,
                   [ {doc, "bool f"}
                   , {default, avro_primitive:boolean(false)} ])
    , define_field("int", int,
                   [ {doc, "int f"}
                   , {default, avro_primitive:int(0)} ])
    , define_field("long", long,
                   [ {doc, "long f"}
                   , {default, avro_primitive:long(42)} ])
    , define_field("float", float,
                   [ {doc, "float f"}
                   , {default, avro_primitive:float(3.14)} ])
    , define_field("double", double,
                   [ {doc, "double f"}
                   , {default, avro_primitive:double(6.67221937)} ])
    , define_field("bytes", bytes,
                   [ {doc, "bytes f"} ])
    , define_field("string", string,
                   [ {doc, "string f"}
                   , {default, avro_primitive:string("string value")} ])
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
encode_type(Type) ->
  iolist_to_binary(avro:encode_schema(Type)).

%% @private
encode_value(Value) ->
  iolist_to_binary(avro_json_encoder:encode_value(Value)).

%% @private
encode(StoreOrLkupFun, TypeOrName, Value) ->
  iolist_to_binary(
    avro_json_encoder:encode(StoreOrLkupFun, TypeOrName, Value)).

%% @private
define_field(Name, Type) ->
  avro_record:define_field(Name, Type).

%% @private
define_field(Name, Type, Opts) ->
  avro_record:define_field(Name, Type, Opts).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
