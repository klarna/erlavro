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
-module(avro_binary_encoder_tests).

-import(avro_binary_encoder, [ encode/3
                             , encode_value/1
                             , int/1
                             , long/1
                             , string/1
                             , zigzag/2
                             ]).

-include("avro_internal.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TO_STRING(S), iolist_to_binary(S)).

-define(assertBinEq(A,B),
  ?assertEqual(iolist_to_binary(A),iolist_to_binary(B))).

encode_null_test() ->
  BinNull = encode_value(avro_primitive:null()),
  ?assertBinEq(<<>>, BinNull).

encode_boolean_test() ->
  BinaryTrue = encode_value(avro_primitive:boolean(true)),
  BinaryFalse = encode_value(avro_primitive:boolean(false)),
  ?assertBinEq(<<1>>, BinaryTrue),
  ?assertBinEq(<<0>>, BinaryFalse).

zigzag_test() ->
  ?assertEqual(0, zigzag(int, 0)),
  ?assertEqual(1, zigzag(int, -1)),
  ?assertEqual(2, zigzag(int, 1)),
  ?assertEqual(3, zigzag(int, -2)),
  ?assertEqual(4294967294, zigzag(long, 2147483647)),
  ?assertEqual(4294967295, zigzag(long, -2147483648)).

encode_int_test() ->
  ?assertBinEq(<<8>>, encode_value(avro_primitive:int(4))),
  ?assertBinEq(<<3>>, encode_value(avro_primitive:int(-2))),
  ?assertBinEq(<<127>>,    encode_value(avro_primitive:int(-64))),
  ?assertBinEq(<<128, 1>>, encode_value(avro_primitive:int(64))),
  ?assertBinEq(<<253, 1>>, encode_value(avro_primitive:int(-127))),
  ?assertBinEq(<<254, 1>>, encode_value(avro_primitive:int(127))),
  ?assertBinEq(<<128, 2>>, encode_value(avro_primitive:int(128))).

encode_long_test() ->
  BinLong = encode_value(avro_primitive:long(12345678901)),
  ?assertBinEq(<<234, 240, 224, 253, 91>>, BinLong).

encode_float_test() ->
  BinFloat = encode_value(avro_primitive:float(3.14159265358)),
  ?assertBinEq(<<219, 15, 73, 64>>, BinFloat).

encode_float_precision_lost_test() ->
  %% Warning: implementation of doubles in erlang loses
  %% precision on such numbers.
  ?assertBinEq(<<202,27,14,90>>,
               encode_value(avro_primitive:float(10000000000000001))).

encode_integer_float_test() ->
  ?assertBinEq(<<180, 74, 146, 82>>,
               encode_value(avro_primitive:float(314159265358))).

encode_double_test() ->
  BinDouble = encode_value(avro_primitive:double(3.14159265358)),
  ?assertBinEq(<<244, 214, 67, 84, 251, 33, 9, 64>>, BinDouble).

encode_integer_double_test() ->
  ?assertBinEq(<<0, 128, 147, 125, 86, 73, 82, 66>>,
               encode_value(avro_primitive:double(314159265358))).

encode_empty_bytes_test() ->
  BinBytes = encode_value(avro_primitive:bytes(<<>>)),
  ?assertBinEq(<<0>>, iolist_to_binary(BinBytes)).

encode_bytes_test() ->
  BinBytes = encode_value(avro_primitive:bytes(<<0, 1, 100, 255>>)),
  ?assertBinEq([8, <<0, 1, 100, 255>>], BinBytes).

encode_string_test() ->
  BinString = encode_value(avro_primitive:string("Hello, Avro!")),
  ?assertBinEq([24, <<72, 101, 108, 108, 111, 44, 32,
                      65, 118, 114, 111, 33>>], BinString).

encode_string_with_quoting_test() ->
  BinString = encode_value(avro_primitive:string("\"\\")),
  ?assertBinEq([4, <<34, 92>>], BinString).

encode_utf8_string_test() ->
  S = unicode:characters_to_binary("Avro är populär", latin1, utf8),
  BinString = encode_value(avro_primitive:string(?TO_STRING(S))),
  ?assertBinEq([34, "Avro ", [195,164], "r popul", [195,164], "r"], BinString).

encode_record_test() ->
  BinRecord = encode_value(sample_record()),
  Expected =
    [<<1>>, %% bool
     <<200,1>>, %% int
     <<170,252,130,205,245,210,205,182,3>>, % long
     <<84, 248, 45, 64>>, % float
     <<244, 214, 67, 84, 251, 33, 9, 64>>, % double
     [22, <<98, 121, 116, 101, 115, 32, 118, 97, 108, 117, 101>>], % bytes
     [12, <<115, 116, 114, 105, 110, 103>>] % string
    ],
  ?assertBinEq(Expected, BinRecord).

encode_record_error_test() ->
  SubType =
    avro_record:type("sub_rec",
                     [ define_field("bool_field", boolean, [])
                     , define_field("int_field", int, [])
                     ]),
  RootType =
    avro_record:type("root_rec",
                     [ define_field("sub", SubType, [])
                     ]),
  In = [{sub, [{<<"bool_field">>, true}, {<<"int_field">>, "not int"}]}],
  ?assertError(?ENC_ERR(_, [{record, <<"root_rec">>},
                            {field, <<"sub">>},
                            {record, <<"sub_rec">>},
                            {field, <<"int_field">>}]),
               encode(fun(_) -> error(unexpected) end, RootType, In)).

encode_enum_test() ->
  EnumType = avro_enum:type("TestEnum",
                            ["SYMBOL_0", "SYMBOL_1", "SYMBOL_2",
                             "SYMBOL_3", "SYMBOL_4", "SYMBOL_5"],
                            [{namespace, "com.klarna.test.bix"}]),
  EnumValue = ?AVRO_VALUE(EnumType, "SYMBOL_4"),
  BynaryEnum = encode_value(EnumValue),
  ?assertBinEq([8], BynaryEnum).

encode_empty_array_test() ->
  Type = avro_array:type(int),
  TypedValue = avro_array:new(Type, []),
  ?assertBinEq([0], encode_value(TypedValue)).

encode_array_test() ->
  Type = avro_array:type(int),
  TypedValue = avro_array:new(Type, [3, 27]),
  ?assertBinEq([3,4,6,54,0], encode_value(TypedValue)).

encode_empty_map_test() ->
  Type = avro_map:type(int),
  TypedValue = avro_map:new(Type, []),
  ?assertBinEq([0], encode_value(TypedValue)).

encode_map_test() ->
  Type = avro_map:type(int),
  TypedValue = avro_map:new(Type, [{a, 3}, {"b", 27}]),
  Body = iolist_to_binary([string(a), int(3), string(<<"b">>), int(27)]),
  ?assertBinEq([long(-2), long(size(Body)), Body, 0], encode_value(TypedValue)).

encode_map_error_test() ->
  Type = avro_map:type(int),
  Value = [{a, 3}, {"b", "not int"}],
  ?assertError(?ENC_ERR(_, [{map, Type}, {key, "b"}]),
               encode(fun(_) -> error(unexpected) end, Type, Value)).

encode_union_test() ->
  Type = avro_union:type([null, string]),
  Value1 = avro_union:new(Type, null),
  Value2 = avro_union:new(Type, "a"),
  ?assertBinEq([0], encode_value(Value1)),
  ?assertBinEq([long(1), long(1), 97], encode_value(Value2)).

encode_fixed_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Value = avro_fixed:new(Type, <<1,127>>),
  ?assertBinEq(<<1, 127>>, encode_value(Value)).

encode_binary_properly_test() ->
  MyRecordType =
    avro_record:type("MyRecord",
                     [define_field("f1", int),
                      define_field("f2", string)],
                     [{namespace, "my.com"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Term = [{"f1", 1}, {"f2", "my string"}],
  Encoded = encode(Store, "my.com.MyRecord", Term),
  {ok, AvroValue} = avro:cast(MyRecordType, Term),
  EncodedValue = encode_value(AvroValue),
  ?assertEqual(Encoded, EncodedValue).

encode_enum_properly_test() ->
  UnionType = avro_union:type([string, null]),
  Value1 = avro_union:new(UnionType, avro_primitive:null()),
  Value2 = avro_union:new(UnionType, avro_primitive:string("bar")),
  EncodedValue1 = encode_value(Value1),
  EncodedValue2 = encode_value(Value2),
  Encoded1 = encode(fun(_) -> UnionType end, "some_union", null),
  Encoded2 = encode(fun(_) -> UnionType end, "some_union", "bar"),
  ?assertEqual(EncodedValue1, Encoded1),
  ?assertEqual(EncodedValue2, Encoded2).

encode_map_properly_test() ->
  Type = avro_map:type(int),
  TypedValue = avro_map:new(Type, [{"a", 3}, {"b", 27}]),
  Encoded = encode(fun(_) -> Type end, "some map", [{"a", 3}, {"b", 27}]),
  ?assertBinEq(encode_value(TypedValue), Encoded).

%% @private
sample_record_type() ->
  avro_record:type("SampleRecord",
                   [ define_field("bool", boolean,
                                  [ {doc, "bool f"}
                                  , {default, avro_primitive:boolean(false)}
                                  ])
                   , define_field("int", int,
                                  [ {doc, "int f"}
                                  , {default, avro_primitive:int(0)}
                                  ])
                   , define_field("long", long,
                                  [ {doc, "long f"}
                                  , {default, avro_primitive:long(42)}
                                  ])
                   , define_field("float", float,
                                  [ {doc, "float f"}
                                  , {default, avro_primitive:float(3.14)}
                                  ])
                   , define_field("double", double,
                                  [ {doc, "double f"}
                                  , {default, avro_primitive:double(6.67221937)}
                                  ])
                   , define_field("bytes", bytes,
                                  [ {doc, "bytes f"}
                                  ])
                   , define_field("string", string,
                                  [ {doc, "string f"}
                                  ])
                   ],
                   [ {namespace, "com.klarna.test.bix"}
                   , {doc, "Record documentation"}]).

%% @private
sample_record() ->
  avro_record:new(sample_record_type(),
    [ {"string", "string"}
    , {"double", 3.14159265358}
    , {"long",   123456789123456789}
    , {"bool",   true}
    , {"int",    100}
    , {"float",  2.718281828}
    , {"bytes",  <<"bytes value">>}
    ]).

%% @private
define_field(Name, Type) ->
  avro_record:define_field(Name, Type).

%% @private
define_field(Name, Type, Opts) ->
  avro_record:define_field(Name, Type, Opts).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
