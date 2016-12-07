%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Dec 2016 10:28 AM
%%%-------------------------------------------------------------------
-module(avro_binary_encoder_tests).
-author("tihon").

-include("erlavro.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(assertBinEq(A,B),
  ?assertEqual(iolist_to_binary(A),iolist_to_binary(B))).

encode_null_test() ->
  BinNull = avro_binary_encoder:encode_value(avro_primitive:null()),
  ?assertBinEq(<<>>, BinNull).

encode_boolean_test() ->
  BinaryTrue = avro_binary_encoder:encode_value(avro_primitive:boolean(true)),
  BinaryFalse = avro_binary_encoder:encode_value(avro_primitive:boolean(false)),
  ?assertBinEq(<<1>>, BinaryTrue),
  ?assertBinEq(<<0>>, BinaryFalse).

zigzag_test() ->
  ?assertEqual(0, avro_binary_encoder:zigzag(int, 0)),
  ?assertEqual(1, avro_binary_encoder:zigzag(int, -1)),
  ?assertEqual(2, avro_binary_encoder:zigzag(int, 1)),
  ?assertEqual(3, avro_binary_encoder:zigzag(int, -2)),
  ?assertEqual(4294967294, avro_binary_encoder:zigzag(long, 2147483647)),
  ?assertEqual(4294967295, avro_binary_encoder:zigzag(long, -2147483648)).

encode_int_test() ->
  ?assertBinEq(<<8>>, avro_binary_encoder:encode_value(avro_primitive:int(4))),
  ?assertBinEq(<<3>>, avro_binary_encoder:encode_value(avro_primitive:int(-2))),
  ?assertBinEq(<<127>>,    avro_binary_encoder:encode_value(avro_primitive:int(-64))),
  ?assertBinEq(<<128, 1>>, avro_binary_encoder:encode_value(avro_primitive:int(64))),
  ?assertBinEq(<<253, 1>>, avro_binary_encoder:encode_value(avro_primitive:int(-127))),
  ?assertBinEq(<<254, 1>>, avro_binary_encoder:encode_value(avro_primitive:int(127))),
  ?assertBinEq(<<128, 2>>, avro_binary_encoder:encode_value(avro_primitive:int(128))).

encode_long_test() ->
  BinLong = avro_binary_encoder:encode_value(avro_primitive:long(12345678901)),
  ?assertBinEq(<<234, 240, 224, 253, 91>>, BinLong).

encode_float_test() ->
  BinFloat = avro_binary_encoder:encode_value(avro_primitive:float(3.14159265358)),
  ?assertBinEq(<<219, 15, 73, 64>>, BinFloat).

encode_float_precision_lost_test() ->
  %% Warning: implementation of doubles in erlang loses
  %% precision on such numbers.
  ?assertBinEq(<<202,27,14,90>>,
    avro_binary_encoder:encode_value(avro_primitive:float(10000000000000001))).

encode_integer_float_test() ->
  ?assertBinEq(<<180, 74, 146, 82>>,
    avro_binary_encoder:encode_value(avro_primitive:float(314159265358))).

encode_double_test() ->
  BinDouble = avro_binary_encoder:encode_value(avro_primitive:double(3.14159265358)),
  ?assertBinEq(<<244, 214, 67, 84, 251, 33, 9, 64>>, BinDouble).

encode_integer_double_test() ->
  ?assertBinEq(<<0, 128, 147, 125, 86, 73, 82, 66>>,
    avro_binary_encoder:encode_value(avro_primitive:double(314159265358))).

encode_empty_bytes_test() ->
  BinBytes = avro_binary_encoder:encode_value(avro_primitive:bytes(<<>>)),
  ?assertBinEq(<<0>>, iolist_to_binary(BinBytes)).

encode_bytes_test() ->
  BinBytes = avro_binary_encoder:encode_value(avro_primitive:bytes(<<0, 1, 100, 255>>)),
  ?assertBinEq([8, <<0, 1, 100, 255>>], BinBytes).

encode_string_test() ->
  BinString = avro_binary_encoder:encode_value(avro_primitive:string("Hello, Avro!")),
  ?assertBinEq([24, <<72, 101, 108, 108, 111, 44, 32,
    65, 118, 114, 111, 33>>], BinString).

encode_string_with_quoting_test() ->
  BinString = avro_binary_encoder:encode_value(avro_primitive:string("\"\\")),
  ?assertBinEq([4, <<34, 92>>], BinString).

encode_utf8_string_test() ->
  S = unicode:characters_to_binary("Avro är populär", latin1, utf8),
  BinString = avro_binary_encoder:encode_value(avro_primitive:string(binary_to_list(S))),
  ?assertBinEq([34, "Avro ", [195,164], "r popul", [195,164], "r"], BinString).

encode_record_test() ->
  BinRecord = avro_binary_encoder:encode_value(sample_record()),
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

encode_enum_test() ->
  EnumType = avro_enum:type("TestEnum",
    ["SYMBOL_0", "SYMBOL_1", "SYMBOL_2",
      "SYMBOL_3", "SYMBOL_4", "SYMBOL_5"],
    [{namespace, "com.klarna.test.bix"}]),
  EnumValue = ?AVRO_VALUE(EnumType, "SYMBOL_4"),
  BynaryEnum = avro_binary_encoder:encode_value(EnumValue),
  ?assertBinEq([8], BynaryEnum).

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
      ])
    ],
    [ {namespace, "com.klarna.test.bix"}
      , {doc, "Record documentation"}]).

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

encode_empty_array_test() ->
  Type = avro_array:type(avro_primitive:int_type()),
  TypedValue = avro_array:new(Type, []),
  ?assertBinEq([0], avro_binary_encoder:encode_value(TypedValue)).

encode_array_test() ->
  Type = avro_array:type(avro_primitive:int_type()),
  TypedValue = avro_array:new(Type, [3, 27]),
  ?assertBinEq([3,4,6,54,0], avro_binary_encoder:encode_value(TypedValue)).

encode_empty_map_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  TypedValue = avro_map:new(Type, []),
  ?assertBinEq([0], avro_binary_encoder:encode_value(TypedValue)).

encode_map_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  TypedValue = avro_map:new(Type, [{"a", 3}, {"b", 27}]),
  Body = iolist_to_binary([avro_binary_encoder:string("a"),
    avro_binary_encoder:int(3),
    avro_binary_encoder:string("b"),
    avro_binary_encoder:int(27)]),
  ?assertBinEq([avro_binary_encoder:long(-2),
    avro_binary_encoder:long(size(Body)),
    Body,
    0], avro_binary_encoder:encode_value(TypedValue)).

encode_union_test() ->
  Type = avro_union:type([avro_primitive:null_type(),
    avro_primitive:string_type()]),
  Value1 = avro_union:new(Type, null),
  Value2 = avro_union:new(Type, "a"),
  ?assertBinEq([0], avro_binary_encoder:encode_value(Value1)),
  ?assertBinEq([avro_binary_encoder:long(1), avro_binary_encoder:long(1), 97], avro_binary_encoder:encode_value(Value2)).

encode_fixed_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Value = avro_fixed:new(Type, <<1,127>>),
  ?assertBinEq(<<1, 127>>, avro_binary_encoder:encode_value(Value)).

encode_binary_properly_test() ->
  MyRecordType = avro_record:type("MyRecord",
    [avro_record:define_field("f1", avro_primitive:int_type()),
      avro_record:define_field("f2", avro_primitive:string_type())],
    [{namespace, "my.com"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Term = [{"f1", 1}, {"f2", "my string"}],
  Encoded = avro_binary_encoder:encode(Store, "my.com.MyRecord", Term),
  {ok, AvroValue} = avro:cast(MyRecordType, Term),
  EncodedValue = avro_binary_encoder:encode_value(AvroValue),
  ?assertEqual(Encoded, EncodedValue).

encode_enum_properly_test() ->
  UnionType = avro_union:type([avro_primitive:string_type(), avro_primitive:null_type()]),
  Value1 = avro_union:new(UnionType, avro_primitive:null()),
  Value2 = avro_union:new(UnionType, avro_primitive:string("bar")),
  EncodedValue1 = avro_binary_encoder:encode_value(Value1),
  EncodedValue2 = avro_binary_encoder:encode_value(Value2),
  Encoded1 = avro_binary_encoder:encode(fun(_) -> UnionType end, "some_union", null),
  Encoded2 = avro_binary_encoder:encode(fun(_) -> UnionType end, "some_union", "bar"),
  ?assertEqual(EncodedValue1, Encoded1),
  ?assertEqual(EncodedValue2, Encoded2).