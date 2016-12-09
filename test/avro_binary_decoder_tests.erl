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
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%%-------------------------------------------------------------------
-module(avro_binary_decoder_tests).
-author("tihon").

-include_lib("eunit/include/eunit.hrl").

%% simple types, never reference
decode_t(Bin, Type) -> avro_binary_decoder:decode(Bin, Type, fun(_) -> exit(error) end).

decode_null_test() ->
  ?assertEqual(null, decode_t(<<>>, avro_primitive:null_type())).

decode_boolean_test() ->
  ?assertEqual(true, decode_t([1], avro_primitive:boolean_type())),
  ?assertEqual(false, decode_t([0], avro_primitive:boolean_type())).

zigzag_test() ->
  ?assertEqual(0, avro_binary_decoder:zigzag(0)),
  ?assertEqual(-1, avro_binary_decoder:zigzag(1)),
  ?assertEqual(1, avro_binary_decoder:zigzag(2)),
  ?assertEqual(-2, avro_binary_decoder:zigzag(3)),
  ?assertEqual(2147483647, avro_binary_decoder:zigzag(4294967294)),
  ?assertEqual(-2147483648, avro_binary_decoder:zigzag(4294967295)).

encode_int_test() ->
  Int = avro_primitive:int_type(),
  Long = avro_primitive:long_type(),
  ?assertEqual(4,    decode_t(<<8>>,      Int)),
  ?assertEqual(-2,   decode_t([<<3>>],    Long)),
  ?assertEqual(-64,  decode_t(<<127>>,    Long)),
  ?assertEqual(64,   decode_t(<<128, 1>>, Int)),
  ?assertEqual(-127, decode_t(<<253, 1>>, Long)),
  ?assertEqual(127,  decode_t(<<254, 1>>, Int)),
  ?assertEqual(128,  decode_t(<<128, 2>>, Int)).

decode_float_test() ->
  Type = avro_primitive:float_type(),
  V_orig = 3.14159265358,
  Typed = avro_primitive:float(V_orig),
  Bin = avro_binary_encoder:encode_value(Typed),
  V_dec = decode_t(Bin, Type),
  ?assert(abs(V_orig - V_dec) < 0.000001).

decode_double_test() ->
  Type = avro_primitive:double_type(),
  V_orig = 3.14159265358,
  Typed = avro_primitive:double(V_orig),
  Bin = avro_binary_encoder:encode_value(Typed),
  V_dec = decode_t(Bin, Type),
  ?assert(abs(V_orig - V_dec) < 0.000000000001).

decode_empty_bytes_test() ->
  ?assertEqual(<<>>, decode_t(<<0>>, avro_primitive:bytes_type())).

decode_string_test() ->
  Str = "Avro is popular",
  Enc = [30, Str],
  ?assertEqual(Str, decode_t(Enc, avro_primitive:string_type())).

decode_utf8_string_test() ->
  Str = "Avro är populär",
  Utf8 = unicode:characters_to_binary(Str, latin1, utf8),
  Enc = [size(Utf8) * 2, Utf8],
  ?assertEqual(binary_to_list(Utf8),
    decode_t(Enc, avro_primitive:string_type())),
  ?assertEqual(Str, unicode:characters_to_list(Utf8, utf8)).

decode_empty_array_test() ->
  Type = avro_array:type(avro_primitive:int_type()),
  ?assertEqual([], decode_t(<<0>>, Type)).

decode_array_test() ->
  Type = avro_array:type(avro_primitive:int_type()),
  TypedValue = avro_array:new(Type, [3, 27]),
  Bin = avro_binary_encoder:encode_value(TypedValue),
  ?assertEqual([3, 27], decode_t(Bin, Type)).

decode_empty_map_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  ?assertEqual([], decode_t([0], Type)).

decode_map_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  Data1 = [4, 2, "a", 0, 2, "b", 2, 0], %% Count + Items
  Data2 = [3, 12, 2, "a", 0, 2, "b", 2, 0], %% -Count + Size + Items
  ?assertEqual([{"a", 0}, {"b", 1}], decode_t(Data1, Type)),
  ?assertEqual([{"a", 0}, {"b", 1}], decode_t(Data2, Type)).

decode_union_test() ->
  Type = avro_union:type([avro_primitive:null_type(),
    avro_primitive:string_type()]),
  Value1 = [0],
  Value2 = [2, 2, "a"],
  ?assertEqual(null, decode_t(Value1, Type)),
  ?assertEqual("a", decode_t(Value2, Type)).

decode_fixed_test() ->
  Type = avro_fixed:type("FooBar", 2),
  ?assertEqual(<<1, 127>>, decode_t(<<1, 127>>, Type)).

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
      , avro_record:define_field("array",
      avro_array:type(avro_primitive:long_type()),
      [ {doc, "array f"}
      ])
      , avro_record:define_field("map",
      avro_map:type(avro_primitive:long_type()),
      [ {doc, "map f"}
      ])
    ],
    [ {namespace, "com.klarna.test.bix"}
      , {doc, "Record documentation"}]).

sample_record_binary() ->
  [<<1>>, %% bool
    <<200,1>>, %% int
    <<170,252,130,205,245,210,205,182,3>>, % long
    <<84, 248, 45, 64>>, % float
    <<244, 214, 67, 84, 251, 33, 9, 64>>, % double
    [22, <<98, 121, 116, 101, 115, 32, 118, 97, 108, 117, 101>>], % bytes
    [12, <<115, 116, 114, 105, 110, 103>>], % string
    [2, 0, 0], %% array
    [0] %% map
  ].

decode_record_test() ->
  Fields = decode_t(sample_record_binary(), sample_record_type()),
  ?assertMatch([ {"bool",   true}
    , {"int",    100}
    , {"long",   123456789123456789}
    , {"float",  _}
    , {"double", _}
    , {"bytes",  <<"bytes value">>}
    , {"string", "string"}
    , {"array",  [0]}
    , {"map",    []}
  ], Fields).

decode_with_hook_test() ->
  Binary = sample_record_binary(),
  Schema = sample_record_type(),
  Lkup = fun(_) -> exit(error) end,
  Hook = avro_util:pretty_print_decoder_hook(),
  Fields = avro_binary_decoder:decode(Binary, Schema, Lkup, Hook),
  ?assertMatch([ {"bool",   true}
    , {"int",    100}
    , {"long",   123456789123456789}
    , {"float",  _}
    , {"double", _}
    , {"bytes",  <<"bytes value">>}
    , {"string", "string"}
    , {"array",  [0]}
    , {"map",    []}
  ], Fields).