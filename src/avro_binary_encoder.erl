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
%%% @author Fabrizio Iacolucci <fabrizio.iacolucci@klarna.com>
%%% @doc
%%% Encodes Avro values to binary format according to Avro 1.7.5
%%% specification.
%%%
%%% Schema is written following parsing canonical form recommendations
%%% but keeps all information (attributes are kept even if they are
%%% not relevant for parsing).
%%% @end
%%%-------------------------------------------------------------------
-module(avro_binary_encoder).

%% API
-export([encode_value/1]).

-include("erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Encode avro value in binary format.
%% @end
-spec encode_value(avro_value() | avro_encoded_value()) -> iodata().

encode_value(Value) when ?AVRO_IS_NULL_VALUE(Value) ->
  <<>>;

encode_value(Value) when ?AVRO_IS_BOOLEAN_VALUE(Value) ->
  do_encode_bool(?AVRO_VALUE_DATA(Value));

encode_value(Value) when ?AVRO_IS_INT_VALUE(Value) ->
  do_encode_int(?AVRO_VALUE_DATA(Value));

encode_value(Value) when ?AVRO_IS_LONG_VALUE(Value) ->
  do_encode_long(?AVRO_VALUE_DATA(Value));

encode_value(Value) when ?AVRO_IS_FLOAT_VALUE(Value) ->
  do_encode_float(?AVRO_VALUE_DATA(Value));

encode_value(Value) when ?AVRO_IS_DOUBLE_VALUE(Value) ->
  do_encode_double(?AVRO_VALUE_DATA(Value));

encode_value(Value) when ?AVRO_IS_BYTES_VALUE(Value) ->
  do_encode_binary(?AVRO_VALUE_DATA(Value));

encode_value(Value) when ?AVRO_IS_STRING_VALUE(Value) ->
  do_encode_string(?AVRO_VALUE_DATA(Value));

encode_value(Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
  [encode_value(X) || {_, X} <- avro_record:to_list(Record)];

encode_value(Enum) when ?AVRO_IS_ENUM_VALUE(Enum) ->
  Index = avro_enum:get_index(Enum),
  do_encode_int(Index).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_encode_bool(false) ->
  <<0>>;
do_encode_bool(true) ->
  <<1>>.

do_encode_int(Int) ->
  Zz_int = do_encode_zigzag(int, Int),
  do_encode_varint(<<Zz_int:32>>).

do_encode_long(Long) ->
  Zz_long = do_encode_zigzag(long, Long),
  do_encode_varint(<<Zz_long:64>>).

do_encode_float(Float) when is_float(Float) ->
  <<Float:32/little-float>>.

do_encode_double(Double) when is_float(Double) ->
  <<Double:64/little-float>>.

do_encode_binary(Data) when is_binary(Data) ->
  [do_encode_long(byte_size(Data)), Data].

do_encode_string(Data) when is_list(Data) ->
  [do_encode_long(length(Data)), list_to_binary(Data)].

%% ZigZag encode/decode
%% https://developers.google.com/protocol-buffers/docs/encoding?&csw=1#types
do_encode_zigzag(int, Int) ->
    (Int bsl 1) bxor (Int bsr 31);
do_encode_zigzag(long, Int) ->
    (Int bsl 1) bxor (Int bsr 63).

%% Variable-length format
%% http://lucene.apache.org/core/3_5_0/fileformats.html#VInt

%% 32 bit encode
do_encode_varint(<<0:32>>) -> <<0>>;
do_encode_varint(<<0:25, B1:7>>) -> <<B1>>;
do_encode_varint(<<0:18, B1:7, B2:7>>) ->
    <<1:1, B2:7, B1>>;
do_encode_varint(<<0:11, B1:7, B2:7, B3:7>>) ->
    <<1:1, B3:7, 1:1, B2:7, B1>>;
do_encode_varint(<<0:4, B1:7, B2:7, B3:7, B4:7>>) ->
    <<1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
do_encode_varint(<<B1:4, B2:7, B3:7, B4:7, B5:7>>) ->
    <<1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;

%% 64 bit encode
do_encode_varint(<<0:64>>) -> <<0>>;
do_encode_varint(<<0:57, B1:7>>) -> <<B1>>;
do_encode_varint(<<0:50, B1:7, B2:7>>) ->
    <<1:1, B2:7, B1>>;
do_encode_varint(<<0:43, B1:7, B2:7, B3:7>>) ->
    <<1:1, B3:7, 1:1, B2:7, B1>>;
do_encode_varint(<<0:36, B1:7, B2:7, B3:7, B4:7>>) ->
    <<1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
do_encode_varint(<<0:29, B1:7, B2:7, B3:7, B4:7, B5:7>>) ->
    <<1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
do_encode_varint(<<0:22, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7>>) ->
    <<1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
do_encode_varint(<<0:15, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7>>) ->
    <<1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
do_encode_varint(<<0:8, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7>>) ->
    <<1:1, B8:7, 1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
do_encode_varint(<<0:1, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7, B9:7>>) ->
    <<1:1, B9:7, 1:1, B8:7, 1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
do_encode_varint(<<B1:1, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7, B9:7, B10:7>>) ->
    <<1:1, B10:7, 1:1, B9:7, 1:1, B8:7, 1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

encode_null_test() ->
  BinNull = encode_value(avro_primitive:null()),
  ?assertEqual(<<>>, BinNull).

encode_boolean_test() ->
  BinaryTrue = encode_value(avro_primitive:boolean(true)),
  BinaryFalse = encode_value(avro_primitive:boolean(false)),
  ?assertEqual(<<1>>, BinaryTrue),
  ?assertEqual(<<0>>, BinaryFalse).

encode_int_test() ->
  ?assertEqual(<<8>>, encode_value(avro_primitive:int(4))),
  ?assertEqual(<<128, 1>>, encode_value(avro_primitive:int(64))).

encode_long_test() ->
  BinLong = encode_value(avro_primitive:long(12345678901)),
  ?assertEqual(<<234, 240, 224, 253, 91>>, BinLong).

encode_float_test() ->
  BinFloat = encode_value(avro_primitive:float(3.14159265358)),
  ?assertEqual(<<219, 15, 73, 64>>, BinFloat).

encode_float_precision_lost_test() ->
  %% Warning: implementation of doubles in erlang loses
  %% precision on such numbers.
  ?assertEqual(<<202,27,14,90>>,
	       encode_value(avro_primitive:float(10000000000000001))).

encode_integer_float_test() ->
  ?assertEqual(<<180, 74, 146, 82>>,
	       encode_value(avro_primitive:float(314159265358))).

encode_double_test() ->
  BinDouble = encode_value(avro_primitive:double(3.14159265358)),
  ?assertEqual(<<244, 214, 67, 84, 251, 33, 9, 64>>, BinDouble).

encode_integer_double_test() ->
  ?assertEqual(<<0, 128, 147, 125, 86, 73, 82, 66>>,
	       encode_value(avro_primitive:double(314159265358))).

encode_empty_bytes_test() ->
  BinBytes = encode_value(avro_primitive:bytes(<<>>)),
  ?assertEqual([<<0>>, <<>>], BinBytes).

encode_bytes_test() ->
  BinBytes = encode_value(avro_primitive:bytes(<<0, 1, 100, 255>>)),
  ?assertEqual([<<8>>, <<0, 1, 100, 255>>], BinBytes).

encode_string_test() ->
  BinString = encode_value(avro_primitive:string("Hello, Avro!")),
  ?assertEqual([<<24>>, <<72, 101, 108, 108, 111, 44, 32,
			  65, 118, 114, 111, 33>>], BinString).

encode_string_with_quoting_test() ->
  BinString = encode_value(avro_primitive:string("\"\\")),
  ?assertEqual([<<4>>, <<34, 92>>], BinString).

encode_utf8_string_test() ->
  S = xmerl_ucs:to_utf8("Avro �r popul�r"),
  BinString = encode_value(avro_primitive:string(S)),
  ?assertEqual([<<50>>, <<65, 118, 114, 111, 32, 195, 175, 194,
                          191, 194, 189, 114, 32, 112, 111, 112,
                          117, 108, 195, 175, 194, 191, 194,
                          189, 114>>],
               BinString).

encode_record_test() ->
  BinRecord = encode_value(sample_record()),
  Expected = [<<1>>, <<200,1>>, <<170,252,130,205,245,210,205,182,3>>,
	      <<84, 248, 45, 64>>, <<244, 214, 67, 84, 251, 33, 9, 64>>,
	      [<<22>>, <<98, 121, 116, 101, 115, 32, 118, 97, 108,
			 117, 101>>],
	      [<<24>>, <<115, 116, 114, 105, 110, 103, 32, 118, 97,
			 108, 117, 101>>]],
  ?assertEqual(Expected, BinRecord).

encode_enum_test() ->
  EnumType = avro_enum:type("TestEnum",
                            ["SYMBOL_0", "SYMBOL_1", "SYMBOL_2",
                             "SYMBOL_3", "SYMBOL_4", "SYMBOL_5"],
                            [{namespace, "com.klarna.test.bix"}]),
  EnumValue = ?AVRO_VALUE(EnumType, "SYMBOL_4"),
  BynaryEnum = encode_value(EnumValue),
  ?assertEqual(<<8>>, BynaryEnum).

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
-endif.

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

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
