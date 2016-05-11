%% coding: latin-1
%%% ============================================================================
%%% Copyright (c) 2016 Klarna AB
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
%%% @doc
%%% Encodes Avro values to binary format according to Avro 1.7.5
%%% specification.
%%%
%%% Schema is written following parsing canonical form recommendations
%%% but keeps all information (attributes are kept even if they are
%%% not relevant for parsing).
%%% @end
%%% ============================================================================
-module(avro_binary_encoder).

%% APIs for typed data encoding
-export([ encode_value/1
        , encode/3
        ]).

-include("erlavro.hrl").

-type lkup_fun() :: fun((string()) -> avro_type()).

%%%_* APIs =====================================================================

%% @doc Encode avro value in binary format.
%% @end
-spec encode_value(avro_value() | avro_encoded_value()) -> iodata().
encode_value(?AVRO_ENCODED_VALUE_BINARY(_Type, _Value = Encoded)) ->
  Encoded;
encode_value(V) when ?AVRO_IS_PRIMITIVE_TYPE(?AVRO_VALUE_TYPE(V)) ->
  encode_prim(?AVRO_VALUE_TYPE(V), ?AVRO_VALUE_DATA(V));
encode_value(Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
  [encode_value(X) || {_FieldName, X} <- avro_record:to_list(Record)];
encode_value(Enum) when ?AVRO_IS_ENUM_VALUE(Enum) ->
  int(avro_enum:get_index(Enum));
encode_value(Array) when ?AVRO_IS_ARRAY_VALUE(Array) ->
  Count = length(?AVRO_VALUE_DATA(Array)),
  block(Count, [encode_value(I) || I <- ?AVRO_VALUE_DATA(Array)]);
encode_value(Map) when ?AVRO_IS_MAP_VALUE(Map) ->
  KvList = avro_map:to_list(Map),
  Count  = length(KvList),
  block(Count, [[string(K), encode_value(V)] || {K, V} <- KvList]);
encode_value(Fixed) when ?AVRO_IS_FIXED_VALUE(Fixed) ->
  ?AVRO_VALUE_DATA(Fixed);
encode_value(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  TypedData = ?AVRO_VALUE_DATA(Union),
  Index = avro_union:get_child_type_index(Union),
  [long(Index), encode_value(TypedData)].

%% @doc Encode unwrapped (raw) values directly without (possibilly
%% recursive) type info wrapped with values.
%% i.e. data can be recursive, but recursive types are resolved by
%% schema lookup
%% @end
-spec encode(lkup_fun(), avro_type_or_name(), term()) -> iodata().
encode(Lkup, TypeName, Value) when is_list(TypeName) ->
  encode(Lkup, Lkup(TypeName), Value);
encode(_Lkup, Type, Value) when ?AVRO_IS_PRIMITIVE_TYPE(Type) ->
  {ok, AvroValue} = avro:cast(Type, Value),
  encode_value(AvroValue);
encode(Lkup, Type, Value) when ?AVRO_IS_RECORD_TYPE(Type) ->
  FieldTypes = avro_record:get_all_field_types(Type),
  TypeFullName = avro:get_type_fullname(Type),
  FieldValues =
    case Value of
      {TypeFullName_, FieldValues_} ->
        TypeFullName_ = TypeFullName, %% assert
        FieldValues_;
      L when is_list(L) ->
        L
    end,
  TypeAndValueList =
    zip_record_field_types_with_value(TypeFullName, FieldTypes, FieldValues),
  [encode(Lkup, FT, FV) || {FT, FV} <- TypeAndValueList];
encode(_Lkup, Type, Value) when ?AVRO_IS_ENUM_TYPE(Type) ->
  int(avro_enum:get_index(Type, Value));
encode(Lkup, Type, Value) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  ItemsType = avro_array:get_items_type(Type),
  Count = length(Value),
  block(Count, [encode(Lkup, ItemsType, Item) || Item <- Value]);
encode(Lkup, Type, Value) when ?AVRO_IS_MAP_TYPE(Type) ->
  ItemsType = avro_map:get_items_type(Type),
  Count = length(Value),
  block(Count, [[string(K), encode(Lkup, ItemsType, V)] || {K, V} <- Value]);
encode(_Lkup, Type, Value) when ?AVRO_IS_FIXED_TYPE(Type) ->
  %% force binary size check for the value
  encode_value(avro_fixed:new(Type, Value));
encode(Lkup, Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  %% encoding union might be quite costy but we'll have to live with it.
  MemberTypes = avro_union:get_types(Type),
  try_encode_union_loop(Lkup, Type, MemberTypes, Value, 0).

%%%_* Internal functions =======================================================
zip_record_field_types_with_value(_Name, [], []) -> [];
zip_record_field_types_with_value(Name, [{FieldName, FieldType} | Rest],
                                  FieldValues0) ->
  {FieldValue, FieldValues} =
    take_record_filed_value(Name, FieldName, FieldValues0, []),
  [ {FieldType, FieldValue}
  | zip_record_field_types_with_value(Name, Rest, FieldValues)
  ].

take_record_filed_value(RecordName, FieldName, [], _) ->
  erlang:error({field_value_not_found, RecordName, FieldName});
take_record_filed_value(RecordName, FieldName, [{Tag, Value} | Rest], Tried) ->
  case Tag =:= FieldName orelse
       (is_atom(Tag) andalso atom_to_list(Tag) =:= FieldName) of
    true ->
      {Value, Tried ++ Rest};
    false ->
      take_record_filed_value(RecordName, FieldName,
                              Rest, [{Tag, Value} | Tried])
  end.

try_encode_union_loop(_Lkup, UnionType, [], Value, _Index) ->
  erlang:error({failed_to_encode_union, UnionType, Value});
try_encode_union_loop(Lkup, UnionType, [MemberT | Rest], Value, Index) ->
  try
    [long(Index), encode(Lkup, MemberT, Value)]
  catch _ : _ ->
    try_encode_union_loop(Lkup, UnionType, Rest, Value, Index + 1)
  end.

encode_prim(T, _) when ?AVRO_IS_NULL_TYPE(T)    -> null();
encode_prim(T, V) when ?AVRO_IS_BOOLEAN_TYPE(T) -> bool(V);
encode_prim(T, V) when ?AVRO_IS_INT_TYPE(T)     -> int(V);
encode_prim(T, V) when ?AVRO_IS_LONG_TYPE(T)    -> long(V);
encode_prim(T, V) when ?AVRO_IS_FLOAT_TYPE(T)   -> float(V);
encode_prim(T, V) when ?AVRO_IS_DOUBLE_TYPE(T)  -> double(V);
encode_prim(T, V) when ?AVRO_IS_BYTES_TYPE(T)   -> bytes(V);
encode_prim(T, V) when ?AVRO_IS_STRING_TYPE(T)  -> string(V).

%% @private Encode blocks, for arrays and maps
%% 1. Blocks start with a 'long' type count
%% 2. If count is negative (abs value for real count), it should be followed by
%%    a 'long' type data size
%% 3. A serial of blocks end with a zero-count block
%%
%% in erlavro implementation, blocks are always encoded with negative count
%% followed by size
%%
%% This block size permits fast skipping through data,
%% e.g., when projecting a record to a subset of its fields.
%%
%% The blocked representation also permits one to read and write arrays/maps
%% larger than can be buffered in memory, since one can start writing items
%% without knowing the full length of the array/map.
%%
%% Although we are not trying to optimise memory usage
%% (hard to do so with current erlavro typing mechanism
%%  because it requires everyting in memory already).
%% This is however benifical when concatinating large lists which have chunks
%% encoded in different processes etc.
%% @end
block(0, []) -> [0];
block(Count, Payload) when is_binary(Payload) ->
  Header = iolist_to_binary([long(-Count), long(size(Payload))]),
  [Header, Payload, 0];
block(Count, Payload) ->
  block(Count, iolist_to_binary(Payload)).

null() -> <<>>.

bool(false) -> <<0>>;
bool(true)  -> <<1>>.

int(Int) ->
  Zz_int = zigzag(int, Int),
  varint(Zz_int).

long(Long) ->
  Zz_long = zigzag(long, Long),
  varint(Zz_long).

-compile({no_auto_import,[float/1]}).
float(Float) when is_float(Float) ->
  <<Float:32/little-float>>.

double(Double) when is_float(Double) ->
  <<Double:64/little-float>>.

bytes(Data) when is_binary(Data) ->
  [long(byte_size(Data)), Data].

string(Data) when is_list(Data) ->
  [long(length(Data)), list_to_binary(Data)].

%% ZigZag encode/decode
%% https://developers.google.com/protocol-buffers/docs/encoding?&csw=1#types
zigzag(int, Int)  -> (Int bsl 1) bxor (Int bsr 31);
zigzag(long, Int) -> (Int bsl 1) bxor (Int bsr 63).

%% Variable-length format
%% http://lucene.apache.org/core/3_5_0/fileformats.html#VInt

varint(I) ->
  H = I bsr 7,
  L = I band 127,
  case H =:= 0 of
    true  -> [L];
    false -> [128 + L | varint(H)]
  end.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

-define(assertBinEq(A,B), ?assertEqual(iolist_to_binary(A),iolist_to_binary(B))).

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
  S = xmerl_ucs:to_utf8("Avro är populär"),
  BinString = encode_value(avro_primitive:string(S)),
  ?assertBinEq([42, <<65,118,114,111,32,195,131,194,164,114,32,
                      112,111,112,117,108,195,131,194,164,114>>], BinString).

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

encode_enum_test() ->
  EnumType = avro_enum:type("TestEnum",
                            ["SYMBOL_0", "SYMBOL_1", "SYMBOL_2",
                             "SYMBOL_3", "SYMBOL_4", "SYMBOL_5"],
                            [{namespace, "com.klarna.test.bix"}]),
  EnumValue = ?AVRO_VALUE(EnumType, "SYMBOL_4"),
  BynaryEnum = encode_value(EnumValue),
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
  ?assertBinEq([0], encode_value(TypedValue)).

encode_array_test() ->
  Type = avro_array:type(avro_primitive:int_type()),
  TypedValue = avro_array:new(Type, [3, 27]),
  ?assertBinEq([3,4,6,54,0], encode_value(TypedValue)).

encode_empty_map_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  TypedValue = avro_map:new(Type, []),
  ?assertBinEq([0], encode_value(TypedValue)).

encode_map_test() ->
  Type = avro_map:type(avro_primitive:int_type()),
  TypedValue = avro_map:new(Type, [{"a", 3}, {"b", 27}]),
  Body = iolist_to_binary([string("a"), int(3), string("b"), int(27)]),
  ?assertBinEq([long(-2),
                long(size(Body)),
                Body,
                0], encode_value(TypedValue)).

encode_union_test() ->
  Type = avro_union:type([avro_primitive:null_type(),
                          avro_primitive:string_type()]),
  Value1 = avro_union:new(Type, null),
  Value2 = avro_union:new(Type, "a"),
  ?assertBinEq([0], encode_value(Value1)),
  ?assertBinEq([long(1), long(1), 97], encode_value(Value2)).

encode_fixed_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Value = avro_fixed:new(Type, <<1,127>>),
  ?assertBinEq(<<1, 127>>, encode_value(Value)).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
