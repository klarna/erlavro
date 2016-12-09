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

-include("avro_internal.hrl").

-ifdef(TEST).
-export([string/1, int/1, long/1, zigzag/2]).
-endif.

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
-spec encode(schema_store() | lkup_fun(), avro_type_or_name(), term()) ->
        iodata().
encode(Store, TypeName, Value) when not is_function(Store) ->
  Lkup = ?AVRO_SCHEMA_LOOKUP_FUN(Store),
  encode(Lkup, TypeName, Value);
encode(Lkup, TypeName, Value) when ?IS_NAME(TypeName) ->
  encode(Lkup, Lkup(TypeName), Value);
encode(_Lkup, Type, Value) when ?AVRO_IS_PRIMITIVE_TYPE(Type) ->
  {ok, AvroValue} = avro:cast(Type, Value),
  encode_value(AvroValue);
encode(Lkup, Type, Value) when ?AVRO_IS_RECORD_TYPE(Type) ->
  avro_record:encode(Type, Value,
    fun({_, FT, FV}) -> encode(Lkup, FT, FV) end);
encode(_Lkup, Type, Value) when ?AVRO_IS_ENUM_TYPE(Type) ->
  int(avro_enum:get_index(Type, Value));
encode(Lkup, Type, Value) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  Count = length(Value),
  Encoded = avro_array:encode(Type, Value,
    fun(IType, Item) -> encode(Lkup, IType, Item) end),
  block(Count, Encoded);
encode(Lkup, Type, Value) when ?AVRO_IS_MAP_TYPE(Type) ->
  Encoded = avro_map:encode(Type, Value,
    fun(IType, K, V) -> [string(K), encode(Lkup, IType, V)] end),
  Count = length(Value),
  block(Count, Encoded);
encode(_Lkup, Type, Value) when ?AVRO_IS_FIXED_TYPE(Type) ->
  %% force binary size check for the value
  encode_value(avro_fixed:new(Type, Value));
encode(Lkup, Type, Union) when ?AVRO_IS_UNION_TYPE(Type) ->
  avro_union:encode(Type, Union,
    fun(MemberT, Value, Index) ->
      [long(Index), encode(Lkup, MemberT, Value)]
    end).

%%%_* Internal functions =======================================================

%% @private
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

%% @private
null() -> <<>>.

%% @private
bool(false) -> <<0>>;
bool(true)  -> <<1>>.

%% @private
int(Int) ->
  Zz_int = zigzag(int, Int),
  varint(Zz_int).

%% @private
long(Long) ->
  Zz_long = zigzag(long, Long),
  varint(Zz_long).

%% @private
-compile({no_auto_import,[float/1]}).
float(Float) when is_float(Float) ->
  <<Float:32/little-float>>.

%% @private
double(Double) when is_float(Double) ->
  <<Double:64/little-float>>.

%% @private
bytes(Data) when is_binary(Data) ->
  [long(byte_size(Data)), Data].

%% @private
string(Data) when is_list(Data) ->
  [long(length(Data)), list_to_binary(Data)].

%% @private
%% ZigZag encode/decode
%% https://developers.google.com/protocol-buffers/docs/encoding?&csw=1#types
zigzag(int, Int)  -> (Int bsl 1) bxor (Int bsr 31);
zigzag(long, Int) -> (Int bsl 1) bxor (Int bsr 63).

%% @private
%% Variable-length format
%% http://lucene.apache.org/core/3_5_0/fileformats.html#VInt
varint(I) ->
  H = I bsr 7,
  L = I band 127,
  case H =:= 0 of
    true  -> [L];
    false -> [128 + L | varint(H)]
  end.


%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
