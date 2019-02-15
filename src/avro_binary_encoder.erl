%% coding: latin-1
%%%-----------------------------------------------------------------------------
%%%
%%% Copyright (c) 2016-2018 Klarna Bank AB (publ)
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
%%%-----------------------------------------------------------------------------

-module(avro_binary_encoder).

%% APIs for typed data encoding
-export([ encode_value/1
        , encode/3
        ]).

-include("avro_internal.hrl").

%% Exported for test
-export([ int/1
        , long/1
        , string/1
        , zigzag/2
        ]).

-type index() :: non_neg_integer(). %% zero based

%%%_* APIs =====================================================================

%% @doc Encode avro value in binary format.
-spec encode_value(avro_value()) -> iodata().
encode_value(?AVRO_ENCODED_VALUE_BINARY(_Type, _Value = Encoded)) ->
  Encoded;
encode_value(V) when ?IS_PRIMITIVE_TYPE(?AVRO_VALUE_TYPE(V)) ->
  encode_prim(?AVRO_VALUE_TYPE(V), ?AVRO_VALUE_DATA(V));
encode_value(Record) when ?IS_RECORD_VALUE(Record) ->
  [encode_value(X) || {_FieldName, X} <- avro_record:to_list(Record)];
encode_value(Enum) when ?IS_ENUM_VALUE(Enum) ->
  int(avro_enum:get_index(Enum));
encode_value(Array) when ?IS_ARRAY_VALUE(Array) ->
  Count = length(?AVRO_VALUE_DATA(Array)),
  block(Count, [encode_value(I) || I <- ?AVRO_VALUE_DATA(Array)]);
encode_value(Map) when ?IS_MAP_VALUE(Map) ->
  KvList = avro_map:to_list(Map),
  Count  = length(KvList),
  block(Count, [[string(K), encode_value(V)] || {K, V} <- KvList]);
encode_value(Fixed) when ?IS_FIXED_VALUE(Fixed) ->
  ?AVRO_VALUE_DATA(Fixed);
encode_value(Union) when ?IS_UNION_VALUE(Union) ->
  TypedData = ?AVRO_VALUE_DATA(Union),
  Index = avro_union:get_child_type_index(Union),
  [long(Index), encode_value(TypedData)].

%% @doc Encode unwrapped (raw) values directly without (possibilly
%% recursive) type info wrapped with values.
%% i.e. data can be recursive, but recursive types are resolved by
%% schema lookup
%% @end
-spec encode(avro:schema_all(), type_or_name(), avro_value() | avro:in()) ->
        iodata().
encode(Sc, Type, Input) ->
  Lkup = avro_util:ensure_lkup_fun(Sc),
  do_encode(Lkup, Type, Input).

%%%_* Internal functions =======================================================

%% Tested in OTP-21, dialyzer had trouble understanding the 3 arg
%% for the call to enc/3.
-dialyzer({nowarn_function, [do_encode/3]}).
-spec do_encode(lkup_fun(), type_or_name(), avro_value() | avro:in()) ->
        iodata().
do_encode(Lkup, Type, #avro_value{type = T} = V) ->
  case avro:is_same_type(Type, T) of
    true  -> encode_value(V);
    false -> enc(Lkup, Type, V) %% try deeper
  end;
do_encode(Lkup, TypeName, Value) when ?IS_NAME_RAW(TypeName) ->
  enc(Lkup, Lkup(?NAME(TypeName)), Value);
do_encode(Lkup, Type, Value) ->
  enc(Lkup, Type, Value).

-spec enc(schema_store() | lkup_fun(), type_or_name(),
          avro_value() | avro:in()) -> iodata().
enc(_Lkup, Type, Value) when ?IS_PRIMITIVE_TYPE(Type) ->
  {ok, AvroValue} = avro:cast(Type, Value),
  encode_value(AvroValue);
enc(Lkup, Type, Value) when ?IS_RECORD_TYPE(Type) ->
  avro_record:encode(Type, Value,
    fun(_FN, FT, FV) -> encode(Lkup, FT, FV) end);
enc(_Lkup, Type, Value) when ?IS_ENUM_TYPE(Type) ->
  int(avro_enum:get_index(Type, Value));
enc(Lkup, Type, Value) when ?IS_ARRAY_TYPE(Type) ->
  Count = length(Value),
  Encoded = avro_array:encode(Type, Value,
    fun(IType, Item) -> encode(Lkup, IType, Item) end),
  block(Count, Encoded);
enc(Lkup, Type, Value) when ?IS_MAP_TYPE(Type) ->
  Encoded = avro_map:encode(Type, Value,
    fun(IType, K, V) -> [string(K), encode(Lkup, IType, V)] end),
  Count = case is_map(Value) of
    true -> maps:size(Value);
    false -> length(Value)
  end,
  block(Count, Encoded);
enc(_Lkup, Type, Value) when ?IS_FIXED_TYPE(Type) ->
  %% force binary size check for the value
  encode_value(avro_fixed:new(Type, Value));
enc(Lkup, Type, Union) when ?IS_UNION_TYPE(Type) ->
  avro_union:encode(Type, Union,
    fun(MemberT, Value, Index) ->
      [long(Index), encode(Lkup, MemberT, Value)]
    end).

-spec encode_prim(avro_type(), avro:in()) -> iodata().
encode_prim(T, _) when ?IS_NULL_TYPE(T)    -> null();
encode_prim(T, V) when ?IS_BOOLEAN_TYPE(T) -> bool(V);
encode_prim(T, V) when ?IS_INT_TYPE(T)     -> int(V);
encode_prim(T, V) when ?IS_LONG_TYPE(T)    -> long(V);
encode_prim(T, V) when ?IS_FLOAT_TYPE(T)   -> float(V);
encode_prim(T, V) when ?IS_DOUBLE_TYPE(T)  -> double(V);
encode_prim(T, V) when ?IS_BYTES_TYPE(T)   -> bytes(V);
encode_prim(T, V) when ?IS_STRING_TYPE(T)  -> string(V).

%% Encode blocks, for arrays and maps
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
-spec block(index(), iodata()) -> iodata().
block(0, []) -> [0];
block(Count, Payload) when is_binary(Payload) ->
  Header = iolist_to_binary([long(-Count), long(size(Payload))]),
  [Header, Payload, 0];
block(Count, Payload) ->
  block(Count, iolist_to_binary(Payload)).

-spec null() -> binary().
null() -> <<>>.

-spec bool(boolean()) -> <<_:8>>.
bool(false) -> <<0>>;
bool(true)  -> <<1>>.

-spec int(integer()) -> iodata().
int(Int) ->
  ZzInt = zigzag(int, Int),
  varint(ZzInt).

-spec long(integer()) -> iodata().
long(Long) ->
  ZzLong = zigzag(long, Long),
  varint(ZzLong).

-compile({no_auto_import, [float/1]}).
-spec float(float()) -> binary().
float(Float) when is_float(Float) ->
  <<Float:32/little-float>>.

-spec double(float()) -> binary().
double(Double) when is_float(Double) ->
  <<Double:64/little-float>>.

-spec bytes(binary()) -> iodata().
bytes(Data) when is_binary(Data) ->
  [long(byte_size(Data)), Data].

-spec string(atom() | iodata()) -> iodata().
string(Atom) when is_atom(Atom) ->
  string(atom_to_binary(Atom, utf8));
string(String) when is_list(String) ->
  %% NOTE: not unicode:chardata_to_binary(String)
  %% we do not want to deal with utf8 in erlavro
  string(iolist_to_binary(String));
string(String) when is_binary(String) ->
  [long(size(String)), String].

%% ZigZag encode/decode
%% https://developers.google.com/protocol-buffers/docs/encoding?&csw=1#types
-compile({inline, [zigzag/2]}).
-spec zigzag(int | long, integer()) -> integer().
zigzag(int, Int)  -> (Int bsl 1) bxor (Int bsr 31);
zigzag(long, Int) -> (Int bsl 1) bxor (Int bsr 63).

%% Variable-length format
%% http://lucene.apache.org/core/3_5_0/fileformats.html#VInt
-spec varint(integer()) -> iodata().
varint(I) when I =< 127 -> [I];
varint(I) -> [128 + (I band 127) | varint(I bsr 7)].

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
