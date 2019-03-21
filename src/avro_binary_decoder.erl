%% coding: latin-1
%%%-----------------------------------------------------------------------------
%%%
%%% Copyright (c) 2016-2018 Klarna AB
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
%%% Decode Avro values from binary format according to Avro 1.7.5
%%% specification.
%%%
%%% Schema is written following parsing canonical form recommendations
%%% but keeps all information (attributes are kept even if they are
%%% not relevant for parsing).
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro_binary_decoder).

-export([ decode/3
        , decode/4
        , decode_stream/3
        , decode_stream/4
        ]).

%% Exported for test
-export([zigzag/1]).

-include("avro_internal.hrl").

-type hook() :: decoder_hook_fun().
-type index() :: pos_integer().
-type block_item_decode_fun() ::
        fun((index(), binary()) -> {avro:out(), binary()}).
-type decoder_options() :: map().

%%%_* APIs =====================================================================

%% @doc decode/4 equivalent with default hook fun.
-spec decode(iodata(), type_or_name(),
             schema_store() | lkup_fun()) -> avro:out().
decode(IoData, Type, StoreOrLkupFun) ->
  decode(IoData, Type, StoreOrLkupFun, avro:make_decoder_options([])).


%% @doc Decode bytes into unwrapped avro value, assuming the input bytes
%% matches the given schema without tailing bytes.
%% @end
-spec decode(iodata(), type_or_name(),
             schema_store() | lkup_fun(),
             decoder_options()) -> avro:out().
decode(IoData, Type, StoreOrLkupFun, Options) ->
  %% return decoded value as raw erlang term directly
  Lkup = avro_util:ensure_lkup_fun(StoreOrLkupFun),
  {Value, <<>>} = do_decode(IoData, Type, Lkup, Options),
  Value.

%% @doc decode_stream/4 equivalent with default hook fun.
-spec decode_stream(iodata(), type_or_name(),
                    schema_store() | lkup_fun()) ->
        {avro:out(), binary()}.
decode_stream(IoData, Type, StoreOrLkupFun) ->
  decode_stream(IoData, Type, StoreOrLkupFun, ?DEFAULT_DECODER_HOOK).

%% @doc Decode the header of a byte stream, return unwrapped value and tail
%% bytes in a tuple.
%% @end
-spec decode_stream(iodata(), type_or_name(),
                    schema_store() | lkup_fun(), hook()) ->
        {avro:out(), binary()}.
decode_stream(IoData, Type, StoreOrLkupFun, Hook) ->
  do_decode(IoData, Type, avro_util:ensure_lkup_fun(StoreOrLkupFun),
            avro:make_decoder_options([{hook, Hook}])).

%%%_* Internal functions =======================================================

%% @private
-spec do_decode(iodata(), type_or_name(), lkup_fun(),
                decoder_options()) -> {avro:out(), binary()}.
do_decode(IoData, Type, Lkup, Options) when is_list(IoData) ->
  do_decode(iolist_to_binary(IoData), Type, Lkup, Options);
do_decode(Bin, TypeName, Lkup, Options) when ?IS_NAME_RAW(TypeName) ->
  do_decode(Bin, Lkup(?NAME(TypeName)), Lkup, Options);
do_decode(Bin, Type, Lkup,
          #{hook := Hook} = Options) when is_function(Hook, 4) ->
  dec(Bin, Type, Lkup, Options).

%% @private
-spec dec(binary(), avro_type(), lkup_fun(),
          decoder_options()) -> {avro:out(), binary()}.
dec(Bin, T, _Lkup, #{hook := Hook}) when ?IS_PRIMITIVE_TYPE(T) ->
  Hook(T, "", Bin, fun(B) -> prim(B, T#avro_primitive_type.name) end);
dec(Bin, T, Lkup, #{hook := Hook} = Options) when ?IS_RECORD_TYPE(T) ->
  Hook(T, none, Bin, fun(B) -> dec_record(B, T, Lkup, Options) end);
dec(Bin, T, _Lkup, #{hook := Hook}) when ?IS_ENUM_TYPE(T) ->
  {Index, Tail} = int(Bin),
  Hook(T, Index, Tail,
       fun(B) ->
         Symbol = avro_enum:get_symbol_from_index(T, Index),
         {Symbol, B}
       end);
dec(Bin, T, Lkup, Options) when ?IS_ARRAY_TYPE(T) ->
  ItemsType = avro_array:get_items_type(T),
  ItemDecodeFun =
    fun(Index, BinIn) ->
      dec_item(T, Index, ItemsType, BinIn, Lkup, Options)
    end,
  blocks(Bin, ItemDecodeFun);
dec(Bin, T, Lkup,
    #{map_type := MapType} = Options) when ?IS_MAP_TYPE(T) ->
  ItemsType = avro_map:get_items_type(T),
  ItemDecodeFun =
    fun(_Index, BinIn) ->
      {Key, Tail1} = prim(BinIn, ?AVRO_STRING),
      {Value, Tail} = dec_item(T, Key, ItemsType, Tail1, Lkup, Options),
      {{Key, Value}, Tail}
    end,
  {KVs, Tail} = blocks(Bin, ItemDecodeFun),
  case MapType of
    proplist -> {KVs, Tail};
    map -> {maps:from_list(KVs), Tail}
  end;
dec(Bin, T, Lkup, Options) when ?IS_UNION_TYPE(T) ->
  {Index, Tail} = long(Bin),
  {ok, MemberType} = avro_union:lookup_type(Index, T),
  dec_item(T, Index, MemberType, Tail, Lkup, Options);
dec(Bin, T, _Lkup, #{hook := Hook}) when ?IS_FIXED_TYPE(T) ->
  Hook(T, "", Bin,
       fun(B) ->
          Size = avro_fixed:get_size(T),
          <<Value:Size/binary, Tail/binary>> = B,
          {Value, Tail}
       end).

%% @private
-spec dec_record(binary(), record_type(), lkup_fun(),
                 decoder_options()) -> {avro:out(), binary()}.
dec_record(Bin, T, Lkup, #{record_type := RecordType} = Options) ->
  FieldTypes = avro_record:get_all_field_types(T),
  {FieldValuesReversed, Tail} =
    lists:foldl(
      fun({FieldName, FieldType}, {Values, BinIn}) ->
        {Value, BinOut} = dec_item(T, FieldName, FieldType,
                                   BinIn, Lkup, Options),
        {[{FieldName, Value} | Values], BinOut}
      end, {[], Bin}, FieldTypes),
  FieldValues1 = case RecordType of
    proplist -> lists:reverse(FieldValuesReversed);
    map -> maps:from_list(FieldValuesReversed)
  end,
  {FieldValues1, Tail}.

%% @private Common decode logic for map/array items, union members,
%% and record fields.
%% @end
-spec dec_item(avro_type(), name() | non_neg_integer(), type_or_name(),
               binary(), lkup_fun(), decoder_options()) ->
                  {avro:out(), binary()}.
dec_item(ParentType, ItemId, ItemsType, Input, Lkup,
         #{hook := Hook} = Options) ->
  Hook(ParentType, ItemId, Input,
       fun(B) -> do_decode(B, ItemsType, Lkup, Options) end).

%% @private Decode primitive values.
%% NOTE: keep all binary decoding exceptions to error:{badmatch, _}
%%       to simplify higher level try catches when detecting error
%% @end
-spec prim(binary(), _PrimitiveName :: name()) -> {avro:out(), binary()}.
prim(Bin, ?AVRO_NULL) ->
  {null, Bin};
prim(Bin, ?AVRO_BOOLEAN) ->
  <<Bool:8, Rest/binary>> = Bin,
  {Bool =:= 1, Rest};
prim(Bin, ?AVRO_INT) ->
  int(Bin);
prim(Bin, ?AVRO_LONG) ->
  long(Bin);
prim(Bin, ?AVRO_FLOAT) ->
  <<Float:32/little-float, Rest/binary>> = Bin,
  {Float, Rest};
prim(Bin, ?AVRO_DOUBLE) ->
  <<Float:64/little-float, Rest/binary>> = Bin,
  {Float, Rest};
prim(Bin, ?AVRO_BYTES) ->
  bytes(Bin);
prim(Bin, ?AVRO_STRING) ->
  bytes(Bin).

%% @private
-spec bytes(binary()) -> {binary(), binary()}.
bytes(Bin) ->
  {Size, Rest} = long(Bin),
  <<Bytes:Size/binary, Tail/binary>> = Rest,
  {Bytes, Tail}.

%% @private
-spec blocks(binary(), block_item_decode_fun()) -> {[avro:out()], binary()}.
blocks(Bin, ItemDecodeFun) ->
  blocks(Bin, ItemDecodeFun, _Index = 1, _Acc = []).

%% @private
-spec blocks(binary(), block_item_decode_fun(), index(), [avro:out()]) ->
        {[avro:out()], binary()}.
blocks(Bin, ItemDecodeFun, Index, Acc) ->
  {Count0, Rest} = long(Bin),
  case Count0 =:= 0 of
    true ->
      %% a serial of blocks ends with 0
      {lists:reverse(Acc), Rest};
    false ->
      {Count, Tail0} =
        case Count0 < 0 of
          true ->
            %% block start with negative count number
            %% is followed by the block size in bytes
            %% here we simply discard the size info
            {_Size, Rest1} = long(Rest),
            {-Count0, Rest1};
          false ->
            {Count0, Rest}
        end,
      block(Tail0, ItemDecodeFun, Index, Acc, Count)
  end.

%% @private
-spec block(binary(), block_item_decode_fun(),
            index(), [avro:out()], non_neg_integer()) ->
              {[avro:out()], binary()}.
block(Bin, ItemDecodeFun, Index, Acc, 0) ->
  blocks(Bin, ItemDecodeFun, Index, Acc);
block(Bin, ItemDecodeFun, Index, Acc, Count) ->
  {Item, Tail} = ItemDecodeFun(Index, Bin),
  block(Tail, ItemDecodeFun, Index + 1, [Item | Acc], Count-1).

%% @private
-spec int(binary()) -> {integer(), binary()}.
int(Bin) -> zigzag(varint(Bin, 0, 0, 32)).

%% @private
-spec long(binary()) -> {integer(), binary()}.
long(Bin) -> zigzag(varint(Bin, 0, 0, 64)).

%% @private
-spec zigzag({integer(), binary()} | integer()) ->
        {integer(), binary()} | integer().
zigzag({Int, TailBin}) -> {zigzag(Int), TailBin};
zigzag(Int)            -> (Int bsr 1) bxor -(Int band 1).

%% @private
-spec varint(binary(), integer(), integer(), integer()) ->
        {integer(), binary()}.
varint(Bin, Acc, AccBits, MaxBits) ->
  <<Tag:1, Value:7, Tail/binary>> = Bin,
  true = (AccBits < MaxBits), %% assert
  NewAcc = (Value bsl AccBits) bor Acc,
  case Tag =:= 0 of
    true  -> {NewAcc, Tail};
    false -> varint(Tail, NewAcc, AccBits + 7, MaxBits)
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
