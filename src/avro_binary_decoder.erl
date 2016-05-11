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
%%% Decode Avro values from binary format according to Avro 1.7.5
%%% specification.
%%%
%%% Schema is written following parsing canonical form recommendations
%%% but keeps all information (attributes are kept even if they are
%%% not relevant for parsing).
%%% @end
%%% ============================================================================

-module(avro_binary_decoder).

-export([ decode/3
        , decode_stream/3
        ]).

-include("erlavro.hrl").

%%%_* APIs =====================================================================

%% @doc Decode bytes into unwrapped avro value, assuming the input bytes
%% matches the given schema without tailing bytes.
%% @end
-spec decode(iodata(), string() | avro_type(),
             fun((string()) -> avro_type())) -> term().
decode(IoData, Type, LookupFun) ->
  %% return decoded value as raw erlang term directly
  {Value, <<>>} = do_decode(IoData, Type, LookupFun),
  Value.

%% @doc Decode the header of a byte stream, return unwrapped value and tail bytes
%% in a tuple.
%% @end
-spec decode_stream(iodata(), string() | avro_type(),
                    fun((string()) -> avro_type())) -> term().
decode_stream(IoData, Type, LookupFun) ->
  do_decode(IoData, Type, LookupFun).

%%%_* Internal functions =======================================================

do_decode(IoList, Type, LookupFun) when is_list(IoList) ->
  do_decode(iolist_to_binary(IoList), Type, LookupFun);
do_decode(Bin, TypeName, LookupFun) when is_list(TypeName) ->
  Type = LookupFun(TypeName),
  do_decode(Bin, Type, LookupFun);
do_decode(Bin, Type, LookupFun) ->
  dec(Bin, Type, LookupFun).

dec(Bin, T, _Lkup) when ?AVRO_IS_PRIMITIVE_TYPE(T) ->
  prim(Bin, T#avro_primitive_type.name);
dec(Bin, T, Lkup) when ?AVRO_IS_RECORD_TYPE(T) ->
  FieldTypes = avro_record:get_all_field_types(T),
  {FieldValuesReversed, Tail} =
    lists:foldl(
      fun({FieldName, FieldTypeOrName}, {Values, BinIn}) ->
        {Value, BinOut} = do_decode(BinIn, FieldTypeOrName, Lkup),
        {[{FieldName, Value} | Values], BinOut}
      end, {[], Bin}, FieldTypes),
  FieldValues = lists:reverse(FieldValuesReversed),
  Name = avro:get_type_fullname(T),
  {{Name, FieldValues}, Tail};
dec(Bin, T, _Lkup) when ?AVRO_IS_ENUM_TYPE(T) ->
  {Index, Tail} = int(Bin),
  Symbol = avro_enum:get_symbol_from_index(T, Index),
  {Symbol, Tail};
dec(Bin, T, Lkup) when ?AVRO_IS_ARRAY_TYPE(T) ->
  ItemsType = avro_array:get_items_type(T),
  ItemDecodeFun = fun(BinIn) -> do_decode(BinIn, ItemsType, Lkup) end,
  block(Bin, ItemDecodeFun);
dec(Bin, T, Lkup) when ?AVRO_IS_MAP_TYPE(T) ->
  ItemsType = avro_map:get_items_type(T),
  ItemDecodeFun =
    fun(BinIn) ->
      {Key, Tail1} = prim(BinIn, "string"),
      {Value, Tail} = do_decode(Tail1, ItemsType, Lkup),
      {{Key, Value}, Tail}
    end,
  block(Bin, ItemDecodeFun);
dec(Bin, T, Lkup) when ?AVRO_IS_UNION_TYPE(T) ->
  {Index, Tail} = long(Bin),
  {ok, ChildType} = avro_union:lookup_child_type(T, Index),
  do_decode(Tail, ChildType, Lkup);
dec(Bin, T, _Lkup) when ?AVRO_IS_FIXED_TYPE(T) ->
  Size = avro_fixed:get_size(T),
  <<Value:Size/binary, Tail/binary>> = Bin,
  {Value, Tail}.

%% @private Decode primitive values.
%% NOTE: keep all binary decoding exceptions to error:{badmatch, _}
%%       to simplify higher level try catchs when detecting error
%% @end
prim(Bin, "null") ->
  {null, Bin};
prim(Bin, "boolean") ->
  <<Bool:8, Rest/binary>> = Bin,
  {Bool =:= 1, Rest};
prim(Bin, "int") ->
  int(Bin);
prim(Bin, "long") ->
  long(Bin);
prim(Bin, "float") ->
  <<Float:32/little-float, Rest/binary>> = Bin,
  {Float, Rest};
prim(Bin, "double") ->
  <<Float:64/little-float, Rest/binary>> = Bin,
  {Float, Rest};
prim(Bin, "bytes") ->
  bytes(Bin);
prim(Bin, "string") ->
  {Bytes, Tail} = bytes(Bin),
  {unicode:characters_to_list(Bytes, utf8), Tail}.

bytes(Bin) ->
  {Size, Rest} = long(Bin),
  <<Bytes:Size/binary, Tail/binary>> = Rest,
  {Bytes, Tail}.

block(Bin, ItemDecodeFun) -> block(Bin, ItemDecodeFun, _Acc = []).

block(Bin, ItemDecodeFun, Acc) ->
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
            {_Size, Rest_} = long(Rest),
            {-Count0, Rest_};
          false ->
            {Count0, Rest}
        end,
      block(Tail0, ItemDecodeFun, Acc, Count)
  end.

block(Bin, ItemDecodeFun, Acc, 0) ->
  block(Bin, ItemDecodeFun, Acc);
block(Bin, ItemDecodeFun, Acc, Count) ->
  {Item, Tail} = ItemDecodeFun(Bin),
  block(Tail, ItemDecodeFun, [Item | Acc], Count-1).

int(Bin) -> zigzag(varint(Bin, 0, 0, 32)).
long(Bin) -> zigzag(varint(Bin, 0, 0, 64)).

zigzag({Int, TailBin}) -> {zigzag(Int), TailBin};
zigzag(Int)            -> (Int bsr 1) bxor -(Int band 1).

varint(Bin, Acc, AccBits, MaxBits) ->
  <<Tag:1, Value:7, Tail/binary>> = Bin,
  true = (AccBits < MaxBits), %% assert
  NewAcc = (Value bsl AccBits) bor Acc,
  case Tag =:= 0 of
    true  -> {NewAcc, Tail};
    false -> varint(Tail, NewAcc, AccBits + 7, MaxBits)
  end.

%%%_* Tests ====================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

%% simple types, never reference
decode_t(Bin, Type) -> decode(Bin, Type, fun(_) -> exit(error) end).

decode_null_test() ->
  ?assertEqual(null, decode_t(<<>>, avro_primitive:null_type())).

decode_boolean_test() ->
  ?assertEqual(true, decode_t([1], avro_primitive:boolean_type())),
  ?assertEqual(false, decode_t([0], avro_primitive:boolean_type())).

zigzag_test() ->
  ?assertEqual(0, zigzag(0)),
  ?assertEqual(-1, zigzag(1)),
  ?assertEqual(1, zigzag(2)),
  ?assertEqual(-2, zigzag(3)),
  ?assertEqual(2147483647, zigzag(4294967294)),
  ?assertEqual(-2147483648, zigzag(4294967295)).

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
  Utf8 = xmerl_ucs:to_utf8(Str),
  Enc = [42, Utf8],
  ?assertEqual(Str, decode_t(Enc, avro_primitive:string_type())).

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
	 [12, <<115, 116, 114, 105, 110, 103>>] % string
  ].

decode_record_test() ->
  {Name, Fields} = decode_t(sample_record_binary(), sample_record_type()),
  ?assertEqual(Name, "com.klarna.test.bix.SampleRecord"),
  ?assertMatch([ {"bool",   true}
               , {"int",    100}
               , {"long",   123456789123456789}
               , {"float",  _}
               , {"double", _}
               , {"bytes",  <<"bytes value">>}
               , {"string", "string"}
               ], Fields).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
