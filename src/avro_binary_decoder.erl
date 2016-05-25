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
        , decode/4
        , decode_stream/3
        , decode_stream/4
        ]).

-export([ pretty_print_debug_hook/0
        ]).

-include("erlavro.hrl").

%% Decoding hook is a function to be evaluated when decoding:
%% 1. primitives
%% 2. each field/element of complex types.
%%
%% A hook fun can be used to fast skipping undesired data fields of records
%% or undesired data of big maps etc.
%% For example, to dig out only the field named "MyField" in "MyRecord", the
%% hook may probably look like:
%%
%% fun(Type, SubNameOrIndex, Data, DecodeFun) ->
%%      case {avro:get_type_fullname(Type), SubNameOrIndex} of
%%        {"MyRecord.example.com", "MyField"} ->
%%          DecodeFun(Data);
%%        {"MyRecord.example.com", _OtherFields} ->
%%          ignored;
%%        _OtherType ->
%%          DecodeFun(Data)
%%      end
%% end.
%%
%% A hook fun can be used for debug. For example, blow hook should print
%% the decoding stack along the decode function traverses through the bytes.
%%
%% fun(Type, SubNameOrIndex, Data, DecodeFun) ->
%%      SubInfo = case is_integer(SubNameOrIndex) of
%%                  true  -> integer_to_list(SubNameOrIndex);
%%                  false -> SubNameOrIndex
%%                end,
%%      io:format("~s.~s\n", [avro:get_type_name(Type), SubInfo]),
%%      DecodeFun(Data)
%% end
%%
%% A hook can also be used as a dirty patch to fix some corrupted data.
%%
-type hook_fun() :: fun((avro_type(), string() | integer(), binary(),
                        fun((binary()) -> term())) -> term()).
%% By default, the hook fun does nothing else but calling the decode function.
-define(DEFAULT_HOOK,
        fun(__Type__, __SubNameOrId__, Data, DecodeFun) -> DecodeFun(Data) end).

-type lkup_fun() :: fun((string()) -> avro_type()).

%%%_* APIs =====================================================================

%% @doc decode/4 equivalent with default hook fun.
-spec decode(iodata(), string() | avro_type(), lkup_fun()) -> term().
decode(IoData, Type, Lkup) when is_function(Lkup, 1) ->
  decode(IoData, Type, Lkup, ?DEFAULT_HOOK).

%% @doc Decode bytes into unwrapped avro value, assuming the input bytes
%% matches the given schema without tailing bytes.
%% @end
-spec decode(iodata(), string() | avro_type(),
             lkup_fun(), hook_fun()) -> term().
decode(IoData, Type, Lkup, Hook) when is_function(Lkup, 1) ->
  %% return decoded value as raw erlang term directly
  {Value, <<>>} = do_decode(IoData, Type, Lkup, Hook),
  Value.

%% @doc decode_stream/4 equivalent with default hook fun.
-spec decode_stream(iodata(), string() | avro_type(),
                    fun((string()) -> avro_type())) -> term().
decode_stream(IoData, Type, Lkup) when is_function(Lkup, 1) ->
  decode_stream(IoData, Type, Lkup, ?DEFAULT_HOOK).

%% @doc Decode the header of a byte stream, return unwrapped value and tail
%% bytes in a tuple.
%% @end
decode_stream(IoData, Type, Lkup, Hook) when is_function(Lkup, 1) ->
  do_decode(IoData, Type, Lkup, Hook).

%% @doc Return a function to be used as the decode hook.
%% The hook prints the type tree with indentation, and the leaf values.
%% @end
-spec pretty_print_debug_hook() -> hook_fun().
pretty_print_debug_hook() ->
  fun(T, SubInfo, Data, DecodeFun) ->
    Name = avro:get_type_fullname(T),
    Indentation =
      case get(avro_binary_decoder_pp_indentation) of
        undefined -> 0;
        Indentati -> Indentati
      end,
    IndentationStr = lists:duplicate(Indentation * 2, $\s),
    ToPrint =
      [ IndentationStr
      , Name
      , case SubInfo of
          []                   -> ": ";
          I when is_integer(I) -> [$., integer_to_list(I), "\n"];
          S                    -> [$., S, "\n"]
        end
      ],
    io:format("~s", [ToPrint]),
    _ = put(avro_binary_decoder_pp_indentation, Indentation + 1),
    {Result, Tail} = DecodeFun(Data),
    %% print empty array and empty map
    case SubInfo =/= [] andalso Result =:= [] of
      true  -> io:format("~s  []\n", [IndentationStr]);
      false -> ok
    end,
    %% print the value if it's a leaf in the type tree
    _ = SubInfo =:= [] andalso io:format("~1000000p\n", [Result]),
    _ = put(avro_binary_decoder_pp_indentation, Indentation),
    {Result, Tail}
  end.

%%%_* Internal functions =======================================================

do_decode(IoList, Type, Lkup, Hook) when is_list(IoList) ->
  do_decode(iolist_to_binary(IoList), Type, Lkup, Hook);
do_decode(Bin, TypeName, Lkup, Hook) when is_list(TypeName) ->
  do_decode(Bin, Lkup(TypeName), Lkup, Hook);
do_decode(Bin, Type, Lkup, Hook) when is_function(Hook, 4) ->
  dec(Bin, Type, Lkup, Hook).

dec(Bin, T, _Lkup, Hook) when ?AVRO_IS_PRIMITIVE_TYPE(T) ->
  Hook(T, "", Bin, fun(B) -> prim(B, T#avro_primitive_type.name) end);
dec(Bin, T, Lkup, Hook) when ?AVRO_IS_RECORD_TYPE(T) ->
  FieldTypes = avro_record:get_all_field_types(T),
  {FieldValuesReversed, Tail} =
    lists:foldl(
      fun({FieldName, FieldTypeOrName}, {Values, BinIn}) ->
        {Value, BinOut} =
          Hook(T, FieldName, BinIn,
               fun(B) -> do_decode(B, FieldTypeOrName, Lkup, Hook) end),
        {[{FieldName, Value} | Values], BinOut}
      end, {[], Bin}, FieldTypes),
  FieldValues = lists:reverse(FieldValuesReversed),
  Name = avro:get_type_fullname(T),
  {{Name, FieldValues}, Tail};
dec(Bin, T, _Lkup, Hook) when ?AVRO_IS_ENUM_TYPE(T) ->
  {Index, Tail} = int(Bin),
  Hook(T, Index, Tail,
       fun(B) ->
         Symbol = avro_enum:get_symbol_from_index(T, Index),
         {Symbol, B}
       end);
dec(Bin, T, Lkup, Hook) when ?AVRO_IS_ARRAY_TYPE(T) ->
  ItemsType = avro_array:get_items_type(T),
  ItemDecodeFun =
    fun(Index, BinIn) ->
      Hook(T, Index, BinIn,
           fun(B) -> do_decode(B, ItemsType, Lkup, Hook) end)
    end,
  blocks(Bin, ItemDecodeFun);
dec(Bin, T, Lkup, Hook) when ?AVRO_IS_MAP_TYPE(T) ->
  ItemsType = avro_map:get_items_type(T),
  ItemDecodeFun =
    fun(_Index, BinIn) ->
      {Key, Tail1} = prim(BinIn, "string"),
      {Value, Tail} =
        Hook(T, Key, Tail1,
             fun(B) -> do_decode(B, ItemsType, Lkup, Hook) end),
      {{Key, Value}, Tail}
    end,
  blocks(Bin, ItemDecodeFun);
dec(Bin, T, Lkup, Hook) when ?AVRO_IS_UNION_TYPE(T) ->
  {Index, Tail} = long(Bin),
  {ok, ChildType} = avro_union:lookup_child_type(T, Index),
  Hook(T, Index, Tail,
       fun(B) -> do_decode(B, ChildType, Lkup, Hook) end);
dec(Bin, T, _Lkup, Hook) when ?AVRO_IS_FIXED_TYPE(T) ->
  Hook(T, "", Bin,
       fun(B) ->
          Size = avro_fixed:get_size(T),
          <<Value:Size/binary, Tail/binary>> = B,
          {Value, Tail}
       end).

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
  {binary_to_list(Bytes), Tail}.

bytes(Bin) ->
  {Size, Rest} = long(Bin),
  <<Bytes:Size/binary, Tail/binary>> = Rest,
  {Bytes, Tail}.

blocks(Bin, ItemDecodeFun) ->
  blocks(Bin, ItemDecodeFun, _Index = 1, _Acc = []).

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
            {_Size, Rest_} = long(Rest),
            {-Count0, Rest_};
          false ->
            {Count0, Rest}
        end,
      block(Tail0, ItemDecodeFun, Index, Acc, Count)
  end.

block(Bin, ItemDecodeFun, Index, Acc, 0) ->
  blocks(Bin, ItemDecodeFun, Index, Acc);
block(Bin, ItemDecodeFun, Index, Acc, Count) ->
  {Item, Tail} = ItemDecodeFun(Index, Bin),
  block(Tail, ItemDecodeFun, Index+1, [Item | Acc], Count-1).

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
    , avro_record:define_field("array", avro_array:type(avro_primitive:long_type()),
			       [ {doc, "array f"}
			       ])
    , avro_record:define_field("map", avro_map:type(avro_primitive:long_type()),
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
  {Name, Fields} = decode_t(sample_record_binary(), sample_record_type()),
  ?assertEqual(Name, "com.klarna.test.bix.SampleRecord"),
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
  {Name, Fields} = decode(Binary, Schema, Lkup, pretty_print_debug_hook()),
  ?assertEqual(Name, "com.klarna.test.bix.SampleRecord"),
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


-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
