%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc Avro fixed type implementation.
%%% Internal data for fixed values is a binary value.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_fixed).

%% API
-export([type/2]).
-export([type/3]).
-export([get_size/1]).
-export([new/2]).
-export([get_value/1]).
-export([to_integer/1]).
-export([cast/2]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type(Name, Size) ->
  type(Name, Size, []).

%% Options:
%%   namespace    :: string()
%%   aliases      :: [string()]
%%   enclosing_ns :: string()
type(Name, Size, Opts) ->
  ?ERROR_IF(Size < 1, invalid_size),
  Ns          = avro_util:get_opt(namespace, Opts, ""),
  Aliases     = avro_util:get_opt(aliases, Opts, []),
  EnclosingNs = avro_util:get_opt(enclosing_ns, Opts, ""),
  avro_util:verify_aliases(Aliases),
  Type = #avro_fixed_type
         { name      = Name
         , namespace = Ns
         , aliases   = avro_util:canonicalize_aliases(
                         Aliases, Name, Ns, EnclosingNs)
         , size      = Size
         , fullname  = avro:build_type_fullname(Name, Ns, EnclosingNs)
         },
  avro_util:verify_type(Type),
  Type.

-spec get_size(#avro_fixed_type{}) -> integer().

get_size(#avro_fixed_type{ size = Size }) -> Size.

-spec new(#avro_fixed_type{}, term()) -> avro_value().

new(Type, Value) when ?AVRO_IS_FIXED_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Rec}    -> Rec;
    {error, Err} -> erlang:error(Err)
  end.

-spec get_value(avro_value()) -> binary().

get_value(Value) when ?AVRO_IS_FIXED_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

%% Interprets internal binary as an encoded integer, decodes and returns it.
-spec to_integer(avro_value()) -> integer().

to_integer(Value) when ?AVRO_IS_FIXED_VALUE(Value) ->
  binary:decode_unsigned(?AVRO_VALUE_DATA(Value)).

%% Fixed values can be casted from other fixed values, from integers
%% or from binaries
-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.

cast(Type, Value) when ?AVRO_IS_FIXED_TYPE(Type) ->
  do_cast(Type, Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

pad_binary(Size, Bin) ->
  PadSize = (Size - size(Bin))*8,
  <<0:PadSize, Bin/binary>>.

do_cast(Type, Value) when ?AVRO_IS_FIXED_VALUE(Value) ->
  TargetTypeName = Type#avro_fixed_type.fullname,
  SourceTypeName = (?AVRO_VALUE_TYPE(Value))#avro_fixed_type.fullname,
  if TargetTypeName =:= SourceTypeName -> {ok, Value};
     true                              -> {error, type_name_mismatch}
  end;
do_cast(Type, Value) when is_integer(Value) ->
  #avro_fixed_type{ size = Size } = Type,
  case Value >= 0 andalso Value < (1 bsl (8*Size)) of
    true  ->
      do_cast(Type, pad_binary(Size, binary:encode_unsigned(Value)));
    false ->
      {error, integer_out_of_range}
  end;
do_cast(Type, Value) when is_binary(Value) ->
  #avro_fixed_type{ size = Size } = Type,
  case size(Value) =:= Size of
    true  -> {ok, ?AVRO_VALUE(Type, Value)};
    false -> {error, wrong_binary_size}
  end;
do_cast(Type, Value) ->
  {error, {cast_error, Type, Value}}.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

neg_size_test() ->
  ?assertError(invalid_size, type("FooBar", -1)).

short_create_test() ->
  Type = type("FooBar", 16),
  ?assertEqual("FooBar", avro:get_type_fullname(Type)),
  ?assertEqual(16, get_size(Type)).

full_create_test() ->
  Type = type("FooBar", 16,
              [ {namespace, "name.space"}
              , {aliases, ["Zoo", "Bee"]}
              , {enclosing_ns, "enc.losing"}
              ]),
  ?assertEqual("name.space.FooBar", avro:get_type_fullname(Type)),
  ?assertEqual(16, get_size(Type)).

incorrect_cast_from_fixed_test() ->
  SourceType = type("FooBar", 2),
  SourceValue = new(SourceType, <<1,2>>),
  TargetType = type("BarFoo", 2),
  ?assertEqual({error, type_name_mismatch}, cast(TargetType, SourceValue)).

correct_cast_from_fixed_test() ->
  SourceType = type("FooBar", 2),
  SourceValue = new(SourceType, <<1,2>>),
  TargetType = type("FooBar", 2),
  ?assertEqual({ok, SourceValue}, cast(TargetType, SourceValue)).

incorrect_cast_from_binary_test() ->
  Type = type("FooBar", 2),
  ?assertEqual({error, wrong_binary_size}, cast(Type, <<1,2,3>>)),
  ?assertEqual({error, wrong_binary_size}, cast(Type, <<1>>)).

correct_cast_from_binary_test() ->
  Type = type("FooBar", 2),
  Bin = <<1,2>>,
  ?assertEqual({ok, ?AVRO_VALUE(Type, Bin)}, cast(Type, Bin)).

integer_cast_test() ->
  Type = type("FooBar", 2),
  Value1 = new(Type, 67),   %% 1 byte
  Value2 = new(Type, 1017), %% 2 bytes
  ?assertEqual(67, to_integer(Value1)),
  ?assertEqual(1017, to_integer(Value2)).

get_value_test() ->
  Type = type("FooBar", 2),
  Value = new(Type, <<1,2>>),
  ?assertEqual(<<1,2>>, get_value(Value)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
