%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_array).

%% API
-export([type/1]).
-export([get_items_type/1]).

-export([new/1]).
-export([new/2]).
-export([cast/2]).

-export([prepend/2]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type(SubType) ->
  #avro_array_type{ type = SubType }.

get_items_type(ArrayType) when ?AVRO_IS_ARRAY_TYPE(ArrayType) ->
  ArrayType#avro_array_type.type.

new(Type) ->
  new(Type, []).

new(Type, List) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  case cast(Type, List) of
    {ok, Value} -> Value;
    false       -> erlang:error({avro_error, wrong_cast})
  end.

prepend(Items, Value) when ?AVRO_IS_ARRAY_VALUE(Value) ->
  new(?AVRO_VALUE_TYPE(Value), Items ++ ?AVRO_VALUE_DATA(Value)).

%% Only other Avro array type or erlang list can be casted to arrays
-spec cast(avro_type(), term()) -> {ok, avro_value()} | false.

cast(Type, Value) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  do_cast(Type, Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec do_cast(#avro_array_type{}, avro_value() | [term()])
             -> {ok, avro_value()} | false.

do_cast(Type, Array) when ?AVRO_IS_ARRAY_VALUE(Array) ->
  %% Since we can't compare array types we just cast all items one by one
  %% and see if this succeeds
  do_cast(Type, ?AVRO_VALUE_DATA(Array));
do_cast(Type, Items) when is_list(Items) ->
  #avro_array_type{type = ItemType} = Type,
  case cast_items(ItemType, Items, []) of
    false -> false;
    CastedItems -> {ok, ?AVRO_VALUE(Type, CastedItems)}
  end.

cast_items(_TargetType, [], Acc) ->
  lists:reverse(Acc);
cast_items(TargetType, [Item|H], Acc) ->
  case avro:cast(TargetType, Item) of
    {ok, Value} -> cast_items(TargetType, H, [Value|Acc]);
    false       -> false
  end.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

cast_test() ->
  ArrayType = type(avro_primitive:string_type()),
  {ok, Array} = cast(ArrayType, ["a", "b"]),
  ?assertEqual(ArrayType, ?AVRO_VALUE_TYPE(Array)),
  ?assertEqual([avro_primitive:string("a"), avro_primitive:string("b")],
              ?AVRO_VALUE_DATA(Array)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
