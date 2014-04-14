%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc Handling of Avro maps.
%%% Data are kept internally as a proplist [{string(), avro_value()}]
%%% @end
%%%-------------------------------------------------------------------
-module(avro_map).

%% API
-export([type/1]).
-export([get_items_type/1]).
-export([new/2]).
-export([to_list/1]).
-export([keyfind/2]).
-export([cast/2]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type(ItemsType) ->
  #avro_map_type{ type = ItemsType }.

get_items_type(#avro_map_type{ type = SubType }) ->
  SubType.

new(Type, List) when ?AVRO_IS_MAP_TYPE(Type) ->
  case cast(Type, List) of
    {ok, Value}  -> Value;
    {error, Err} -> erlang:error(Err)
  end.

to_list(Value) when ?AVRO_IS_MAP_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

keyfind(Key, Value) when ?AVRO_IS_MAP_VALUE(Value) ->
  lists:keyfind(Key, 1, ?AVRO_VALUE_DATA(Value)).

%% Value is other Avro map value or a proplist with string keys.
-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.

cast(Type, Value) when ?AVRO_IS_MAP_TYPE(Type) ->
  do_cast(Type, Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_cast(Type, Value) when ?AVRO_IS_MAP_VALUE(Value) ->
  %% Just cast data of the source map
  do_cast(Type, ?AVRO_VALUE_DATA(Value));
do_cast(Type, List) ->
  #avro_map_type{ type = ItemsType } = Type,
  case cast_items(ItemsType, List, []) of
    Res when is_list(Res) -> {ok, ?AVRO_VALUE(Type, Res)};
    Err                   -> Err
  end.

cast_items(_ItemsType, [], Acc) ->
  lists:reverse(Acc);
cast_items(ItemsType, [{K,V}|Rest], Acc) ->
  case avro:cast(ItemsType, V) of
    {ok, CV} -> cast_items(ItemsType, Rest, [{K,CV}|Acc]);
    Err      -> Err
  end.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

cast_test() ->
  Type = type(avro_primitive:int_type()),
  Value = cast(Type, [{"v1", 1}, {"v2", 2}, {"v3", 3}]),
  Expected = ?AVRO_VALUE(Type, [{"v1", avro_primitive:int(1)}
                               ,{"v2", avro_primitive:int(2)}
                               ,{"v3", avro_primitive:int(3)}]),
  ?assertEqual({ok, Expected}, Value).

get_values_test() ->
  Type = type(avro_primitive:int_type()),
  Value = new(Type, [{"v1", 1}, {"v2", 2}, {"v3", 3}]),
  ?assertEqual([{"v1", avro_primitive:int(1)}
               ,{"v2", avro_primitive:int(2)}
               ,{"v3", avro_primitive:int(3)}],
               to_list(Value)).

keyfind_test() ->
  Type = type(avro_primitive:int_type()),
  Value = new(Type, [{"v1", 1}, {"v2", 2}, {"v3", 3}]),
  ?assertEqual({"v2", avro_primitive:int(2)},
               keyfind("v2", Value)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
