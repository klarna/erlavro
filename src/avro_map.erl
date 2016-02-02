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
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc Handling of Avro maps.
%%% Data are kept internally as a dict :: string() -> avro_value()
%%% @end
%%%-------------------------------------------------------------------
-module(avro_map).

%% API
-export([type/1]).
-export([get_items_type/1]).
-export([new/2]).
-export([to_dict/1]).
-export([cast/2]).
-export([to_term/1]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type(ItemsType) ->
  #avro_map_type{ type = ItemsType }.

get_items_type(#avro_map_type{ type = SubType }) ->
  SubType.

new(Type, Data) when ?AVRO_IS_MAP_TYPE(Type) ->
  case cast(Type, Data) of
    {ok, Value}  -> Value;
    {error, Err} -> erlang:error(Err)
  end.

to_dict(Value) when ?AVRO_IS_MAP_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

%% Value is other Avro map value or a proplist with string keys.
-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.

cast(Type, Value) when ?AVRO_IS_MAP_TYPE(Type) ->
  do_cast(Type, Value).

-spec to_term(avro_value()) -> list().
to_term(Map) ->
  [{K, avro:to_term(V)} || {K, V} <- dict:to_list(to_dict(Map))].

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_cast(Type, Value) when ?AVRO_IS_MAP_VALUE(Value) ->
  %% Just cast data of the source map
  do_cast(Type, ?AVRO_VALUE_DATA(Value));
do_cast(Type, List) when is_list(List) ->
  %% Cast from a proplist :: [{string(), term()}]
  do_cast(Type, dict:from_list(List));
do_cast(Type, Dict) ->
  #avro_map_type{ type = ItemsType } = Type,
  case cast_from_dict(ItemsType, Dict) of
    {error, _} = Err -> Err;
    NewDict          -> {ok, ?AVRO_VALUE(Type, NewDict)}
  end.

cast_from_dict(ItemsType, Dict) ->
  dict:fold(
    fun(_Key, _Value, {error, _} = Acc) ->
        %% Ignore the rest after the first error
        Acc;
      (Key, Value, Acc) when is_list(Key) ->
        case avro:cast(ItemsType, Value) of
          {ok, CV} -> dict:store(Key, CV, Acc);
          Err      -> Err
        end;
      (Key, _Value, _Acc) ->
        %% If Key is not a string
        {error, {wrong_key_value, Key}}
    end,
    dict:new(),
    Dict).

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

cast_test() ->
  Type = type(avro_primitive:int_type()),
  Value = cast(Type, [{"v1", 1}, {"v2", 2}, {"v3", 3}]),
  Expected = ?AVRO_VALUE(Type, dict:from_list(
                                 [{"v1", avro_primitive:int(1)}
                                 ,{"v2", avro_primitive:int(2)}
                                 ,{"v3", avro_primitive:int(3)}])),
  ?assertEqual({ok, Expected}, Value).

to_dict_test() ->
  Type = type(avro_primitive:int_type()),
  Value = new(Type, [{"v1", 1}, {"v2", 2}, {"v3", 3}]),
  Expected = dict:from_list(
               [{"v1", avro_primitive:int(1)}
               ,{"v2", avro_primitive:int(2)}
               ,{"v3", avro_primitive:int(3)}]),
  ?assertEqual(Expected,
               to_dict(Value)).

to_term_test() ->
  Type = type(avro_primitive:int_type()),
  ExpectedMappings = [{"v1", 1}, {"v2", 2}, {"v3", 3}],
  Value = new(Type, ExpectedMappings),
  Mappings = avro:to_term(Value),
  ?assertEqual(ExpectedMappings, lists:keysort(1, Mappings)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
