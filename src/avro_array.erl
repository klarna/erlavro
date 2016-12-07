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
-export([get/1]).
-export([cast/2]).
-export([to_term/1]).

-export([prepend/2]).

%% API to be used only inside erlavro
-export([new_direct/2]).

-include("erlavro.hrl").

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
    {ok, Value}  -> Value;
    {error, Err} -> erlang:error(Err)
  end.

%% Special optimized version of new which assumes that all items in List have
%% been already casted to items type of the array, so we can skip checking
%% types one more time during casting. Should only be used inside erlavro.
new_direct(Type, List) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  ?AVRO_VALUE(Type, List).

%% Returns array contents as a list of Avro values
get(Value) when ?AVRO_IS_ARRAY_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

prepend(Items0, Value) when ?AVRO_IS_ARRAY_VALUE(Value) ->
  Type = ?AVRO_VALUE_TYPE(Value),
  Data = ?AVRO_VALUE_DATA(Value),
  #avro_array_type{type = ItemType} = Type,
  Items = cast_items(ItemType, Items0, []),
  new_direct(Type, Items ++ Data).

%% Only other Avro array type or erlang list can be casted to arrays
-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.

cast(Type, Value) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  do_cast(Type, Value).

-spec to_term(avro_value()) -> list().
to_term(Array) when ?AVRO_IS_ARRAY_VALUE(Array) ->
  [ avro:to_term(Item) || Item <- ?AVRO_VALUE_DATA(Array) ].

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec do_cast(#avro_array_type{}, avro_value() | [term()])
             -> {ok, avro_value()} | {error, term()}.

do_cast(Type, Array) when ?AVRO_IS_ARRAY_VALUE(Array) ->
  %% Since we can't compare array types we just cast all items one by one
  %% and see if this succeeds
  do_cast(Type, ?AVRO_VALUE_DATA(Array));
do_cast(Type, Items) when is_list(Items) ->
  #avro_array_type{type = ItemType} = Type,
  case cast_items(ItemType, Items, []) of
    ResArray when is_list(ResArray) -> {ok, ?AVRO_VALUE(Type, ResArray)};
    Err                             -> Err
  end.

cast_items(_TargetType, [], Acc) ->
  lists:reverse(Acc);
cast_items(TargetType, [Item|H], Acc) ->
  case avro:cast(TargetType, Item) of
    {ok, Value} -> cast_items(TargetType, H, [Value|Acc]);
    Err         -> Err
  end.


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
