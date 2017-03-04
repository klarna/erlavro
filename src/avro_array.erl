%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2013-2017 Klarna AB
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
%%%-----------------------------------------------------------------------------
-module(avro_array).

%% API
-export([ cast/2
        , encode/3
        , get_items/1
        , get_items_type/1
        , new/1
        , new/2
        , prepend/2
        , resolve_fullname/2
        , to_term/1
        , type/1
        ]).

%% API to be used only inside erlavro
-export([new_direct/2]).

-include("avro_internal.hrl").

%%%_* APIs =====================================================================

%% @doc Define array type.
-spec type(avro_type_or_name()) -> array_type().
type(Type) ->
  #avro_array_type{type = avro_util:canonicalize_type_or_name(Type)}.

%% @doc Resolve children type's fullnames.
-spec resolve_fullname(array_type(), namespace()) -> array_type().
resolve_fullname(#avro_array_type{type = SubType} = T, Ns) ->
  T#avro_array_type{type = avro:resolve_fullname(SubType, Ns)}.

%% @doc Get array element type.
-spec get_items_type(array_type()) -> avro_type().
get_items_type(ArrayType) when ?AVRO_IS_ARRAY_TYPE(ArrayType) ->
  ArrayType#avro_array_type.type.

%% @doc Create a wrapped (boxed) empty array avro value.
-spec new(array_type()) -> avro_value().
new(Type) ->
  new(Type, []).

%% @doc Create a wrapped (boxed) avro value with given array data.
-spec new(array_type(), [term()]) -> avro_value() | no_return().
new(Type, List) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  case cast(Type, List) of
    {ok, Value}  -> Value;
    {error, Err} -> erlang:error(Err)
  end.

%% @doc Special optimized version of new which assumes that all items in List
%% have been already casted to items type of the array, so we can skip checking
%% types one more time during casting. Should only be used inside erlavro.
%% @end
-spec new_direct(array_type(), [avro:in()]) -> avro_value().
new_direct(Type, List) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  ?AVRO_VALUE(Type, List).

%% @doc Returns array contents as a list of avro values.
-spec get_items(avro_value()) -> [avro_value()].
get_items(Value) when ?AVRO_IS_ARRAY_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

%% @doc Prepend elements to the array.
-spec prepend([term()], avro_value()) -> avro_value() | no_return().
prepend(Items0, Value) when ?AVRO_IS_ARRAY_VALUE(Value) ->
  Type = ?AVRO_VALUE_TYPE(Value),
  Data = ?AVRO_VALUE_DATA(Value),
  #avro_array_type{type = ItemType} = Type,
  {ok, Items} = cast_items(ItemType, Items0, []),
  new_direct(Type, Items ++ Data).

%% @hidden Only other Avro array type or erlang list can be casted to arrays.
-spec cast(array_type(), [avro:in()]) -> {ok, avro_value()} | {error, term()}.
cast(Type, Value) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  do_cast(Type, Value).

%% @hidden Recursively unbox typed value.
-spec to_term(avro_value()) -> list().
to_term(Array) when ?AVRO_IS_ARRAY_VALUE(Array) ->
  [ avro:to_term(Item) || Item <- ?AVRO_VALUE_DATA(Array) ].

%% @hidden Encoder help function. For internal use only.
-spec encode(avro_type_or_name(), list(), fun()) -> list().
encode(Type, Value, EncodeFun) ->
  ItemsType = avro_array:get_items_type(Type),
  lists:map(fun(Element) -> EncodeFun(ItemsType, Element) end, Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec do_cast(#avro_array_type{}, [avro:in()]) ->
        {ok, avro_value()} | {error, term()}.
do_cast(Type, Items) when is_list(Items) ->
  #avro_array_type{type = ItemType} = Type,
  case cast_items(ItemType, Items, []) of
    {ok, ResArray}  -> {ok, ?AVRO_VALUE(Type, ResArray)};
    {error, Reason} -> {error, Reason}
  end.

%% @private
-spec cast_items(avro_type(), [term()], [avro_value()]) ->
        {ok, [avro_value()]} | {error, any()}.
cast_items(_TargetType, [], Acc) ->
  {ok, lists:reverse(Acc)};
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
