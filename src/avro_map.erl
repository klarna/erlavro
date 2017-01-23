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
%%% Data are kept internally as a gb_trees:: binary() -> avro_value()
%%% @end
%%%-------------------------------------------------------------------
-module(avro_map).

%% API
-export([type/1]).
-export([get_items_type/1]).
-export([new/2]).
-export([to_list/1]).
-export([cast/2]).
-export([to_term/1]).
-export([encode/3]).

-include("erlavro.hrl").

-define(IS_IOLIST(S), (is_list(S) orelse is_binary(S))).

-type key() :: binary().
-type value() :: avro_value().
-type data() :: gb_trees:tree(key(), value()).

-type input_key() :: atom() | iolist().
-type input_value() :: term().
-type input_data() :: [{input_key(), input_value()}].

%%%=============================================================================
%%% API
%%%=============================================================================

%% @doc Define a map type.
-spec type(avro_type()) -> #avro_map_type{}.
type(ItemsType) ->
  #avro_map_type{ type = ItemsType }.

%% @doc Return the map-value's type definition.
-spec get_items_type(#avro_map_type{}) -> avro_type().
get_items_type(#avro_map_type{ type = SubType }) ->
  SubType.

%% @doc Create a new typed (boxed) value.
%% Raise an 'error' in case of failure.
%% @end
-spec new(#avro_map_type{}, input_data()) -> avro_value() | no_return().
new(Type, Data) when ?AVRO_IS_MAP_TYPE(Type) ->
  case cast(Type, Data) of
    {ok, Value}  -> Value;
    {error, Err} -> erlang:error(Err)
  end.

%% @doc Unboxing the wrapped avro_value().
-spec to_term(avro_value()) -> [{key(), value()}].
to_term(Map) ->
  [{K, avro:to_term(V)} || {K, V} <- to_list(Map)].

%% @hidden Return the typed value as a kv-list.
%% NOTE: The value is not recursively unboxed as what to_term/1 does.
%% @end
-spec to_list(avro_value()) -> [{key(), avro_value()}].
to_list(Value) when ?AVRO_IS_MAP_VALUE(Value) ->
  lists:keysort(1, gb_trees:to_list(?AVRO_VALUE_DATA(Value))).

%% @hidden Value is other Avro map value or a kv-list with iolist keys.
-spec cast(avro_type(), input_data()) -> {ok, avro_value()} | {error, term()}.
cast(Type, Value) when ?AVRO_IS_MAP_TYPE(Type) ->
  do_cast(Type, Value).

%% @hidden
-spec encode(avro_type_or_name(), input_data(), fun()) -> iolist().
encode(Type, Value, EncodeFun) ->
  ItemsType = avro_map:get_items_type(Type),
  [EncodeFun(ItemsType, K, V) || {K, V} <- Value].

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%% @private
-spec do_cast(#avro_map_type{}, input_data()) ->
        {ok, avro_value()} | {error, any()}.
do_cast(Type, Value) when ?AVRO_IS_MAP_VALUE(Value) ->
  %% Just cast data of the source map
  do_cast(Type, gb_trees:to_list(?AVRO_VALUE_DATA(Value)));
do_cast(Type, KvList0) when is_list(KvList0) ->
  #avro_map_type{type = ItemsType} = Type,
  MapFun =
    fun({K, V}) ->
      Key = bin(K),
      Value = case avro:cast(ItemsType, V) of
                {ok, CV}        -> CV;
                {error, Reason} -> throw({?MODULE, Reason})
              end,
      {Key, Value}
    end,
  try
    KvList = lists:map(MapFun, KvList0),
    {ok, ?AVRO_VALUE(Type, from_list(KvList))}
  catch
    throw : {?MODULE, Reason} ->
      {error, Reason}
  end.

bin(A) when is_atom(A)   -> bin(atom_to_list(A));
bin(S) when is_list(S)   -> iolist_to_binary(S);
bin(B) when is_binary(B) -> B.

-spec from_list([{key(), value()}]) -> data().
from_list(KvL) -> from_list(gb_trees:empty(), KvL).

-spec from_list([{key(), value()}], data()) -> data().
from_list(Tree, []) -> Tree;
from_list(Tree, [{K, V} | Rest]) ->
  from_list(gb_trees:enter(K, V, Tree), Rest).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
