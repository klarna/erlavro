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
%%% @doc Implements unions support for Avro.
%%%
%%% Unions may not contain more than one schema with the same type, except
%%% for the named types record, fixed and enum. For example, unions containing
%%% two array types or two map types are not permitted, but two types with
%%% different names are permitted. (Names permit efficient resolution when
%%% reading and writing unions.)
%%%
%%% Unions may not immediately contain other unions.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_union).

%% API
-export([type/1]).
-export([get_types/1]).
-export([lookup_child_type/2]).
-export([get_child_type_name/1]).
-export([get_child_type_index/1]).
-export([get_child_type_index/2]).

-export([cast/2]).
-export([to_term/1]).

-export([new/2]).
-export([get_value/1]).
-export([set_value/2]).
-export([encode/3]).

%% API functions which should be used only inside erlavro
-export([new_direct/2]).

-include("avro_internal.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type([]) ->
  erlang:error(avro_union_should_have_at_least_one_type);
type(Types) when ?IS_NAME(Types) ->
  Count = length(Types),
  IndexedTypes = lists:zip(lists:seq(0, Count - 1), Types),
  TypesDict =
    case Count > 10 of
      true  -> build_types_dict(IndexedTypes);
      false -> undefined
    end,
  #avro_union_type
  { types      = IndexedTypes
  , types_dict = TypesDict
  }.

get_types(#avro_union_type{types = IndexedTypes}) ->
  {_Ids, Types} = lists:unzip(IndexedTypes),
  Types.

%% Search for a type by its full name through the children types
%% of the union
-spec lookup_child_type(#avro_union_type{}, string() | integer()) ->
        false | {ok, avro_type()}.
lookup_child_type(Union, TypeId) when is_integer(TypeId)  ->
  #avro_union_type{types = IndexedTypes} = Union,
  case lists:keyfind(TypeId, 1, IndexedTypes) of
    {TypeId, Type} -> {ok, Type};
    false          -> false
  end;
lookup_child_type(Union, TypeName) when ?IS_NAME(TypeName) ->
  case lookup(TypeName, Union) of
    {ok, {_TypeId, Type}} -> {ok, Type};
    false                 -> false
  end.

-spec get_child_type_index(#avro_value{}) -> non_neg_integer().
get_child_type_index(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  UnionType = ?AVRO_VALUE_TYPE(Union),
  TypeName = get_child_type_name(Union),
  {ok, Index} = get_child_type_index(UnionType, TypeName),
  Index.

-spec get_child_type_name(#avro_value{}) -> string().
get_child_type_name(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  TypedData = ?AVRO_VALUE_DATA(Union),
  get_type_fullname_ex(?AVRO_VALUE_TYPE(TypedData)).

-spec get_child_type_index(#avro_union_type{}, string()) ->
        false | {ok, non_neg_integer()}.
get_child_type_index(Union, TypeName) when ?AVRO_IS_UNION_TYPE(Union) ->
  case lookup(TypeName, Union) of
    {ok, {TypeId, _Type}} -> {ok, TypeId};
    false                 -> false
  end.

new(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Union}  -> Union;
    {error, Err} -> erlang:error(Err)
  end.

%% Special optimized version of new which assumes that Value
%% is already casted to one of the union types. Should only
%% be used inside erlavro.
new_direct(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  ?AVRO_VALUE(Type, Value).

%% Get current value of a union type variable
get_value(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  ?AVRO_VALUE_DATA(Union).

%% Sets new value to a union type variable
set_value(Union, Value) when ?AVRO_IS_UNION_VALUE(Union) ->
  ?AVRO_UPDATE_VALUE(Union, Value).

-spec encode(avro_type_or_name(), term(),fun()) -> term().
encode(Type, Union,EncodeFun) ->
  MemberTypes = avro_union:get_types(Type),
  try_encode_union_loop(Type, MemberTypes, Union, 0, EncodeFun).

%%%===================================================================
%%% API: casting
%%%===================================================================

%% Note: in some cases casting to an union type can be ambiguous, for
%% example when it contains both string and enum types. In such cases
%% it is recommended to explicitly specify types for values, or not
%% use such combinations of types at all.

-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.
cast(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  do_cast(Type, Value).

-spec to_term(avro_value()) -> term().
to_term(Union) -> avro:to_term(get_value(Union)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
try_encode_union_loop(UnionType, [], Value, _Index, _EncodeFun) ->
  erlang:error({failed_to_encode_union, UnionType, Value});
try_encode_union_loop(UnionType, [MemberT | Rest], Value, Index, EncodeFun) ->
  try
    EncodeFun(MemberT, Value, Index)
  catch _ : _ ->
    try_encode_union_loop(UnionType, Rest, Value, Index + 1, EncodeFun)
  end.

%% @private
build_types_dict(IndexedTypes) ->
  lists:foldl(
    fun({Index, Type}, D) ->
        dict:store(get_type_fullname_ex(Type), {Index, Type}, D)
    end,
    dict:new(),
    IndexedTypes).

%% @private
%% If type is specified by its name then return this name,
%% otherwise return type's full name.
get_type_fullname_ex(TypeName) when ?IS_NAME(TypeName) ->
  TypeName;
get_type_fullname_ex(Type) ->
  avro:get_type_fullname(Type).

%% @private
lookup(TypeName, #avro_union_type{types = Types, types_dict = undefined}) ->
  scan(TypeName, Types);
lookup(TypeName, #avro_union_type{types_dict = Dict}) ->
  case dict:find(TypeName, Dict) of
    {ok, _IndexedType} = Res -> Res;
    error                    -> false
  end.

%% @private
scan(_TypeName, []) -> false;
scan(TypeName, [{Id, Type} | Rest]) ->
  CandidateTypeName = get_type_fullname_ex(Type),
  case TypeName =:= CandidateTypeName of
    true  -> {ok, {Id, Type}};
    false -> scan(TypeName, Rest)
  end.

%% @private
do_cast(Type, Value) when ?AVRO_IS_UNION_VALUE(Value) ->
  %% Unions can't have other unions as their subtypes, so in this case
  %% we cast the value of the source union, not the union itself.
  do_cast(Type, ?AVRO_VALUE_DATA(Value));
do_cast(Type, Value) ->
  case cast_over_types(Type#avro_union_type.types, Value) of
    {ok, V} -> {ok, ?AVRO_VALUE(Type, V)};
    Err     -> Err
  end.

%% @private
-spec cast_over_types([], _Value) -> {ok, avro_value()} | {error, term()}.
cast_over_types([], _Value) ->
  {error, type_mismatch};
cast_over_types([{_Id,Type} | Rest], Value) ->
  case avro:cast(Type, Value) of
    {error, _} -> cast_over_types(Rest, Value);
    R          -> R %% appropriate type found
  end.


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
