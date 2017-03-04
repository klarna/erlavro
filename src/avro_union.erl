%%%-----------------------------------------------------------------------------
%%%
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
%%%-----------------------------------------------------------------------------

-module(avro_union).

%% API
-export([ cast/2
        , encode/3
        , get_child_type_index/1
        , get_types/1
        , get_value/1
        , lookup_type/2
        , new/2
        , resolve_fullname/2
        , to_term/1
        , type/1
        ]).

%% API functions which should be used only inside erlavro
-export([new_direct/2]).

-export_type([ id2type/0
             , name2id/0
             ]).

-include("avro_internal.hrl").

-type id2type() :: gb_trees:tree(union_index(), avro_type_or_name()).
-type name2id() :: gb_trees:tree(name(), {union_index(), boolean()}).
-type encode_result() :: avro_binary() | avro_json().
-type encode_fun() :: fun((avro_type(), avro:in(),
                           union_index()) -> encode_result()).

%%%_* APIs =====================================================================

%% @doc Define a union type.
%% A union should have at least one member.
%% @end
-spec type([avro_type_or_name()]) -> union_type() | no_return().
type([]) ->
  erlang:error(<<"union should have at least one member type">>);
type([_ | _ ] = Types0) ->
  IsUnion = fun(T) -> ?AVRO_IS_UNION_TYPE(T) end,
  lists:any(IsUnion, Types0) andalso
    erlang:error(<<"union should not have union as member">>),
  Types = lists:map(fun avro_util:canonicalize_type_or_name/1, Types0),
  Count = length(Types),
  IndexedTypes = lists:zip(lists:seq(0, Count - 1), Types),
  Name2Id = build_name_to_id(IndexedTypes),
  ok = assert_no_duplicated_names(Name2Id, []),
  #avro_union_type
  { id2type = gb_trees:from_orddict(IndexedTypes)
  , name2id = gb_trees:from_orddict(orddict:from_list(Name2Id))
  }.

%% @doc Resolve fullname by newly discovered enclosing namespace.
-spec resolve_fullname(union_type(), namespace()) -> union_type().
resolve_fullname(T0, Ns) ->
  Types = get_types(T0),
  ResolvedTypes = lists:map(fun(T) -> avro:resolve_fullname(T, Ns) end, Types),
  type(ResolvedTypes).

%% @doc Get the union member types in a list.
-spec get_types(union_type()) -> [avro_type()].
get_types(#avro_union_type{id2type = IndexedTypes}) ->
  {_Ids, Types} = lists:unzip(gb_trees:to_list(IndexedTypes)),
  Types.

%% @doc Search for a union member by its index or full name.
-spec lookup_type(name_raw() | union_index(), union_type()) ->
        {ok, avro_type()} | false.
lookup_type(TypeId, #avro_union_type{id2type = Types}) when is_integer(TypeId) ->
  case gb_trees:lookup(TypeId, Types) of
    {value, Type} -> {ok, Type};
    none          -> false
  end;
lookup_type(Name, Union) when ?IS_NAME(Name) ->
  case lookup_index(Name, Union) of
    {ok, {_Id, true}} -> {ok, avro:name2type(Name)};
    {ok, {Id, false}} -> {ok, _} = lookup_type(Id, Union);
    false             -> false
  end;
lookup_type(Name, Union) when ?IS_NAME_RAW(Name) ->
  lookup_type(?NAME(Name), Union).

%% @doc Get member type index.
-spec get_child_type_index(avro_value()) -> union_index().
get_child_type_index(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  UnionType = ?AVRO_VALUE_TYPE(Union),
  TypeName = get_child_type_name(Union),
  {ok, {TypeId, _IsSelfRef}} = lookup_index(TypeName, UnionType),
  TypeId.

%% @doc Get typeed member's full type name.
%% This is used to encode wrapped (boxed) value to JSON format,
%% where member type full name instead of member index is used.
%% @end
-spec get_child_type_name(avro_value()) -> fullname().
get_child_type_name(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  TypedData = ?AVRO_VALUE_DATA(Union),
  avro:get_type_fullname(?AVRO_VALUE_TYPE(TypedData)).

%% @doc Create a wrapped (boxed) value.
-spec new(union_type(), avro:in()) -> avro_value() | no_return().
new(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Union}  -> Union;
    {error, Err} -> erlang:error(Err)
  end.

%% @hidden Special optimized version of new which assumes that Value
%% is already casted to one of the union types. Should only
%% be used inside erlavro.
%% @end
new_direct(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  ?AVRO_VALUE(Type, Value).

%% @doc Get current value of a union type variable
get_value(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  ?AVRO_VALUE_DATA(Union).

%% @doc Encode shared logic for JSON and binary encoder.
%% Encoding logic is implemented in EncodeFun.
%% @end
-spec encode(avro_type_or_name(), avro:in(), encode_fun()) ->
        encode_result() | no_return().
encode(Type, {MemberId, Value}, EncodeFun) when is_integer(MemberId) ->
  case lookup_type(MemberId, Type) of
    {ok, MemberType} ->
      EncodeFun(MemberType, Value, MemberId);
    false ->
      erlang:error({unknown_tag, Type, MemberId})
  end;
encode(Type, {MemberName, Value}, EncodeFun) when ?IS_NAME_RAW(MemberName) ->
  case lookup_index(MemberName, Type) of
    {ok, {MemberId, true}} ->
      EncodeFun(avro:name2type(MemberName), Value, MemberId);
    {ok, {MemberId, false}} ->
      {ok, MemberType} = lookup_type(MemberId, Type),
      %% the union input value is tagged with a union member name or id
      EncodeFun(MemberType, Value, MemberId);
    false ->
      erlang:error({unknown_tag, Type, MemberName})
  end;
encode(Type, Value, EncodeFun) ->
  MemberTypes = avro_union:get_types(Type),
  try_encode_union_loop(Type, MemberTypes, Value, 0, EncodeFun).

%% @hidden Note: in some cases casting to an union type can be ambiguous, for
%% example when it contains both string and enum types. In such cases
%% it is recommended to explicitly specify types for values, or not
%% use such combinations of types at all.
%% @end
-spec cast(union_type(), avro:in()) -> {ok, avro_value()} | {error, any()}.
cast(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  do_cast(Type, Value).

%% @doc Recursively unbox typed value.
-spec to_term(avro_value()) -> term().
to_term(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  avro:to_term(?AVRO_VALUE_DATA(Union)).

%%%_* Internal functions =======================================================

%% @private Build the member type name to member index mapping.
%% The map result is the id and a 'IsSefRef' boolean tag.
%% When the tag is set to true, there is no need to lookup the id2type
%% mapping for type because:
%% 1. when it's primitive type, simply call avro:name2type would be
%%    faster than another lookup
%% 2. when it's a remote reference to the named member type, the lookup
%%    result would be the name itsef
%% @end
-spec build_name_to_id([{union_index(), avro_type_or_name()}]) ->
        [{name(), {union_index(), IsSelfRef :: boolean()}}].
build_name_to_id(IndexedTypes) ->
  lists:map(
    fun({Id, FullName}) when ?IS_NAME(FullName) ->
        %% This is full name ref to a member type
        {FullName, {Id, _IsSelfRef = true}};
       ({Id, Type}) ->
        FullName = avro:get_type_fullname(Type),
        {FullName, {Id, _IsSelfRef = ?AVRO_IS_PRIMITIVE_TYPE(Type)}}
    end, IndexedTypes).

%% @private
-spec try_encode_union_loop(union_type(), [avro_type()], avro:in(),
                            union_index(), encode_fun()) ->
        encode_result() | no_return().
try_encode_union_loop(UnionType, [], Value, _Index, _EncodeFun) ->
  erlang:error({failed_to_encode_union, UnionType, Value});
try_encode_union_loop(UnionType, [MemberT | Rest], Value, Index, EncodeFun) ->
  try
    EncodeFun(MemberT, Value, Index)
  catch _ : _ ->
    try_encode_union_loop(UnionType, Rest, Value, Index + 1, EncodeFun)
  end.

%% @private Lookup union member index by union member name.
-spec lookup_index(name(), union_type()) ->
        {ok, {union_index(), boolean()}} | false.
lookup_index(Name, #avro_union_type{name2id = Ids}) when ?IS_NAME_RAW(Name) ->
  case gb_trees:lookup(?NAME(Name), Ids) of
    {value, Id} -> {ok, Id};
    none        -> false
  end.

%% @private
-spec do_cast(union_type(), avro:in()) -> avro_value().
do_cast(Type, {MemberId, Value}) ->
  case lookup_type(MemberId, Type) of
    {ok, MemberType} ->
      %% the union input value is tagged with a union member name or id
      avro:cast(MemberType, Value);
    false ->
      erlang:error({unknown_tag, Type, MemberId})
  end;
do_cast(Type, Value) ->
  case cast_over_types(get_types(Type), Value) of
    {ok, V} -> {ok, ?AVRO_VALUE(Type, V)};
    Err     -> Err
  end.

%% @private
-spec cast_over_types([avro_type()], avro:in()) ->
        {ok, avro_value()} | {error, term()}.
cast_over_types([], _Value) ->
  {error, type_mismatch};
cast_over_types([Type | Rest], Value) ->
  case avro:cast(Type, Value) of
    {error, _} -> cast_over_types(Rest, Value);
    R          -> R %% appropriate type found
  end.

-spec assert_no_duplicated_names([{name(), union_index()}], [name()]) ->
        ok | no_return().
assert_no_duplicated_names([], _UniqueNames) -> ok;
assert_no_duplicated_names([{Name, _Index} | Rest], UniqueNames) ->
  case lists:member(Name, UniqueNames) of
    true  -> erlang:error({<<"duplicated union member">>, Name});
    false -> assert_no_duplicated_names(Rest, [Name | UniqueNames])
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
