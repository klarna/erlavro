%%%-----------------------------------------------------------------------------
%%%
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
%%%-----------------------------------------------------------------------------

-module(avro_union).

%% API
-export([ cast/2
        , encode/3
        , get_child_type_name/1
        , get_child_type_index/1
        , get_types/1
        , get_value/1
        , lookup_child_type/2
        , new/2
        , resolve_fullname/2
        , to_term/1
        , type/1
        ]).

%% API functions which should be used only inside erlavro
-export([new_direct/2]).

-export_type([ types_dict/0
             ]).

-include("avro_internal.hrl").

-type types_dict() :: dict:dict(fullname(), {union_index(), avro_type()}).
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
  Types = lists:map(fun avro_util:canonicalize_type_or_name/1, Types0),
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

%% @doc Resolve fullname by newly discovered enclosing namespace.
-spec resolve_fullname(union_type(), namespace()) -> union_type().
resolve_fullname(T0, Ns) ->
  Types = get_types(T0),
  ResolvedTypes = lists:map(fun(T) -> avro:resolve_fullname(T, Ns) end, Types),
  type(ResolvedTypes).

%% @doc Get the union member types in a list.
-spec get_types(union_type()) -> [avro_type()].
get_types(#avro_union_type{types = IndexedTypes}) ->
  {_Ids, Types} = lists:unzip(IndexedTypes),
  Types.

%% @doc Search for a union member by its index or full name.
-spec lookup_child_type(union_type(), name_raw() | union_index()) ->
        false | {ok, avro_type()}.
lookup_child_type(Union, NameOrId) when ?IS_NAME_RAW(NameOrId) orelse
                                        is_integer(NameOrId) ->
  case lookup(NameOrId, Union) of
    {ok, {_TypeId, Type}} -> {ok, Type};
    false                 -> false
  end.

%% @doc Get member type index.
-spec get_child_type_index(avro_value()) -> union_index().
get_child_type_index(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  UnionType = ?AVRO_VALUE_TYPE(Union),
  TypeName = get_child_type_name(Union),
  {ok, {TypeId, _Type}} = lookup(TypeName, UnionType),
  TypeId.

%% @doc Get typeed member's full type name.
%% This is used to encode wrapped (boxed) value to JSON format,
%% where member type full name instead of member index is used.
%% @end
-spec get_child_type_name(avro_value()) -> fullname().
get_child_type_name(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  TypedData = ?AVRO_VALUE_DATA(Union),
  get_type_fullname_ex(?AVRO_VALUE_TYPE(TypedData)).


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
encode(Type, {MemberId, Value}, EncodeFun) ->
  case lookup(MemberId, Type) of
    {ok, {MemberIdInteger, MemberType}} ->
      %% the union input value is tagged with a union member name or id
      EncodeFun(MemberType, Value, MemberIdInteger);
    false ->
      erlang:error({unknown_tag, Type, MemberId})
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

%% @private
-spec build_types_dict([{union_index(), avro_type()}]) -> types_dict().
build_types_dict(IndexedTypes) ->
  lists:foldl(
    fun({Index, Type}, D) ->
        dict:store(get_type_fullname_ex(Type), {Index, Type}, D)
    end,
    dict:new(),
    IndexedTypes).

%% @private If type is specified by its name then return this name,
%% otherwise return type's full name.
%% @end
-spec get_type_fullname_ex(fullname() | avro_type()) -> fullname().
get_type_fullname_ex(TypeName) when ?IS_NAME(TypeName) ->
  TypeName;
get_type_fullname_ex(Type) ->
  avro:get_type_fullname(Type).

%% @private
-spec lookup(name_raw() | union_index(), union_type()) ->
        {ok, {union_index(), avro_type()}} | false.
lookup(TypeId, #avro_union_type{types = Types}) when is_integer(TypeId) ->
  case lists:keyfind(TypeId, 1, Types) of
    {TypeId, Type} -> {ok, {TypeId, Type}};
    false          -> false
  end;
lookup(TypeName, #avro_union_type{ types      = Types
                                 , types_dict = undefined
                                 }) when ?IS_NAME(TypeName) ->
  scan(TypeName, Types);
lookup(TypeName, #avro_union_type{types_dict = Dict}) when ?IS_NAME(TypeName) ->
  case dict:find(TypeName, Dict) of
    {ok, _IndexedType} = Res -> Res;
    error                    -> false
  end;
lookup(TypeName, Union) when ?IS_NAME_RAW(TypeName) ->
  lookup(?NAME(TypeName), Union).

%% @private
-spec scan(name(), [{union_index(), avro_type()}]) ->
        {ok, {union_index(), avro_type()}} | false.
scan(_TypeName, []) -> false;
scan(TypeName, [{Id, Type} | Rest]) ->
  CandidateTypeName = get_type_fullname_ex(Type),
  case TypeName =:= CandidateTypeName of
    true  -> {ok, {Id, Type}};
    false -> scan(TypeName, Rest)
  end.

%% @private
-spec do_cast(union_type(), avro:in()) -> avro_value().
do_cast(Type, {MemberId, Value}) ->
  case lookup(MemberId, Type) of
    {ok, {_MemberIdInteger, MemberType}} ->
      %% the union input value is tagged with a union member name or id
      avro:cast(MemberType, Value);
    false ->
      erlang:error({unknown_tag, Type, MemberId})
  end;
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


%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
