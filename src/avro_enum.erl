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
%%% @doc Avro enum type implementation.
%%%
%%% All symbols in an enum must be unique; duplicates are prohibited.
%%%
%%% Internal data for an enum is the symbol string itself.
%%% @end
%%%-----------------------------------------------------------------------------
-module(avro_enum).

%% API
-export([ cast/2
        , get_index/1
        , get_index/2
        , get_symbol_from_index/2
        , get_value/1
        , new/2
        , resolve_fullname/2
        , type/2
        , type/3
        ]).

-include("avro_internal.hrl").

-type symbol() :: avro:enum_symbol().
-type symbol_raw() :: avro:enum_symbol_raw().
-type index() :: avro:enum_index().

-define(SYMBOL(X), avro_util:canonicalize_name(X)).
-define(IS_SYMBOL_RAW(T), (is_atom(T) orelse is_list(T) orelse is_binary(T))).

%%%=============================================================================
%%% API
%%%=============================================================================

%% @doc Declare a enum type with default properties.
-spec type(name_raw(), [symbol_raw()]) -> enum_type() | no_return().
type(Name, Symbols) ->
  type(Name, Symbols, []).

%% @doc Declare a enum type.
-spec type(name_raw(), [symbol_raw()], avro:type_props()) ->
        enum_type() | no_return().
type(Name0, Symbols0, Opts) ->
  {Name, Ns0} = avro:split_type_name(Name0, ?NS_GLOBAL),
  Ns          = ?NAME(avro_util:get_opt(namespace, Opts, Ns0)),
  true        = (Ns0 =:= ?NS_GLOBAL orelse Ns0 =:= Ns), %% assert
  Symbols     = lists:map(fun(S) -> ?SYMBOL(S) end, Symbols0),
  ok          = check_symbols(Symbols),
  Doc         = avro_util:get_opt(doc, Opts, ?NO_DOC),
  Aliases0    = avro_util:get_opt(aliases, Opts, []),
  ok          = avro_util:verify_aliases(Aliases0),
  Aliases     = avro_util:canonicalize_aliases(Aliases0, Ns),
  Type = #avro_enum_type
         { name      = Name
         , namespace = Ns
         , aliases   = Aliases
         , doc       = ?DOC(Doc)
         , symbols   = Symbols
         , fullname  = avro:build_type_fullname(Name, Ns)
         , custom    = avro_util:canonicalize_custom_props(Opts)
         },
  ok = avro_util:verify_type(Type),
  Type.

%% @doc Resolve fullname by newly discovered enclosing namespace.
-spec resolve_fullname(enum_type(), namespace()) -> enum_type().
resolve_fullname(#avro_enum_type{ fullname = Fullname
                                , aliases  = Aliases
                                } = T, Ns) ->
  NewFullname = avro:build_type_fullname(Fullname, Ns),
  NewAliases = avro_util:canonicalize_aliases(Aliases, Ns),
  T#avro_enum_type{ fullname = NewFullname
                  , aliases  = NewAliases
                  }.

%% @doc Create a enum wrapped (boxed) value.
-spec new(enum_type(), avro_value() | symbol_raw()) ->
        avro_value() | no_return().
new(Type, Value) when ?AVRO_IS_ENUM_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Rec}    -> Rec;
    {error, Err} -> erlang:error(Err)
  end.

%% @doc Get the enum symbol.
-spec get_value(avro_value()) -> symbol().
get_value(Value) when ?AVRO_IS_ENUM_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

%% @doc Get symbol index from boxed value.
-spec get_index(avro_value()) -> index().
get_index(Value) when ?AVRO_IS_ENUM_VALUE(Value) ->
  Type = ?AVRO_VALUE_TYPE(Value),
  Symbol = ?AVRO_VALUE_DATA(Value),
  get_index(Type, Symbol).

%% @doc Get symbol index from type definition.
-spec get_index(enum_type(), symbol_raw()) -> index().
get_index(Type, Symbol) ->
  get_index(?SYMBOL(Symbol), Type#avro_enum_type.symbols, 0).

%% @doc Find symbol from index.
-spec get_symbol_from_index(enum_type(), index()) -> symbol().
get_symbol_from_index(T, Index) when ?AVRO_IS_ENUM_TYPE(T) ->
  true = (Index < length(T#avro_enum_type.symbols)),
  lists:nth(Index + 1, T#avro_enum_type.symbols).

%% @doc Enums can be casted from other enums or strings.
-spec cast(avro_type(), symbol_raw()) -> {ok, avro_value()} | {error, term()}.
cast(Type, Value) when ?AVRO_IS_ENUM_TYPE(Type) ->
  do_cast(Type, Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec check_symbols([symbol()]) -> ok | no_return().
check_symbols(Symbols) ->
  SymLen = length(Symbols),
  ?ERROR_IF(SymLen =:= 0, empty_symbols),
  ?ERROR_IF(length(lists:usort(Symbols)) =/= SymLen, non_unique_symbols),
  avro_util:verify_names(Symbols).

%% @private
-spec do_cast(enum_type(), symbol_raw()) ->
        {ok, avro_value()} | {error, any()}.
do_cast(Type, Value0) when ?IS_SYMBOL_RAW(Value0) ->
  Value = ?SYMBOL(Value0),
  case is_valid_symbol(Type, Value) of
    true  -> {ok, ?AVRO_VALUE(Type, Value)};
    false -> {error, {cast_error, Type, Value0}}
  end.

%% @private
-spec is_valid_symbol(enum_type(), symbol()) -> boolean().
is_valid_symbol(Type, Symbol) ->
  lists:member(Symbol, Type#avro_enum_type.symbols).

%% @private
-spec get_index(symbol(), [symbol()], index()) -> index().
get_index(Symbol, [Symbol | _Symbols], Index) ->
  Index;
get_index(Symbol, [_ | Symbols], Index) ->
  get_index(Symbol, Symbols, Index + 1).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
