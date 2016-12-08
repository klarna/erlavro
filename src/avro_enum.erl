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
%%% @doc Avro enum type implementation.
%%%
%%% All symbols in an enum must be unique; duplicates are prohibited.
%%%
%%% Internal data for an enum is the symbol string itself.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_enum).

%% API
-export([type/2]).
-export([type/3]).
-export([new/2]).
-export([get_value/1]).
-export([get_index/1]).
-export([get_index/2]).
-export([get_symbol_from_index/2]).
-export([cast/2]).
-export([uncast/2]).

-include("erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

-spec type(string(), [string()]) -> #avro_enum_type{} | no_return().
type(Name, Symbols) ->
  type(Name, Symbols, []).

%% Options:
%%   namespace    :: string()
%%   doc          :: string()
%%   aliases      :: [string()]
%%   enclosing_ns :: string()
-spec type(string(), [string()], [{atom(), term()}]) ->
        #avro_enum_type{} | no_return().
type(Name, Symbols, Opts) ->
  check_symbols(Symbols),
  Ns          = avro_util:get_opt(namespace, Opts, ""),
  Doc         = avro_util:get_opt(doc, Opts, ""),
  Aliases     = avro_util:get_opt(aliases, Opts, []),
  EnclosingNs = avro_util:get_opt(enclosing_ns, Opts, ""),
  avro_util:verify_aliases(Aliases),
  Type = #avro_enum_type
         { name      = Name
         , namespace = Ns
         , aliases   = avro_util:canonicalize_aliases(
                         Aliases, Name, Ns, EnclosingNs)
         , doc       = Doc
         , symbols   = Symbols
         , fullname  = avro:build_type_fullname(Name, Ns, EnclosingNs)
         },
  avro_util:verify_type(Type),
  Type.

-spec new(#avro_enum_type{}, term()) -> avro_value().

new(Type, Value) when ?AVRO_IS_ENUM_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Rec}    -> Rec;
    {error, Err} -> erlang:error(Err)
  end.

get_value(Value) when ?AVRO_IS_ENUM_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

get_index(Value) when ?AVRO_IS_ENUM_VALUE(Value) ->
  Type = ?AVRO_VALUE_TYPE(Value),
  Symbol = ?AVRO_VALUE_DATA(Value),
  get_index(Type, Symbol).

get_index(Type, Symbol) ->
  get_index(Symbol, Type#avro_enum_type.symbols, 0).

get_symbol_from_index(T, Index) when ?AVRO_IS_ENUM_TYPE(T) ->
  true = (Index < length(T#avro_enum_type.symbols)),
  lists:nth(Index + 1, T#avro_enum_type.symbols).

%% Enums can be casted from other enums or strings
-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.

cast(Type, Value) when ?AVRO_IS_ENUM_TYPE(Type) ->
  do_cast(Type, Value).

-spec uncast(avro_type(), avro_value()) -> {ok, term()} | {error, term()}.
uncast(_Type, Value) ->
  {ok, Value}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
check_symbols(Symbols) ->
  SymLen = length(Symbols),
  ?ERROR_IF(SymLen =:= 0,
            empty_symbols),
  ?ERROR_IF(length(lists:usort(Symbols)) =/= SymLen,
            non_unique_symbols),
  lists:foreach(fun(S) -> avro_util:verify_name(S) end, Symbols).

%% @private
do_cast(Type, Value) when ?AVRO_IS_ENUM_VALUE(Value) ->
  %% When casting from other enums only equality of type names is checked
  TargetTypeName = Type#avro_enum_type.fullname,
  SourceTypeName = (?AVRO_VALUE_TYPE(Value))#avro_enum_type.fullname,
  if TargetTypeName =:= SourceTypeName -> {ok, Value};
     true                              -> {error, type_name_mismatch}
  end;
do_cast(Type, Value) when is_list(Value) ->
  case is_valid_symbol(Type, Value) of
    true  -> {ok, ?AVRO_VALUE(Type, Value)};
    false -> {error, {cast_error, Type, Value}}
  end;
do_cast(Type, Value) ->
  {error, {cast_error, Type, Value}}.

%% @private
is_valid_symbol(Type, Symbol) ->
  lists:member(Symbol, Type#avro_enum_type.symbols).

%% @private
get_index(Symbol, [Symbol | _Symbols], Index) ->
  Index;
get_index(Symbol, [_ | Symbols], Index) ->
  get_index(Symbol, Symbols, Index + 1).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
