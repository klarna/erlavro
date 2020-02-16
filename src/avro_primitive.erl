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
%%% @doc
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro_primitive).

%% API
-export([ type/2
        , boolean_type/0
        , bytes_type/0
        , double_type/0
        , float_type/0
        , int_type/0
        , long_type/0
        , string_type/0
        , null_type/0
        ]).

-export([ boolean/1
        , bytes/1
        , cast/2
        , double/1
        , float/1
        , get_value/1
        , int/1
        , long/1
        , null/0
        , string/1
        ]).

-include("avro_internal.hrl").

%%%===================================================================
%%% API: Types
%%%===================================================================

-spec type(name_raw(), [custom_prop()]) -> primitive_type().
type(Name0, CustomProps) ->
  Name = ?NAME(Name0),
  case ?IS_AVRO_PRIMITIVE_NAME(Name) of
    true ->
      Custom = avro_util:canonicalize_custom_props(CustomProps),
      #avro_primitive_type{ name   = Name
                          , custom = Custom
                          };
    false ->
      erlang:error({unknown_name, Name})
  end.

null_type()    -> type(?AVRO_NULL, []).

boolean_type() -> type(?AVRO_BOOLEAN, []).

int_type()     -> type(?AVRO_INT, []).

long_type()    -> type(?AVRO_LONG, []).

float_type()   -> type(?AVRO_FLOAT, []).

double_type()  -> type(?AVRO_DOUBLE, []).

bytes_type()   -> type(?AVRO_BYTES, []).

string_type()  -> type(?AVRO_STRING, []).

%%%===================================================================
%%% API: Casting
%%%===================================================================

-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.
cast(Type, null) when ?IS_NULL_TYPE(Type) ->
  {ok, ?AVRO_VALUE(Type, null)};
% For Elixir compatibility
cast(Type, nil) when ?IS_NULL_TYPE(Type) ->
  {ok, ?AVRO_VALUE(Type, null)};
cast(Type, Value) when ?IS_BOOLEAN_TYPE(Type) andalso
                       is_boolean(Value) ->
  {ok, ?AVRO_VALUE(Type, Value)};
cast(Type, Value) when ?IS_INT_TYPE(Type) andalso
                       Value >= ?INT4_MIN andalso
                       Value =< ?INT4_MAX ->
  {ok, ?AVRO_VALUE(Type, Value)};
cast(Type, Value) when ?IS_LONG_TYPE(Type) andalso
                       Value >= ?INT8_MIN andalso
                       Value =< ?INT8_MAX ->
  {ok, ?AVRO_VALUE(Type, Value)};
cast(Type, Value) when ?IS_FLOAT_TYPE(Type) andalso
                       is_integer(Value) ->
  {ok, ?AVRO_VALUE(Type, erlang:float(Value))};
cast(Type, Value) when ?IS_FLOAT_TYPE(Type) andalso
                       is_float(Value) ->
  {ok, ?AVRO_VALUE(Type, Value)};
cast(Type, Value) when ?IS_DOUBLE_TYPE(Type) andalso
                       is_integer(Value) ->
  {ok, ?AVRO_VALUE(Type, erlang:float(Value))};
cast(Type, Value) when ?IS_DOUBLE_TYPE(Type) andalso
                       is_float(Value) ->
  {ok, ?AVRO_VALUE(Type, Value)};
cast(Type, Value) when ?IS_BYTES_TYPE(Type) andalso
                       is_binary(Value) ->
  {ok, ?AVRO_VALUE(Type, Value)};
cast(Type, Value) when ?IS_STRING_TYPE(Type) andalso
                       (is_list(Value) orelse is_binary(Value)) ->
  {ok, ?AVRO_VALUE(Type, erlang:iolist_to_binary(Value))};
cast(Type, Value) when ?IS_STRING_TYPE(Type) andalso
                       (is_atom(Value) andalso Value =/= null andalso Value =/= nil) ->
  {ok, ?AVRO_VALUE(Type, erlang:atom_to_binary(Value, utf8))};
cast(Type, Value) -> {error, {type_mismatch, Type, Value}}.

%%%===================================================================
%%% API: Helpers
%%%===================================================================

null() -> from_cast(cast(null_type(), null)).

boolean(Value) -> from_cast(cast(boolean_type(), Value)).

int(Value) -> from_cast(cast(int_type(), Value)).

long(Value) -> from_cast(cast(long_type(), Value)).

float(Value) -> from_cast(cast(float_type(), Value)).

double(Value) -> from_cast(cast(double_type(), Value)).

bytes(Value) -> from_cast(cast(bytes_type(), Value)).

string(Value) -> from_cast(cast(string_type(), Value)).

%% Get underlying erlang value from an Avro primitive value
-spec get_value(avro_value()) -> canonicalized_value().
get_value(Value) when ?IS_PRIMITIVE_TYPE(?AVRO_VALUE_TYPE(Value)) ->
  ?AVRO_VALUE_DATA(Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
from_cast({ok, Value})  -> Value;
from_cast({error, Err}) -> erlang:error(Err).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
