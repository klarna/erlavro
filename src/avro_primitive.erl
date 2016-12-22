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
-module(avro_primitive).

%% API
-export([null_type/0]).
-export([boolean_type/0]).
-export([int_type/0]).
-export([long_type/0]).
-export([float_type/0]).
-export([double_type/0]).
-export([bytes_type/0]).
-export([string_type/0]).

-export([cast/2]).

-export([null/0]).
-export([boolean/1]).
-export([int/1]).
-export([long/1]).
-export([float/1]).
-export([double/1]).
-export([bytes/1]).
-export([string/1]).

-export([get_value/1]).

-include("erlavro.hrl").


%%%===================================================================
%%% API: Types
%%%===================================================================

null_type()    -> #avro_primitive_type{name = ?AVRO_NULL}.

boolean_type() -> #avro_primitive_type{name = ?AVRO_BOOLEAN}.

int_type()     -> #avro_primitive_type{name = ?AVRO_INT}.

long_type()    -> #avro_primitive_type{name = ?AVRO_LONG}.

float_type()   -> #avro_primitive_type{name = ?AVRO_FLOAT}.

double_type()  -> #avro_primitive_type{name = ?AVRO_DOUBLE}.

bytes_type()   -> #avro_primitive_type{name = ?AVRO_BYTES}.

string_type()  -> #avro_primitive_type{name = ?AVRO_STRING}.

%%%===================================================================
%%% API: Casting
%%%===================================================================

-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.

cast(Type, Value)
  when ?AVRO_IS_PRIMITIVE_TYPE(Type) andalso
       ?IS_AVRO_VALUE(Value) andalso
       ?AVRO_VALUE_TYPE(Value) =:= Type ->
  %% Any primitive Avro value can be casted to same type
  {ok, Value};

%% Casts from erlang values

cast(Type, null) when ?AVRO_IS_NULL_TYPE(Type) ->
  {ok, ?AVRO_VALUE(Type, null)};

cast(Type, Value) when ?AVRO_IS_BOOLEAN_TYPE(Type) andalso
                       is_boolean(Value) ->
  {ok, ?AVRO_VALUE(Type, Value)};

cast(Type, 0) when ?AVRO_IS_BOOLEAN_TYPE(Type) ->
  {ok, ?AVRO_VALUE(Type, false)};

cast(Type, 1) when ?AVRO_IS_BOOLEAN_TYPE(Type) ->
  {ok, ?AVRO_VALUE(Type, true)};

cast(Type, Value) when ?AVRO_IS_INT_TYPE(Type) andalso
                       Value >= ?INT4_MIN andalso
                       Value =< ?INT4_MAX ->
  {ok, ?AVRO_VALUE(Type, Value)};

cast(Type, Value) when ?AVRO_IS_LONG_TYPE(Type) andalso
                       Value >= ?INT8_MIN andalso
                       Value =< ?INT8_MAX ->
  {ok, ?AVRO_VALUE(Type, Value)};

cast(Type, Value) when ?AVRO_IS_FLOAT_TYPE(Type) andalso
                       is_integer(Value) ->
  {ok, ?AVRO_VALUE(Type, erlang:float(Value))};

cast(Type, Value) when ?AVRO_IS_FLOAT_TYPE(Type) andalso
                       is_float(Value) ->
  {ok, ?AVRO_VALUE(Type, Value)};

cast(Type, Value) when ?AVRO_IS_DOUBLE_TYPE(Type) andalso
                       is_integer(Value) ->
  {ok, ?AVRO_VALUE(Type, erlang:float(Value))};

cast(Type, Value) when ?AVRO_IS_DOUBLE_TYPE(Type) andalso
                       is_float(Value) ->
  {ok, ?AVRO_VALUE(Type, Value)};

cast(Type, Value) when ?AVRO_IS_BYTES_TYPE(Type) andalso
                       is_binary(Value) ->
  {ok, ?AVRO_VALUE(Type, Value)};

cast(Type, Value) when ?AVRO_IS_STRING_TYPE(Type) andalso
                       (is_list(Value) orelse is_binary(Value)) ->
  {ok, ?AVRO_VALUE(Type, Value)};

%% Casts from other primitive Avro types so that we don't lose data

%% int -> long
cast(Type, Value) when ?AVRO_IS_LONG_TYPE(Type) andalso
                       ?AVRO_IS_INT_VALUE(Value) ->
  {ok, ?AVRO_VALUE(Type, ?AVRO_VALUE_DATA(Value))};

%% float -> double
cast(Type, Value) when ?AVRO_IS_DOUBLE_TYPE(Type) andalso
                       ?AVRO_IS_FLOAT_VALUE(Value) ->
  {ok, ?AVRO_VALUE(Type, ?AVRO_VALUE_DATA(Value))};

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
get_value(Value) when ?AVRO_IS_PRIMITIVE_TYPE(?AVRO_VALUE_TYPE(Value)) ->
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
