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
%%% @doc Avro fixed type implementation.
%%% Internal data for fixed values is a binary value.
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro_fixed).

%% API
-export([type/2]).
-export([type/3]).
-export([get_size/1]).
-export([new/2]).
-export([get_value/1]).
-export([cast/2]).

-include("avro_internal.hrl").

%%%_* APIs =====================================================================

%% @doc Declare a fixed type with default properties.
-spec type(name_raw(), pos_integer()) -> fixed_type().
type(Name, Size) ->
  type(Name, Size, []).

%% @doc Declare a fixed type.
-spec type(name_raw(), pos_integer(), type_props()) -> fixed_type().
type(Name, Size, Opts) ->
  ?ERROR_IF(not is_integer(Size) orelse Size < 1, {invalid_size, Size}),
  Ns          = avro_util:get_opt(namespace, Opts, ?NS_GLOBAL),
  Aliases     = avro_util:get_opt(aliases, Opts, []),
  EnclosingNs = avro_util:get_opt(enclosing_ns, Opts, ?NS_GLOBAL),
  ok = avro_util:verify_aliases(Aliases),
  Type = #avro_fixed_type
         { name      = ?NAME(Name)
         , namespace = ?NAME(Ns)
         , aliases   = avro_util:canonicalize_aliases(
                         Aliases, Name, Ns, EnclosingNs)
         , size      = Size
         , fullname  = avro:build_type_fullname(Name, Ns, EnclosingNs)
         },
  ok = avro_util:verify_type(Type),
  Type.

%% @doc Get size of the declared type.
-spec get_size(fixed_type()) -> pos_integer().
get_size(#avro_fixed_type{ size = Size }) -> Size.

%% @doc Create a wrapped (boxed) value.
-spec new(fixed_type(), term()) -> avro_value() | no_return().
new(Type, Value) when ?AVRO_IS_FIXED_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Rec}    -> Rec;
    {error, Err} -> erlang:error(Err)
  end.

%% @doc Return (non-recursive) data in the wrapped (boxed) value.
-spec get_value(avro_value()) -> binary().
get_value(Value) when ?AVRO_IS_FIXED_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

%% @doc Fixed values can be casted from other fixed values, from integers
%% or from binaries.
%% @end
-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.
cast(Type, Value) when ?AVRO_IS_FIXED_TYPE(Type) ->
  do_cast(Type, Value).

%%%_* Internal functions =======================================================

%% @private
-spec integer_to_fixed(pos_integer(), non_neg_integer()) -> binary().
integer_to_fixed(Size, Integer) ->
  Bin = binary:encode_unsigned(Integer),
  true = (Size >= size(Bin)),
  PadSize = (Size - size(Bin))*8,
  <<0:PadSize, Bin/binary>>.

%% @private
do_cast(Type, Value) when is_integer(Value) ->
  #avro_fixed_type{ size = Size } = Type,
  case Value >= 0 andalso Value < (1 bsl (8*Size)) of
    true  ->
      do_cast(Type, integer_to_fixed(Size, Value));
    false ->
      {error, integer_out_of_range}
  end;
do_cast(Type, Value) when is_binary(Value) ->
  #avro_fixed_type{ size = Size } = Type,
  case size(Value) =:= Size of
    true  -> {ok, ?AVRO_VALUE(Type, Value)};
    false -> {error, wrong_binary_size}
  end;
do_cast(Type, Value) ->
  {error, {cast_error, Type, Value}}.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
