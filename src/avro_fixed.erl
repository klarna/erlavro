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
%%% @doc Avro fixed type implementation.
%%% Internal data for fixed values is a binary value.
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro_fixed).

%% API
-export([ cast/2
        , get_size/1
        , get_value/1
        , new/2
        , resolve_fullname/2
        , type/2
        , type/3
        ]).

-include("avro_internal.hrl").

%%%_* APIs =====================================================================

%% @doc Declare a fixed type with default properties.
-spec type(name_raw(), pos_integer()) -> fixed_type().
type(Name, Size) ->
  type(Name, Size, []).

%% @doc Declare a fixed type.
-spec type(name_raw(), pos_integer(), type_props()) -> fixed_type().
type(Name0, Size, Opts) ->
  {Name, Ns0} = avro:split_type_name(Name0, ?NS_GLOBAL),
  Ns          = ?NAME(avro_util:get_opt(namespace, Opts, Ns0)),
  true        = (Ns0 =:= ?NS_GLOBAL orelse Ns0 =:= Ns), %% assert
  Aliases     = avro_util:get_opt(aliases, Opts, []),
  ok          = avro_util:verify_aliases(Aliases),
  ?ERROR_IF(not is_integer(Size) orelse Size < 1, {invalid_size, Size}),
  Type = #avro_fixed_type
         { name      = Name
         , namespace = Ns
         , aliases   = avro_util:canonicalize_aliases(Aliases, Ns)
         , size      = Size
         , fullname  = avro:build_type_fullname(Name, Ns)
         , custom    = avro_util:canonicalize_custom_props(Opts)
         },
  ok = avro_util:verify_type(Type),
  Type.

%% @doc Resolve fullname by newly discovered enclosing namespace.
-spec resolve_fullname(fixed_type(), namespace()) -> fixed_type().
resolve_fullname(#avro_fixed_type{ fullname  = FullName
                                 , aliases   = Aliases
                                 } = T, Ns) ->
  NewFullname = avro:build_type_fullname(FullName, Ns),
  NewAliases = avro_util:canonicalize_aliases(Aliases, Ns),
  T#avro_fixed_type{ fullname = NewFullname
                   , aliases  = NewAliases
                   }.

%% @doc Get size of the declared type.
-spec get_size(fixed_type()) -> pos_integer().
get_size(#avro_fixed_type{ size = Size }) -> Size.

%% @doc Create a wrapped (boxed) value.
-spec new(fixed_type(), avro:in()) -> avro_value() | no_return().
new(Type, Value) when ?IS_FIXED_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Rec}    -> Rec;
    {error, Err} -> erlang:error(Err)
  end.

%% @doc Return (non-recursive) data in the wrapped (boxed) value.
-spec get_value(avro_value()) -> binary().
get_value(Value) when ?IS_FIXED_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value).

%% @doc Fixed values can be casted from other fixed values, from integers
%% or from binaries.
%% @end
-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.
cast(Type, Value) when ?IS_FIXED_TYPE(Type) ->
  do_cast(Type, Value).

%%%_* Internal functions =======================================================

%% @private
-spec integer_to_fixed(pos_integer(), non_neg_integer()) -> binary().
integer_to_fixed(Size, Integer) ->
  Bin = binary:encode_unsigned(Integer),
  true = (Size >= size(Bin)),
  PadSize = (Size - size(Bin)) * 8,
  <<0:PadSize, Bin/binary>>.

%% @private
do_cast(Type, Value) when is_integer(Value) ->
  #avro_fixed_type{ size = Size } = Type,
  case Value >= 0 andalso Value < (1 bsl (8 * Size)) of
    true  ->
      do_cast(Type, integer_to_fixed(Size, Value));
    false ->
      {error, integer_out_of_range}
  end;
do_cast(Type, Value) when is_binary(Value) ->
  #avro_fixed_type{ size = Size } = Type,
  case size(Value) =:= Size of
    true  -> {ok, ?AVRO_VALUE(Type, Value)};
    false -> {error, bad_size}
  end;
do_cast(Type, Value) ->
  {error, {cast_error, Type, Value}}.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
