%%%-------------------------------------------------------------------
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

-export([cast/2]).

-export([new/2]).
-export([get_value/1]).
-export([set_value/2]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type(Types) ->
    #avro_union_type{types = Types}.

get_types(#avro_union_type{types = Types}) -> Types.

new(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Union} -> Union;
    false       -> erlang:error({avro_error, wrong_cast})
  end.

%% Get current value of a union type variable
get_value(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
    ?AVRO_VALUE_DATA(Union).

%% Sets new value to a union type variable
set_value(Union, Value) when ?AVRO_IS_UNION_VALUE(Union) ->
    ?AVRO_UPDATE_VALUE(Union, Value).

%%%===================================================================
%%% API: casting
%%%===================================================================

%% Note: in some cases casting to an union type can be ambiguous, for
%% example when it contains both string and enum types. In such cases
%% it is recommended to explicitly specify types for values, or not
%% use such combinations of types at all.

-spec cast(avro_type(), term()) -> {ok, avro_value()} | false.

cast(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  do_cast(Type, Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_cast(Type, Value) when ?AVRO_IS_UNION_VALUE(Value) ->
  %% Unions can't have other unions as their subtypes, so in this case
  %% we cast the value of the source union, not the union itself.
  do_cast(Type, ?AVRO_VALUE_DATA(Value));
do_cast(Type, Value) ->
  case cast_over_types(Type#avro_union_type.types, Value) of
    {ok, V} -> {ok, ?AVRO_VALUE(Type, V)};
    false   -> false
  end.

cast_over_types([], _Value) ->
  false;
cast_over_types([T|H], Value) ->
  case avro:cast(T, Value) of
    false -> cast_over_types(H, Value);
    R     -> R %% appropriate type found
  end.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).


-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
