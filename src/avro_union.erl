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
-export([new/2]).
-export([get_value/1]).
-export([set_value/2]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type(SubTypes) ->
    #avro_union_type{types = SubTypes}.

new(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
    ?AVRO_VALUE(Type, Value).

%% Get current value of a union type variable
get_value(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
    ?AVRO_VALUE_DATA(Union).

%% Sets new value to a union type variable
set_value(Union, Value) when ?AVRO_IS_UNION_VALUE(Union) ->
    ?AVRO_UPDATE_VALUE(Union, Value).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).


-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
