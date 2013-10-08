%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_union).

%% API
-export([type/1]).
-export([new/1]).
-export([is_compatible_with/2]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type(SubTypes) ->
    #avro_union_type{types = SubTypes}.

new(_Value) ->
    ok.

is_compatible_with(_Type, _Value) ->
    todo.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% From the specification:
%%
%% Unions may not contain more than one schema with the same type, except
%% for the named types record, fixed and enum. For example, unions containing
%% two array types or two map types are not permitted, but two types with
%% different names are permitted. (Names permit efficient resolution when
%% reading and writing unions.)
%%
%% Unions may not immediately contain other unions.
%% check_subtypes(SubTypes) ->
%%     ok.


-ifdef(EUNIT).

-include_lib("eunit/include/eunit.hrl").

create_test() ->
    Type = type([avro_schema:null(), avro_schema:string()]),
    ?assertFalse(is_compatible_with(Type, avro_primitive:int(1))).

-endif.
