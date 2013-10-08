%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_primitive).

%% API
-export([null/0]).
-export([boolean/1]).
-export([int/1]).
-export([long/1]).
-export([float/1]).
-export([double/1]).
-export([bytes/1]).
-export([string/1]).

-include_lib("erlavro/include/erlavro.hrl").


%%%===================================================================
%%% API
%%%===================================================================

null() ->
    #avro_value{ type = avro_schema:null(), value = null }.

boolean(Value) when erlang:is_boolean(Value) ->
    #avro_value{ type = avro_schema:boolean(), value = Value }.

int(Value) when Value >= -2147483648 andalso
                Value =<  2147483647 ->
    #avro_value{ type = avro_schema:int(), value = Value }.

long(Value) when Value >= -9223372036854775808 andalso
                 Value =<  9223372036854775807 ->
    #avro_value{ type = avro_schema:long(), value = Value }.

float(Value) when erlang:is_float(Value) ->
    #avro_value{ type = avro_schema:float(), value = Value }.

double(Value) when erlang:is_float(Value) ->
    #avro_value{ type = avro_schema:double(), value = Value }.

bytes(Value) when is_binary(Value) ->
    #avro_value{ type = avro_schema:bytes(), value = Value }.

string(Value) when is_list(Value) ->
    #avro_value{ type = avro_schema:string(), value = Value }.

%%%===================================================================
%%% Internal functions
%%%===================================================================
