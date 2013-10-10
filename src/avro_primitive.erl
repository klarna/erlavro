%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_primitive).

%% API
-export([is_primitive_type/1]).

-export([null_type/0]).
-export([boolean_type/0]).
-export([int_type/0]).
-export([long_type/0]).
-export([float_type/0]).
-export([double_type/0]).
-export([bytes_type/0]).
-export([string_type/0]).

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
%%% API: Types
%%%===================================================================

is_primitive_type(#avro_primitive_type{}) -> true;
is_primitive_type(_)                      -> false.

null_type()    -> #avro_primitive_type{name = ?AVRO_NULL}.

boolean_type() -> #avro_primitive_type{name = ?AVRO_BOOLEAN}.

int_type()     -> #avro_primitive_type{name = ?AVRO_INT}.

long_type()    -> #avro_primitive_type{name = ?AVRO_LONG}.

float_type()   -> #avro_primitive_type{name = ?AVRO_FLOAT}.

double_type()  -> #avro_primitive_type{name = ?AVRO_DOUBLE}.

bytes_type()   -> #avro_primitive_type{name = ?AVRO_BYTES}.

string_type()  -> #avro_primitive_type{name = ?AVRO_STRING}.

%%%===================================================================
%%% API
%%%===================================================================

null() ->
    #avro_value{ type = null_type(), data = null }.

boolean(Value) when erlang:is_boolean(Value) ->
    #avro_value{ type = boolean_type(), data = Value }.

int(Value) when Value >= -2147483648 andalso
                Value =<  2147483647 ->
    #avro_value{ type = int_type(), data = Value }.

long(Value) when Value >= -9223372036854775808 andalso
                 Value =<  9223372036854775807 ->
    #avro_value{ type = long_type(), data = Value }.

float(Value) when erlang:is_float(Value) ->
    #avro_value{ type = float_type(), data = Value }.

double(Value) when erlang:is_float(Value) ->
    #avro_value{ type = double_type(), data = Value }.

bytes(Value) when is_binary(Value) ->
    #avro_value{ type = bytes_type(), data = Value }.

string(Value) when is_list(Value) ->
    #avro_value{ type = string_type(), data = Value }.

%%%===================================================================
%%% Internal functions
%%%===================================================================
