%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_array).

%% API
-export([type/1]).
-export([get_items_type/1]).

-export([new/1]).
-export([new/2]).

-export([prepend/2]).
-export([append/2]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type(SubType) ->
    #avro_array_type{ type = SubType }.

get_items_type(ArrayType) when ?AVRO_IS_ARRAY_TYPE(ArrayType) ->
    ArrayType#avro_array_type.type.

new(Type) ->
    new(Type, []).

new(Type, List) when ?AVRO_IS_ARRAY_TYPE(Type) andalso is_list(List) ->
    ?AVRO_VALUE(Type, List).

prepend(Items, Value) when ?AVRO_IS_ARRAY_VALUE(Value) ->
    ?AVRO_UPDATE_VALUE(Value, Items ++ ?AVRO_VALUE_DATA(Value)).

append(Items, Value) when ?AVRO_IS_ARRAY_VALUE(Value) ->
    ?AVRO_UPDATE_VALUE(Value, ?AVRO_VALUE_DATA(Value) ++ Items).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
