%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_record).

%% API
-export([is_record/1]).
-export([new/1]).
-export([get/2]).
-export([set/3]).
-export([check/1]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

-spec is_record(avro_value()) -> boolean().

is_record(#avro_value{type = #avro_record_type{}}) -> true;
is_record(_)                                       -> false.


-spec new(#avro_record_type{}) -> avro_value().

new(#avro_record_type{} = Type) ->
    #avro_value
    { type  = Type
    , value = dict:new()
    }.


-spec get(string(), #avro_value{}) -> avro_value().

get(Field, Record) ->
    case dict:find(Field, Record#avro_value.value) of
        {ok, _} = R -> R;
        error       -> false
    end.


-spec set(string(), avro_value(), avro_value()) -> avro_value().

set(Field, _Value, Record) ->
    case get_field_def(Field, Record) of
        {ok, _FieldDef} ->
            todo;
        false ->
            erlang:error({error, unknown_field})
    end.


check(_Record) ->
    %% TODO: complete
    true.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_field_def(FieldName, #avro_record_type{fields = Fields}) ->
    get_field_def(FieldName, Fields);
get_field_def(_FieldName, []) ->
    false;
get_field_def(FieldName, [#avro_field{name = FieldName} = FieldDef | _]) ->
    {ok, FieldDef};
get_field_def(FieldName, [_ | Rest]) ->
    get_field_def(FieldName, Rest).
