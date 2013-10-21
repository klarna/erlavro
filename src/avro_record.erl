%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_record).

%% API
-export([type/4]).
-export([field/3]).
-export([field/4]).
-export([get_field_type/2]).

-export([new/1]).
-export([get/2]).
-export([set/2]).
-export([set/3]).
-export([to_list/1]).
-export([check/1]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API: Type
%%%===================================================================

type(Name, Namespace, Doc, Fields) ->
    #avro_record_type
    { name      = Name
    , namespace = Namespace
    , doc       = Doc
    , fields    = Fields
    }.

field(Name, Type, Doc) ->
    field(Name, Type, Doc, undefined).

field(Name, Type, Doc, Default) ->
    #avro_field
    { name    = Name
    , type    = Type
    , doc     = Doc
    , default = Default
    }.

get_field_type(FieldName, Type) when ?AVRO_IS_RECORD_TYPE(Type) ->
    case get_field_def(FieldName, Type) of
        {ok, #avro_field{type = FieldType}} -> FieldType;
        false  -> raise_unknown_field(FieldName, Type)
    end.

%%%===================================================================
%%% API
%%%===================================================================

-spec new(#avro_record_type{}) -> avro_value().

new(Type) when ?AVRO_IS_RECORD_TYPE(Type) ->
    #avro_value
    { type = Type
    , data = dict:new()
    }.

-spec get(string(), #avro_value{}) -> avro_value().

get(FieldName, Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
    case dict:find(FieldName, ?AVRO_VALUE_DATA(Record)) of
        {ok, _} = R -> R;
        error       -> false
    end.

set(Values, Record) ->
    lists:foldl(
      fun({FieldName, Value}, R) ->
              set(FieldName, Value, R)
      end,
      Record,
      Values).

-spec set(string(), avro_value(), avro_value()) -> avro_value().

set(FieldName, Value, Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
    NewData =
        case get_field_def(FieldName, ?AVRO_VALUE_TYPE(Record)) of
            {ok, _FieldDef} ->
                dict:store(FieldName, Value, ?AVRO_VALUE_DATA(Record));
            false ->
                raise_unknown_field(FieldName, ?AVRO_VALUE_TYPE(Record))
        end,
    Record#avro_value{data = NewData}.

%% Extract fields and their values from the record.
to_list(Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
    #avro_record_type{fields = Fields} = ?AVRO_VALUE_TYPE(Record),
    lists:foldr(fun(Field, Result) ->
                        append_field_value(Field, Record, Result)
                end,
                [], Fields).

check(_Record) ->
    %% TODO: complete
    true.

%%%===================================================================
%%% Internal functions
%%%===================================================================

raise_unknown_field(FieldName, Type) ->
    erlang:error({error, {unknown_field, FieldName, Type}}).

get_field_def(FieldName, #avro_record_type{fields = Fields}) ->
    get_field_def(FieldName, Fields);
get_field_def(_FieldName, []) ->
    false;
get_field_def(FieldName, [#avro_field{name = FieldName} = FieldDef | _]) ->
    {ok, FieldDef};
get_field_def(FieldName, [_ | Rest]) ->
    get_field_def(FieldName, Rest).

append_field_value(#avro_field{name = FieldName, default = Default},
                   Record,
                   Result) ->
    Value = case dict:find(FieldName, Record#avro_value.data) of
                {ok, V} -> V;
                error   -> Default
            end,
    [{FieldName, Value}|Result].

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(EUNIT).

-include_lib("eunit/include/eunit.hrl").

record_creation_test() ->
    Schema = #avro_record_type
             { name = "Test"
             , fields = [#avro_field{name = "invno", type = avro_schema:long()}]
             },
    Record = avro_record:new(Schema),
    avro_record:set("invno", avro_primitive:long(1)),
    ok.

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
