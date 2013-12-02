%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_record).

%% API
-export([type/4]).
-export([name/1]).
-export([namespace/1]).
-export([field/3]).
-export([field/4]).
-export([get_field_type/2]).

-export([new/1]).
-export([get/2]).
-export([set/2]).
-export([set/3]).
-export([to_list/1]).
-export([check/1]).

-include("erlavro.hrl").

%%%===================================================================
%%% API: Type
%%%===================================================================

type(Name, Namespace, Doc, Fields) ->
  Type = #avro_record_type
         { name      = Name
         , namespace = Namespace
         , doc       = Doc
         , fields    = Fields
         , fullname  = avro:build_type_fullname(Name, Namespace, "")
         },
  avro:verify_type(Type),
  Type.

field(Name, Type, Doc) ->
    field(Name, Type, Doc, undefined).

field(Name, Type, Doc, Default) ->
    #avro_record_field
    { name    = Name
    , type    = Type
    , doc     = Doc
    , default = Default
    }.

name(Record) when ?AVRO_IS_RECORD_TYPE(Record) ->
  Record#avro_record_type.name.

namespace(Record) when ?AVRO_IS_RECORD_TYPE(Record) ->
  Record#avro_record_type.namespace.

get_field_type(FieldName, Type) when ?AVRO_IS_RECORD_TYPE(Type) ->
    case get_field_def(FieldName, Type) of
        {ok, #avro_record_field{type = FieldType}} -> FieldType;
        false  -> raise_unknown_field(FieldName, Type)
    end.

%%%===================================================================
%%% API
%%%===================================================================

-spec new(#avro_record_type{}) -> avro_value().

%% TODO: initialize fields with default values
new(Type) when ?AVRO_IS_RECORD_TYPE(Type) ->
    #avro_value
    { type = Type
    , data = []
    }.

-spec get(string(), #avro_value{}) -> avro_value().

get(FieldName, Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
    case lists:keyfind(FieldName, 1, ?AVRO_VALUE_DATA(Record)) of
        {_, V} -> {ok, V};
        false  -> false
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
        lists:keystore(FieldName, 1, ?AVRO_VALUE_DATA(Record),
                       {FieldName, Value});
      false ->
        raise_unknown_field(FieldName, ?AVRO_VALUE_TYPE(Record))
    end,
  Record#avro_value{data = NewData}.

%% Extract fields and their values from the record.
to_list(Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
  ?AVRO_VALUE_DATA(Record).

check(_Record) ->
    %% TODO: complete
    true.

%%%===================================================================
%%% Internal functions
%%%===================================================================

raise_unknown_field(FieldName, Type) ->
    erlang:error({avro_error, {unknown_field, FieldName, Type}}).

get_field_def(FieldName, #avro_record_type{fields = Fields}) ->
  case lists:keyfind(FieldName, #avro_record_field.name, Fields) of
    false -> false;
    Def -> {ok, Def}
  end.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

type_test() ->
  Field = field("invno", avro_primitive:long_type(), ""),
  Schema = type("Test", "name.space", "", [Field]),
  ?assertEqual("name.space.Test", avro:get_type_fullname(Schema)),
  ?assertEqual({ok, Field}, get_field_def("invno", Schema)).

get_field_type_test() ->
  Field = field("invno", avro_primitive:long_type(), ""),
  Schema = type("Test", "name.space", "", [Field]),
  ?assertEqual(avro_primitive:long_type(), get_field_type("invno", Schema)).

get_set_test() ->
  Schema = type("Test", "name.space", "",
                [field("invno", avro_primitive:long_type(), "")]),
  Rec0 = avro_record:new(Schema),
  Rec1 = set("invno", avro_primitive:long(1), Rec0),
  ?assertEqual({ok, avro_primitive:long(1)}, get("invno", Rec1)).

to_list_test() ->
  Schema = type("Test", "name.space", "",
                [ field("invno", avro_primitive:long_type(), "")
                , field("name", avro_primitive:string_type(), "")
                ]),
  Rec0 = avro_record:new(Schema),
  Rec1 = set([ {"invno", avro_primitive:long(1)}
             , {"name", avro_primitive:string("some name")}
             ],
             Rec0),
  L = to_list(Rec1),
  ?assertEqual(2, length(L)),
  ?assertEqual({"invno", avro_primitive:long(1)},
               lists:keyfind("invno", 1, L)),
  ?assertEqual({"name", avro_primitive:string("some name")},
               lists:keyfind("name", 1, L)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
