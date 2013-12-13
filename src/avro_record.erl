%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_record).

%% API
-export([type/4]).
-export([type/6]).
-export([field/3]).
-export([field/4]).
-export([get_field_type/2]).

-export([cast/2]).

-export([new/2]).
-export([get/2]).
-export([set/2]).
-export([set/3]).
-export([update/3]).
-export([to_list/1]).

-include("erlavro.hrl").

%%%===================================================================
%%% API: Type
%%%===================================================================

type(Name, Namespace, Doc, Fields) ->
  type(Name, Namespace, Doc, Fields, [], "").

type(Name, Namespace, Doc, Fields, Aliases, EnclosingNs) ->
  Type = #avro_record_type
         { name      = Name
         , namespace = Namespace
         , doc       = Doc
         , fields    = Fields
         , aliases   = Aliases
         , fullname  = avro:build_type_fullname(Name, Namespace, EnclosingNs)
         },
  avro_check:verify_type(Type),
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

get_field_type(FieldName, Type) when ?AVRO_IS_RECORD_TYPE(Type) ->
    case get_field_def(FieldName, Type) of
        {ok, #avro_record_field{type = FieldType}} -> FieldType;
        false -> erlang:error({unknown_field, FieldName})
    end.

%%%===================================================================
%%% API: casting
%%%===================================================================

%% Records can be casted from other records or from proplists.

-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.

cast(Type, Value) when ?AVRO_IS_RECORD_TYPE(Type) ->
  do_cast(Type, Value).

%%%===================================================================
%%% API
%%%===================================================================

-spec new(#avro_record_type{}, term()) -> avro_value().

%% TODO: initialize fields with default values
new(Type, Value) when ?AVRO_IS_RECORD_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Rec}    -> Rec;
    {error, Err} -> erlang:error(Err)
  end.

-spec get(string(), avro_value()) -> avro_value().

get(FieldName, Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
    case lists:keyfind(FieldName, 1, ?AVRO_VALUE_DATA(Record)) of
        {_N, _T, V} -> V;
        false       -> erlang:error({unknown_field, FieldName})
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
  Data = ?AVRO_VALUE_DATA(Record),
  NewData =
    case lists:keytake(FieldName, 1, Data) of
      {value, {_,T,_}, Rest} ->
        case avro:cast(T, Value) of
          {ok, NewValue} -> [{FieldName, T, NewValue}|Rest];
          Err            -> erlang:error(Err)
        end;
      false ->
        erlang:error({unknown_field, FieldName})
    end,
  Record#avro_value{data = NewData}.

%% Update the value of a field using provided function.
%% update(FieldName, Fun, Record) is equivalent to
%% set(FieldName, Fun(get(FieldName,Record)), Record),
%% but faster.
-spec update(string(), function(), avro_value()) -> avro_value().

update(FieldName, Fun, Record) ->
  Data = ?AVRO_VALUE_DATA(Record),
  NewData =
    case lists:keytake(FieldName, 1, Data) of
      {value, {_,T,OldValue}, Rest} ->
        case avro:cast(T, Fun(OldValue)) of
          {ok, NewValue} -> [{FieldName, T, NewValue}|Rest];
          {error, Err}   -> erlang:error(Err)
        end;
      false ->
        erlang:error({unknown_field, FieldName})
    end,
  Record#avro_value{data = NewData}.

%% Extract fields and their values from the record.
to_list(Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
  lists:map(
    fun({N, _T, V}) ->
        {N,V}
    end,
    ?AVRO_VALUE_DATA(Record)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

cast_field_value(FieldName, FieldType, Value, Acc) ->
  case avro:cast(FieldType, Value) of
    {ok, CastedValue} -> [{FieldName, FieldType, CastedValue}|Acc];
    Err               -> Err
  end.

do_cast(Type, Value) when ?AVRO_IS_RECORD_VALUE(Value) ->
  %% When casting from other record only equality of full names
  %% is checked.
  TargetTypeFullName = Type#avro_record_type.fullname,
  ValueType = ?AVRO_VALUE_TYPE(Value),
  ValueTypeFullName = ValueType#avro_record_type.fullname,
  if TargetTypeFullName =:= ValueTypeFullName -> {ok, Value};
     true                                     -> {error, type_name_mismatch}
  end;
do_cast(Type, Proplist) when is_list(Proplist) ->
  CastResult =
    lists:foldl(
      fun(_FieldDef, {error, _} = Acc) ->
          %% Don't do anything after the first error
          Acc;
         (FieldDef, Acc) ->
          #avro_record_field
            { name = FieldName
            , type = FieldType
            , default = Default
            } = FieldDef,
          case lists:keyfind(FieldName, 1, Proplist) of
            {_, Value} ->
              %% Data has value for the current field, cast it
              cast_field_value(FieldName, FieldType, Value, Acc);
            false ->
              %% There is no value for the current field in Data,
              %% try to use default value if provided
              case Default of
                undefined -> {error, {required_field_missed, FieldName}};
                _       -> cast_field_value(FieldName, FieldType, Default, Acc)
              end
          end
      end,
      [],
      Type#avro_record_type.fields),
  case CastResult of
    {error, _} = Err -> Err;
    Data             -> {ok, ?AVRO_VALUE(Type, Data)}
  end.

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

default_fields_test() ->
  Field = field("invno",
                avro_primitive:long_type(),
                "Invoice number",
                avro_primitive:long(10)),
  Schema = type("Test", "name.space", "", [Field]),
  Rec = new(Schema, []),
  ?assertEqual(avro_primitive:long(10), get("invno", Rec)).

get_set_test() ->
  Schema = type("Test", "name.space", "",
                [field("invno", avro_primitive:long_type(), "")]),
  Rec0 = avro_record:new(Schema, [{"invno", 0}]),
  Rec1 = set("invno", avro_primitive:long(1), Rec0),
  ?assertEqual(avro_primitive:long(1), get("invno", Rec1)).

update_test() ->
  Schema = type("Test", "name.space", "",
                [field("invno", avro_primitive:long_type(), "")]),
  Rec0 = avro_record:new(Schema, [{"invno", 10}]),
  Rec1 = update("invno",
                fun(X) ->
                    avro_primitive:long(avro_primitive:get_value(X)*2)
                end,
                Rec0),
  ?assertEqual(avro_primitive:long(20), get("invno", Rec1)).

to_list_test() ->
  Schema = type("Test", "name.space", "",
                [ field("invno", avro_primitive:long_type(), "")
                , field("name", avro_primitive:string_type(), "")
                ]),
  Rec = avro_record:new(Schema, [ {"invno", avro_primitive:long(1)}
                                , {"name", avro_primitive:string("some name")}
                                ]),
  L = to_list(Rec),
  ?assertEqual(2, length(L)),
  ?assertEqual({"invno", avro_primitive:long(1)},
               lists:keyfind("invno", 1, L)),
  ?assertEqual({"name", avro_primitive:string("some name")},
               lists:keyfind("name", 1, L)).

cast_test() ->
  RecordType = type("Record", "namespace", "",
                    [ field("a", avro_primitive:string_type(), "")
                    , field("b", avro_primitive:int_type(), "")
                    ]),
  {ok, Record} = cast(RecordType, [{"b", 1},
                                   {"a", "foo"}]),
  ?assertEqual(avro_primitive:string("foo"), avro_record:get("a", Record)),
  ?assertEqual(avro_primitive:int(1), avro_record:get("b", Record)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
