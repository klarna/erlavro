%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro_record).

%% API
-export([type/2]).
-export([type/3]).
-export([type/4]). %% DEPRECATED
-export([type/6]). %% DEPRECATED
-export([define_field/2]).
-export([define_field/3]).
-export([field/3]). %% DEPRECATED
-export([field/4]). %% DEPRECATED
-export([get_field_type/2]).

-export([cast/2]).

-export([new/2]).
-export([get/2]). %% DEPRECATED
-export([get_value/2]).
-export([set/2]). %% DEPRECATED
-export([set/3]). %% DEPRECATED
-export([set_values/2]).
-export([set_value/3]).
-export([update/3]).
-export([to_list/1]).

-include("erlavro.hrl").

%% Record internals:
%% Data is a list of {Name, Type, Value} tuples, where
%%   Name is a field name;
%%   Type is a field type (same type as in the field schema);
%%   Value is a field value (#avro_value{}).
%%
%% Type is duplicated in the type spec and in data so we don't need to
%% lookup type explicitly (one lookup instead of two).
%%
%% Data always contains values for all fields, even for non-required ones.

%%%===================================================================
%%% API: Type
%%%===================================================================

type(Name, Fields) ->
  type(Name, Fields, []).

%% Options:
%%   namespace    :: string()
%%   doc          :: string()
%%   aliases      :: [string()]
%%   enclosing_ns :: string()
type(Name, Fields, Opts) ->
  Ns = avro_util:get_opt(namespace, Opts, ""),
  Doc = avro_util:get_opt(doc, Opts, ""),
  Aliases = avro_util:get_opt(aliases, Opts, []),
  EnclosingNs = avro_util:get_opt(enclosing_ns, Opts, ""),
  avro_util:verify_aliases(Aliases),
  Type = #avro_record_type
         { name      = Name
         , namespace = Ns
         , doc       = Doc
         , fields    = Fields
         , aliases   = avro_util:canonicalize_aliases(
                         Aliases, Name, Ns, EnclosingNs)
         , fullname  = avro:build_type_fullname(Name, Ns, EnclosingNs)
         },
  avro_util:verify_type(Type),
  Type.

%% DEPRECATED: Use type/2,3 instead
type(Name, Namespace, Doc, Fields) ->
  type(Name, Fields,
       [ {namespace, Namespace}
       , {doc, Doc}
       ]).

%% DEPRECATED: Use type/2,3 instead
type(Name, Namespace, Doc, Fields, Aliases, EnclosingNs) ->
  type(Name, Fields,
       [ {namespace, Namespace}
       , {doc, Doc}
       , {aliases, Aliases}
       , {enclosing_ns, EnclosingNs}
       ]).

define_field(Name, Type) ->
  define_field(Name, Type, []).

%% Options:
%%   doc     :: string()
%%   order   :: avro_ordering()
%%   default :: avro_value()
%%   aliases :: [string()]
define_field(Name, Type, Opts) ->
  Doc = avro_util:get_opt(doc, Opts, ""),
  Order = avro_util:get_opt(order, Opts, ascending),
  Default = avro_util:get_opt(default, Opts, undefined),
  Aliases = avro_util:get_opt(aliases, Opts, []),
  avro_util:verify_names(Aliases),
  #avro_record_field
  { name    = Name
  , type    = Type
  , doc     = Doc
  , default = Default
  , order   = Order
  , aliases = Aliases
  }.

%% DEPRECATED: use define_field instead
field(Name, Type, Doc) ->
    field(Name, Type, Doc, undefined).

%% DEPRECATED: use define_field instead
field(Name, Type, Doc, Default) ->
  define_field(Name, Type,
               [ {doc, Doc}
               , {default, Default}
               ]).

%% Returns type of the specified field. Aliases can be used for FieldName.
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

%% DEPRECATED: use get_value instead
get(FieldName, Record) ->
  get_value(FieldName, Record).

-spec get_value(string(), avro_value()) -> avro_value().

get_value(FieldName, Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
    case lists:keyfind(FieldName, 1, ?AVRO_VALUE_DATA(Record)) of
        {_N, _T, V} -> V;
        false       -> erlang:error({unknown_field, FieldName})
    end.

%% DEPRECATED: use set_values/2 instead
set(Values, Record) ->
  set_values(Values, Record).

%% Set values for multiple fields in one call
-spec set_values([{string(), any()}], avro_value()) -> avro_value().

set_values(Values, Record) ->
    lists:foldl(
      fun({FieldName, Value}, R) ->
              set_value(FieldName, Value, R)
      end,
      Record,
      Values).

%% DEPRECATED: use set_value/3 instead
set(FieldName, Value, Record) ->
  set_value(FieldName, Value, Record).

%% Set value for the specified field
-spec set_value(string(), avro_value(), avro_value()) -> avro_value().

set_value(FieldName, Value, Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
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

%% Try to find a value for a field specified by list of its names
%% (including direct name and aliases)
lookup_value_by_name([], _Values) ->
  false;
lookup_value_by_name([FieldName|Rest], Values) ->
  case lists:keyfind(FieldName, 1, Values) of
    {_, Value} -> {ok, Value};
    false      -> lookup_value_by_name(Rest, Values)
  end.

lookup_value_from_list(FieldDef, Values) ->
  #avro_record_field
  { name = FieldName
  , default = Default
  , aliases = Aliases
  } = FieldDef,
  case lookup_value_by_name([FieldName|Aliases], Values) of
    {ok, Value} -> Value;
    false       -> Default
  end.

cast_fields([], _Values, Acc) ->
  lists:reverse(Acc);
cast_fields([FieldDef|Rest], Values, Acc) ->
  #avro_record_field
  { name = FieldName
  , type = FieldType
  } = FieldDef,
  case lookup_value_from_list(FieldDef, Values) of
    undefined ->
      {error, {required_field_missed, FieldName}};
    Value ->
      case avro:cast(FieldType, Value) of
        {ok, CastedValue} ->
          cast_fields(Rest, Values, [{FieldName, FieldType, CastedValue}|Acc]);
        Err ->
          Err
      end
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
  FieldsWithValues = cast_fields(Type#avro_record_type.fields, Proplist, []),
  case FieldsWithValues of
    {error, _} = Err -> Err;
    _                -> {ok, ?AVRO_VALUE(Type, FieldsWithValues)}
  end.

get_field_def(FieldName, #avro_record_type{fields = Fields}) ->
  case lists:keyfind(FieldName, #avro_record_field.name, Fields) of
    #avro_record_field{} = Def -> {ok, Def};
    false ->
      %% Field definition has not been found by its direct name,
      %% try to search by aliases
      get_field_def_by_alias(FieldName, Fields)
  end.

get_field_def_by_alias(_Alias, []) ->
  false;
get_field_def_by_alias(Alias, [FieldDef|Rest]) ->
  case lists:member(Alias, FieldDef#avro_record_field.aliases) of
    true -> {ok, FieldDef};
    false -> get_field_def_by_alias(Alias, Rest)
  end.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

type_test() ->
  Field = define_field("invno", avro_primitive:long_type()),
  Schema = type("Test", [Field],
                [ {namespace, "name.space"}
                ]),
  ?assertEqual("name.space.Test", avro:get_type_fullname(Schema)),
  ?assertEqual({ok, Field}, get_field_def("invno", Schema)).

get_field_def_test() ->
  Field1 = define_field("f1", avro_primitive:long_type()),
  Field2 = define_field("f2", avro_primitive:long_type(),
                        [{aliases, ["a1", "a2"]}]),
  Field3 = define_field("f3", avro_primitive:long_type()),
  Record = type("Test", [Field1, Field2, Field3]),
  ?assertEqual(false, get_field_def("f4", Record)),
  ?assertEqual({ok, Field2}, get_field_def("f2", Record)),
  ?assertEqual({ok, Field3}, get_field_def("f3", Record)),
  ?assertEqual({ok, Field2}, get_field_def("a2", Record)).

get_field_type_test() ->
  Field = define_field("invno", avro_primitive:long_type()),
  Schema = type("Test", [Field],
                [ {namespace, "name.space"}
                ]),
  ?assertEqual(avro_primitive:long_type(), get_field_type("invno", Schema)).

default_fields_test() ->
  Field = define_field("invno",
                       avro_primitive:long_type(),
                       [ {default, avro_primitive:long(10)}
                       ]),
  Schema = type("Test", [Field],
                [ {namespace, "name.space"}
                ]),
  Rec = new(Schema, []),
  ?assertEqual(avro_primitive:long(10), get_value("invno", Rec)).

get_set_test() ->
  Schema = type("Test",
                [define_field("invno", avro_primitive:long_type())],
                [ {namespace, "name.space"}
                ]),
  Rec0 = avro_record:new(Schema, [{"invno", 0}]),
  Rec1 = set_value("invno", avro_primitive:long(1), Rec0),
  ?assertEqual(avro_primitive:long(1), get_value("invno", Rec1)).

update_test() ->
  Schema = type("Test",
                [define_field("invno", avro_primitive:long_type())],
                [ {namespace, "name.space"}
                ]),
  Rec0 = avro_record:new(Schema, [{"invno", 10}]),
  Rec1 = update("invno",
                fun(X) ->
                    avro_primitive:long(avro_primitive:get_value(X)*2)
                end,
                Rec0),
  ?assertEqual(avro_primitive:long(20), get_value("invno", Rec1)).

to_list_test() ->
  Schema = type("Test",
                [ define_field("invno", avro_primitive:long_type())
                , define_field("name", avro_primitive:string_type())
                ],
                [ {namespace, "name.space"}
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
  RecordType = type("Record",
                    [ define_field("a", avro_primitive:string_type())
                    , define_field("b", avro_primitive:int_type())
                    ],
                    [ {namespace, "name.space"}
                    ]),
  {ok, Record} = cast(RecordType, [{"b", 1},
                                   {"a", "foo"}]),
  ?assertEqual(avro_primitive:string("foo"), get_value("a", Record)),
  ?assertEqual(avro_primitive:int(1), get_value("b", Record)).

cast_by_aliases_test() ->
  RecordType = type("Record",
                    [ define_field("a", avro_primitive:string_type(),
                                  [{aliases, ["al1", "al2"]}])
                    , define_field("b", avro_primitive:int_type(),
                                   [{aliases, ["al3", "al4"]}])
                    ],
                    [ {namespace, "name.space"}
                    ]),
  {ok, Record} = cast(RecordType, [{"al4", 1},
                                   {"al1", "foo"}]),
  ?assertEqual(avro_primitive:string("foo"), get_value("a", Record)),
  ?assertEqual(avro_primitive:int(1), get_value("b", Record)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
