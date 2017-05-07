%%%-----------------------------------------------------------------------------
%%%
%%% Copyright (c) 2013-2017 Klarna AB
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%%-----------------------------------------------------------------------------

-module(avro_record).

%% API
-export([ cast/2
        , define_field/2
        , define_field/3
        , encode/3
        , get_all_field_types/1
        , get_field_type/2
        , get_value/2
        , new/2
        , resolve_fullname/2
        , set_values/2
        , set_value/3
        , to_list/1
        , to_term/1
        , type/2
        , type/3
        , update/3
        ]).

-include("avro_internal.hrl").

-ifdef(TEST).
-export([get_field_def/2]).
-endif.

-type field_name() :: binary().
-type field_name_raw() :: atom() | string() | binary().

-type field_opt_name() :: doc | order | default | aliases.
-type record_opt_name() :: doc | namespace | aliases.

-type record_opts() :: [{record_opt_name(), term()}].
-type field_opts() :: [{field_opt_name(), term()}].

%%%_* Type APIs ================================================================

%% @doc Declare a record type with pre-defined record fields
%% and default type properties
%% @end
-spec type(name_raw(), [record_field()]) -> record_type().
type(Name, Fields) ->
  type(Name, Fields, []).

%% @doc Declare a record type with pre-defined record fields.
-spec type(name_raw(), [record_field()], record_opts()) -> record_type().
type(Name0, Fields0, Opts) ->
  {Name, Ns0} = avro:split_type_name(Name0, ?NS_GLOBAL),
  Ns          = ?NAME(avro_util:get_opt(namespace, Opts, Ns0)),
  true        = (Ns0 =:= ?NS_GLOBAL orelse Ns0 =:= Ns), %% assert
  Doc         = avro_util:get_opt(doc, Opts, ?NO_DOC),
  Aliases     = avro_util:get_opt(aliases, Opts, []),
  Fields      = resolve_field_type_fullnames(Fields0, Ns),
  ok          = avro_util:verify_aliases(Aliases),
  Type = #avro_record_type
         { name      = Name
         , namespace = Ns
         , doc       = ?DOC(Doc)
         , fields    = Fields
         , aliases   = avro_util:canonicalize_aliases(Aliases, Ns)
         , fullname  = avro:build_type_fullname(Name, Ns)
         , custom    = avro_util:canonicalize_custom_props(Opts)
         },
  ok = avro_util:verify_type(Type),
  Type.

%% @doc Resolve fullname by newly discovered enclosing namespace.
-spec resolve_fullname(record_type(), namespace()) -> record_type().
resolve_fullname(#avro_record_type{ fullname  = FullName
                                  , fields    = Fields
                                  , aliases   = Aliases
                                  } = T, Ns) ->
  NewFullName = avro:build_type_fullname(FullName, Ns),
  NewFields = resolve_field_type_fullnames(Fields, Ns),
  NewAliases = avro_util:canonicalize_aliases(Aliases, Ns),
  T#avro_record_type{ fullname = NewFullName
                    , fields   = NewFields
                    , aliases  = NewAliases
                    }.

%% @doc Define a record field with default properties.
-spec define_field(name_raw(), type_or_name()) -> record_field().
define_field(Name, Type) ->
  define_field(Name, Type, []).

%% @doc Define a record field.
-spec define_field(name_raw(), type_or_name(), field_opts()) ->
        record_field().
define_field(Name, Type0, Opts) ->
  Type = avro_util:canonicalize_type_or_name(Type0),
  Doc = avro_util:get_opt(doc, Opts, ?NO_DOC),
  Order = avro_util:get_opt(order, Opts, ascending),
  Default = avro_util:get_opt(default, Opts, undefined),
  Aliases = avro_util:get_opt(aliases, Opts, []),
  ok = avro_util:verify_names(Aliases),
  #avro_record_field
  { name    = ?NAME(Name)
  , type    = Type
  , doc     = ?DOC(Doc)
  , default = Default
  , order   = Order
  , aliases = lists:map(fun(A) -> ?NAME(A) end, Aliases)
  }.

%% @doc Returns type of the specified field.
%% Aliases can be used for FieldName.
%% @end
-spec get_field_type(field_name_raw(), record_type()) ->
        avro_type() | no_return().
get_field_type(FieldName, Type) when ?IS_RECORD_TYPE(Type) ->
  case get_field_def(FieldName, Type) of
    {ok, #avro_record_field{type = FieldType}} -> FieldType;
    false -> erlang:error({unknown_field, FieldName})
  end.

%% @doc Get all field types in a tuple list
%% with filed name and field type zipped.
%% @end
-spec get_all_field_types(record_type()) ->
        [{field_name(), type_or_name()}].
get_all_field_types(Type) when ?IS_RECORD_TYPE(Type) ->
  #avro_record_type{fields = Fields} = Type,
  lists:map(
    fun(#avro_record_field{ name = FieldName
                          , type = FieldTypeOrName
                          }) ->
      {FieldName, FieldTypeOrName}
    end, Fields).

%%%_* Value APIs ===============================================================

-spec cast(avro_type(), [{field_name_raw(), avro:in()}]) ->
        {ok, avro_value()} | {error, any()}.
cast(Type, Value) when ?IS_RECORD_TYPE(Type) ->
  do_cast(Type, Value).

-spec new(record_type(), avro:in()) -> avro_value().
new(Type, Value) when ?IS_RECORD_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Rec}    -> Rec;
    {error, Err} -> erlang:error(Err)
  end.

-spec get_value(field_name_raw(), avro_value()) -> avro_value() | no_return().
get_value(FieldName, Record) when not ?IS_NAME(FieldName) ->
  get_value(?NAME(FieldName), Record);
get_value(FieldName, Record) when ?IS_RECORD_VALUE(Record) ->
  Data = ?AVRO_VALUE_DATA(Record),
  ok = ?ASSERT_AVRO_VALUE(Data),
  case lists:keyfind(FieldName, 1, Data) of
    {_N, V} -> V;
    false   -> erlang:error({unknown_field, FieldName})
  end.

%% @private Set values for multiple fields in one call.
-spec set_values([{field_name_raw(), any()}], avro_value()) ->
        avro_value() | no_return().
set_values(Values, Record) ->
  lists:foldl(
    fun({FieldName, Value}, R) ->
        set_value(FieldName, Value, R)
    end,
    Record,
    Values).

%% @private Set value for the specified field.
-spec set_value(field_name_raw(), avro_value(), avro_value()) ->
        avro_value() | no_return().
set_value(FieldName, Value, Record) when not ?IS_NAME(FieldName) ->
  set_value(?NAME(FieldName), Value, Record);
set_value(FieldName, Value, Record) when ?IS_RECORD_VALUE(Record) ->
  Data = ?AVRO_VALUE_DATA(Record),
  ok = ?ASSERT_AVRO_VALUE(Data),
  UpdateFun = fun(_OldFieldValue) -> Value end,
  update(FieldName, UpdateFun, Record).

%% @private Update the value of a field using provided function.
%% update(FieldName, Fun, Record) is equivalent to
%% set_value(FieldName, Fun(get(FieldName,Record)), Record),
%% but faster.
%% @end
-spec update(field_name_raw(), function(), avro_value()) ->
        avro_value() | no_return().
update(FieldName, Fun, Record) when not ?IS_NAME(FieldName) ->
  update(?NAME(FieldName), Fun, Record);
update(FieldName, Fun, Record) ->
  Data = ?AVRO_VALUE_DATA(Record),
  ok = ?ASSERT_AVRO_VALUE(Data),
  NewData =
    case lists:keytake(FieldName, 1, Data) of
      {value, {_, OldValue}, Rest} ->
        T = ?AVRO_VALUE_TYPE(OldValue),
        case avro:cast(T, Fun(OldValue)) of
          {ok, NewValue}  -> [{FieldName, NewValue} | Rest];
          {error, Reason} -> erlang:error({FieldName, Reason})
        end;
      false ->
        erlang:error({unknown_field, FieldName})
    end,
  Record#avro_value{data = NewData}.

%% @doc Extract fields and their values from the record.
-spec to_list(avro_value()) -> [{field_name(), avro_value()}].
to_list(Record) when ?IS_RECORD_VALUE(Record) ->
  Data = ?AVRO_VALUE_DATA(Record),
  ok = ?ASSERT_AVRO_VALUE(Data),
  Data.

%% @doc Recursively unbox field values.
-spec to_term(avro_value()) -> avro:out().
to_term(Record) when ?IS_RECORD_VALUE(Record) ->
  lists:map(fun({N, V}) -> {N, avro:to_term(V)} end, to_list(Record)).

%% @hidden Help function for JSON/binary encoder.
-spec encode(record_type(), [{field_name_raw(), avro:in()}],
             fun(({field_name(), avro_type(), avro:in()}) -> avro:out())) ->
        [avro:out()].
encode(Type, Fields, EncodeFun) ->
  FieldTypes = get_all_field_types(Type),
  TypeFullName = avro:get_type_fullname(Type),
  TypeAndValueList = zip_record_field_types_with_key_value(
                       TypeFullName, FieldTypes, Fields),
  lists:map(fun({FieldName, _FieldType, _FieldValue} = X) ->
                try
                  EncodeFun(X)
                catch
                  C : E -> ?RAISE_ENC_ERR(C, E, [TypeFullName, FieldName])
                end
            end, TypeAndValueList).

%%%_* Internal functions =======================================================

%% @private Incase the children types are defined without namespace,
%% their fullnames need to be resolved with THIS record type's namespace
%% as enclosing namespace.
%% @end
-spec resolve_field_type_fullnames([record_field()], namespace()) ->
        [record_field()].
resolve_field_type_fullnames(Fields, Ns) ->
  F = fun(#avro_record_field{type = Type} = Field) ->
          Field#avro_record_field{type = avro:resolve_fullname(Type, Ns)}
      end,
  lists:map(F, Fields).

%% @private
-spec zip_record_field_types_with_key_value(RecordTypeName :: fullname(),
                                            [{field_name(), avro_type()}],
                                            [{field_name_raw(), avro:in()}]) ->
        [{field_name(), avro_type(), avro:in()}].
zip_record_field_types_with_key_value(_Name, [], []) -> [];
zip_record_field_types_with_key_value(Name, [{FieldName, FieldType} | Rest],
    FieldValues0) ->
  {FieldValue, FieldValues} =
    take_record_field_value(Name, FieldName, FieldValues0, []),
  [ {FieldName, FieldType, FieldValue}
  | zip_record_field_types_with_key_value(Name, Rest, FieldValues)
  ].

%% @private
-spec take_record_field_value(fullname(), field_name(),
                              [{field_name_raw(), avro:in()}],
                              [{field_name_raw(), avro:in()}]) ->
        {avro:in(), [{field_name_raw(), avro:in()}]}.
take_record_field_value(RecordName, FieldName, [], _) ->
  erlang:error({field_value_not_found, RecordName, FieldName});
take_record_field_value(RecordName, FieldName, [{Tag, Value} | Rest], Tried) ->
  case ?NAME(Tag) =:= FieldName of
    true ->
      {Value, Tried ++ Rest};
    false ->
      take_record_field_value(RecordName, FieldName, Rest,
                              [{Tag, Value} | Tried])
  end.

%% @private Try to find a value for a field specified by list of its names
%% (including direct name and aliases)
%% @end
-spec lookup_value_by_name([field_name()], [{field_name(), avro:in()}]) ->
        {ok, avro:in()} | false.
lookup_value_by_name([], _Values) ->
  false;
lookup_value_by_name([FieldName|Rest], Values) ->
  case lists:keyfind(FieldName, 1, Values) of
    {_, Value} -> {ok, Value};
    false      -> lookup_value_by_name(Rest, Values)
  end.

%% @private
-spec lookup_value_from_list(record_field(), [{field_name(), avro:in()}]) ->
        undefined | avro:in() | avro_value().
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

%% @private
-spec cast_fields([record_field()], [{field_name(), avro:in()}],
                  [{field_name(), avro_value()}]) ->
        [{field_name(), avro_value()}] | {error, any()}.
cast_fields([], _Values, Acc) ->
  lists:reverse(Acc);
cast_fields([FieldDef | Rest], Values, Acc) ->
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
          cast_fields(Rest, Values, [{FieldName, CastedValue} | Acc]);
        {error, Reason} ->
          {error, {FieldName, Reason}}
      end
  end.

%% @private
-spec do_cast(record_type(), [{field_name_raw(), avro:in()}]) ->
        {ok, avro_value()} | {error, any()}.
do_cast(Type, KvList0) when is_list(KvList0) ->
  %% unify field names to binary
  KvList = lists:map(fun({K, V}) -> {?NAME(K), V} end, KvList0),
  case cast_fields(Type#avro_record_type.fields, KvList, []) of
    {error, _} = Err -> Err;
    FieldsWithValues -> {ok, ?AVRO_VALUE(Type, FieldsWithValues)}
  end.

%% @private
-spec get_field_def(field_name_raw(), record_type()) ->
        {ok, record_field()} | false.
get_field_def(FieldName0, #avro_record_type{fields = Fields}) ->
  FieldName = ?NAME(FieldName0),
  case lists:keyfind(FieldName, #avro_record_field.name, Fields) of
    #avro_record_field{} = Def -> {ok, Def};
    false ->
      %% Field definition has not been found by its direct name,
      %% try to search by aliases
      get_field_def_by_alias(FieldName, Fields)
  end.

%% @private
-spec get_field_def_by_alias(name(), [record_field()]) ->
        {ok, record_field()} | false.
get_field_def_by_alias(_Alias, []) ->
  false;
get_field_def_by_alias(Alias, [FieldDef | Rest]) ->
  case lists:member(Alias, FieldDef#avro_record_field.aliases) of
    true  -> {ok, FieldDef};
    false -> get_field_def_by_alias(Alias, Rest)
  end.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
