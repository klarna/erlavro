%%%-------------------------------------------------------------------
%%% Copyright (c) 2013-2016 Klarna AB
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
%%% @doc Stores all types in the schema.
%%%
%%% The module allows to access all types in uniform way by using
%%% their full names. After a type was successfully placed into
%%% the store all its name parts are resolved so that no further
%%% actions to work with names are needed.
%%%
%%% When type is added to the store all its children named types are
%%% extracted and stored as separate types as well. Their placeholders
%%% in the original type are replaced by their full names. The types
%%% itself keep their full names in 'name' field and empty strings
%%% in 'namespace'.
%%%
%%% Error will be thrown when name conflict is detected during type
%%% addition.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_schema_store).

%% Init/Terminate
-export([ new/0
        , new/1
        , new/2
        , close/1
        ]).

%% Import
-export([ import_file/2
        , import_files/2
        , import_schema_json/2
        ]).

%% Add/Lookup
-export([ add_type/2
        , lookup_type/2
        , to_lookup_fun/1
        ]).

%% deprecated
-export([ fold/3
        , lookup_type_json/2
        ]).

-deprecated({fold, 3, eventually}).
-deprecated({lookup_type_json, 2, eventually}).

-include("erlavro.hrl").

-opaque store() :: ets:tab().
-type option_key() :: access | name.
-type filename() :: file:filename_all().

-export_type([store/0]).

-define(IS_STORE(S), (is_integer(S) orelse is_atom(S))).

-type fullname() :: avro:fullname().

%%%_* APIs =====================================================================

%% @equiv new([])
-spec new() -> store().
new() -> new([]).

%% @doc Create a new ets table to store avro types.
%% Options:
%%   {access, public|protected|private} - has same meaning as access
%%     mode in ets:new and defines what processes can have access to
%%   {name, atom()} - used to create a named ets table.
%% @end
-spec new([{option_key(), atom()}]) -> store().
new(Options) ->
  Access = avro_util:get_opt(access, Options, public),
  {Name, EtsOpts} =
    case avro_util:get_opt(name, Options, undefined) of
      undefined -> {?MODULE, []};
      Name_     -> {Name_, [named_table]}
    end,
  ets:new(Name, [Access, {read_concurrency, true} | EtsOpts]).

%% @doc Create a new schema store and improt the given schema JSON files.
-spec new([proplists:property()], [filename()]) -> store().
new(Options, Files) ->
  Store = new(Options),
  import_files(Files, Store).

%% @doc Make a schema lookup function from store.
-spec to_lookup_fun(store()) -> fun((fullname()) -> avro_type()).
to_lookup_fun(Store) ->
  fun(Name) ->
    {ok, Type} = ?MODULE:lookup_type(Name, Store),
    Type
  end.

%% @doc Import avro JSON files into schema store.
-spec import_files([filename()], store()) -> store().
import_files(Files, Store) when ?IS_STORE(Store) ->
  lists:foldl(fun(File, S) -> import_file(File, S) end, Store, Files).

%% @doc Import avro JSON file into schema store.
-spec import_file(filename(), store()) -> store().
import_file(File, Store) when ?IS_STORE(Store) ->
  case file:read_file(File) of
    {ok, Json} ->
      import_schema_json(Json, Store);
    {error, Reason} ->
      erlang:error({failed_to_read_schema_file, File, Reason})
  end.

%% @doc Decode avro schema JSON into erlavro records.
-spec import_schema_json(binary(), store()) -> store().
import_schema_json(Json, Store) when ?IS_STORE(Store) ->
  Schema = avro_json_decoder:decode_schema(Json),
  add_type(Schema, Store).

%% @doc Delete the ets table.
-spec close(store()) -> ok.
close(Store) ->
  ets:delete(Store),
  ok.

%% @doc Add type into the schema store.
%% NOTE: the type is flattened before inserting into the schema store.
%% i.e. named types nested in the given type are lifted up to root level.
-spec add_type(avro_type(), store()) -> store().
add_type(Type, Store) when ?IS_STORE(Store) ->
  case avro:is_named_type(Type) of
    true  ->
      {ConvertedType, ExtractedTypes} =
        extract_children_types(Type),
      lists:foldl(
        fun do_add_type/2,
        do_add_type(ConvertedType, Store),
        ExtractedTypes);
    false ->
      erlang:error({unnamed_type_cant_be_added, Type})
  end.

%% @doc Lookup a type using its full name.
-spec lookup_type(fullname(), store()) -> {ok, avro_type()} | false.
lookup_type(FullName, Store) when ?IS_STORE(Store) ->
  get_type_from_store(FullName, Store).

%% @deprecated Lookup a type as in JSON (already encoded)
%% format using its full name.
%% @end
-spec lookup_type_json(fullname(), store()) -> {ok, term()} | false.
lookup_type_json(FullName, Store) when ?IS_STORE(Store) ->
  get_type_json_from_store(FullName, Store).

%% @deprecated
fold(F, Acc0, Store) ->
  ets:foldl(
    fun({{json, _FullName}, _JSON}, Acc) ->
        Acc;
       ({FullName, Type}, Acc) ->
        F(FullName, Type, Acc)
    end,
    Acc0,
    Store).

%%%_* Internal Functions =======================================================

-spec do_add_type(avro_type(), store()) -> store().
do_add_type(Type, Store) ->
  FullName = avro:get_type_fullname(Type),
  Aliases = avro:get_aliases(Type),
  do_add_type_by_names([FullName|Aliases], Type, Store).

-spec do_add_type_by_names([fullname()], avro_type(), store()) -> store().
do_add_type_by_names([], _Type, Store) ->
  Store;
do_add_type_by_names([Name|Rest], Type, Store) ->
  case get_type_from_store(Name, Store) of
    {ok, _} ->
      erlang:error({type_with_same_name_already_exists_in_store, Name});
    false   ->
      Store1 = put_type_to_store(Name, Type, Store),
      do_add_type_by_names(Rest, Type, Store1)
  end.

%% Extract all children types from the type if possible, replace extracted
%% types with their names. Types specified by names are replaced with their
%% full names. Type names are canonicalized: name is replaced by full name,
%% namespace is cleared.
-spec extract_children_types(avro_type())
                            -> {avro_type(), [avro_type()]}.

extract_children_types(Primitive)
  when ?AVRO_IS_PRIMITIVE_TYPE(Primitive) ->
  %% Primitive types can't have children type definitions
  {Primitive, []};
extract_children_types(Record) when ?AVRO_IS_RECORD_TYPE(Record) ->
  {NewFields, ExtractedTypes} =
    lists:foldr(
      fun(Field, {FieldsAcc, ExtractedAcc}) ->
          {NewType, Extracted} =
            convert_type(Field#avro_record_field.type),
          {[Field#avro_record_field{type = NewType}|FieldsAcc],
           Extracted ++ ExtractedAcc}
      end,
      {[], []},
      Record#avro_record_type.fields),
  {Record#avro_record_type{ fields = NewFields }, ExtractedTypes};
extract_children_types(Enum) when ?AVRO_IS_ENUM_TYPE(Enum) ->
  %% Enums don't have children types
  {Enum, []};
extract_children_types(Array) when ?AVRO_IS_ARRAY_TYPE(Array) ->
  ChildType = avro_array:get_items_type(Array),
  {NewChildType, Extracted} = convert_type(ChildType),
  {avro_array:type(NewChildType), Extracted};
extract_children_types(Map) when ?AVRO_IS_MAP_TYPE(Map) ->
  ChildType = Map#avro_map_type.type,
  {NewChildType, Extracted} = convert_type(ChildType),
  {Map#avro_map_type{type = NewChildType}, Extracted};
extract_children_types(Union) when ?AVRO_IS_UNION_TYPE(Union) ->
  ChildrenTypes = avro_union:get_types(Union),
  {NewChildren, ExtractedTypes} =
    lists:foldr(
      fun(ChildType, {ConvertedAcc, ExtractedAcc}) ->
          {ChildType1, Extracted} = convert_type(ChildType),
          {[ChildType1|ConvertedAcc], Extracted ++ ExtractedAcc}
      end,
      {[], []},
      ChildrenTypes),
  {avro_union:type(NewChildren), ExtractedTypes};
extract_children_types(Fixed) when ?AVRO_IS_FIXED_TYPE(Fixed) ->
  %% Fixed types don't have children types.
  {Fixed, []}.

%% Convert a type using following rules:
%% - if the type is represented by its name then the name is used.
%% - if the type is specified as a schema then the is replaced by
%%   its full name and the type itself is added to resulting list.
%%   Children types of the replaced type is also extracted.
convert_type(TypeName) when is_list(TypeName) ->
  {TypeName, []};
convert_type(Type) ->
  {NewType, Extracted} = extract_children_types(Type),
  case replace_type_with_name(NewType) of
    {ok, Name} -> {Name, [NewType|Extracted]};
    false      -> {NewType, Extracted}
  end.

replace_type_with_name(Type) ->
  case avro:is_named_type(Type) of
    true  ->
      %% Named type should be replaced by its name.
      {ok, avro:get_type_fullname(Type)};
    false ->
      %% Unnamed types are always kept on their places
      false
  end.

%%%===================================================================
%%% Low level store access
%%%===================================================================

-spec put_type_to_store(fullname(), avro_type(), store()) -> store().
put_type_to_store(Name, Type, Store) ->
  true = ets:insert(Store, {Name, Type}),
  Json = iolist_to_binary(avro_json_encoder:encode_type(Type)),
  true = ets:insert(Store, {{json, Name}, Json}),
  Store.

get_type_from_store(Name, Store) ->
  case ets:lookup(Store, Name) of
    []             -> false;
    [{Name, Type}] -> {ok, Type}
  end.

get_type_json_from_store(Name, Store) ->
  case ets:lookup(Store, {json, Name}) of
    []                     -> false;
    [{{json, Name}, Json}] -> {ok, Json}
  end.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

sub_record() ->
  avro_record:type(
    "TestSubRecord",
    [ avro_record:define_field("sub_field1", avro_primitive:boolean_type())
    , avro_record:define_field("sub_field2",
                               avro_enum:type("MyEnum", ["A"],
                                              [{namespace, "another.name"}]))
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestSubRecordAlias"]}
    ]).

extracted_sub_record() ->
  avro_record:type(
    "TestSubRecord",
    [ avro_record:define_field(
        "sub_field1", avro_primitive:boolean_type())
    , avro_record:define_field(
        "sub_field2", "another.name.MyEnum")
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestSubRecordAlias"]}
    ]).

test_record() ->
  avro_record:type(
    "TestRecord",
    [ %% simple type
      avro_record:define_field("field1", avro_primitive:int_type())
      %% huge nested type
    , avro_record:define_field(
        "field2",
        avro_array:type(
          avro_union:type(
            [ avro_primitive:string_type()
            , sub_record()
            , avro_fixed:type("MyFixed", 16,
                              [{namespace, "com.klarna.test.bix"}])
            ])))
      %% named type without explicit namespace
    , avro_record:define_field("field3", "com.klarna.test.bix.SomeType")
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestRecordAlias1", "TestRecordAlias2"]}
    ]
   ).

extracted_test_record() ->
  avro_record:type(
    "TestRecord",
    [ %% simple type
      avro_record:define_field(
        "field1", avro_primitive:int_type())
      %% huge nested type
    , avro_record:define_field(
        "field2", avro_array:type(
                    avro_union:type(
                      [ avro_primitive:string_type()
                      , "com.klarna.test.bix.TestSubRecord"
                      , "com.klarna.test.bix.MyFixed"
                      ])))
      %% named type without explicit namespace
    , avro_record:define_field(
        "field3", "com.klarna.test.bix.SomeType")
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestRecordAlias1", "TestRecordAlias2"]}
    ]).

extract_from_primitive_type_test() ->
  Type = avro_primitive:int_type(),
  ?assertEqual({Type, []}, extract_children_types(Type)).

extract_from_nested_primitive_type_test() ->
  Type = avro_array:type(avro_primitive:int_type()),
  ?assertEqual({Type, []}, extract_children_types(Type)).

extract_from_named_type_test() ->
  Type = avro_array:type("com.klarna.test.bix.SomeType"),
  ?assertEqual({Type, []}, extract_children_types(Type)).

extract_from_extractable_type_test() ->
  Type = avro_array:type(test_record()),
  Expected = { avro_array:type("com.klarna.test.bix.TestRecord")
             , [ extracted_test_record()
               , extracted_sub_record()
               , avro_enum:type("MyEnum", ["A"],
                                [{namespace, "another.name"}])
               , avro_fixed:type("MyFixed", 16,
                                 [{namespace, "com.klarna.test.bix"}])
               ]
             },
  ?assertEqual(Expected, extract_children_types(Type)).

add_type_test() ->
  Store = new(),
  Store1 = add_type(test_record(), Store),
  ?assertEqual({ok, extracted_test_record()},
               lookup_type("com.klarna.test.bix.TestRecord", Store1)),
  ?assertEqual({ok, extracted_test_record()},
               lookup_type("com.klarna.test.bix.TestRecordAlias1", Store1)),
  ?assertEqual({ok, extracted_sub_record()},
               lookup_type("com.klarna.test.bix.TestSubRecord", Store1)),
  ?assertEqual({ok, extracted_sub_record()},
               lookup_type("com.klarna.test.bix.TestSubRecordAlias", Store1)).

import_test() ->
  PrivDir = priv_dir(),
  AvscFile = filename:join([PrivDir, "interop.avsc"]),
  Store = new([], [AvscFile]),
  ets:delete(Store),
  ok.

priv_dir() ->
  case filelib:is_dir(filename:join(["..", priv])) of
    true -> filename:join(["..", priv]);
    _    -> "./priv"
  end.

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
