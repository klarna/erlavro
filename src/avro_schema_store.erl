%%%-----------------------------------------------------------------------------
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
%%%-----------------------------------------------------------------------------

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
        , import_schema_json/3
        ]).

%% Add/Lookup
-export([ add_type/2
        , add_type/3
        , lookup_type/2
        , to_lookup_fun/1
        ]).

%% Flatten/Expand
-export([ flatten_type/1
        , expand_type/2
        ]).

-include("avro_internal.hrl").

-ifdef(TEST).
-export([extract_children_types/1]).
-endif.

-opaque store() :: ets:tab().
-type option_key() :: access | name.
-type filename() :: file:filename_all().

-export_type([store/0]).

-define(IS_STORE(S), (is_integer(S) orelse is_atom(S))).
-define(SEEN, avro_get_nested_type_seen_full_names).

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
-spec to_lookup_fun(store()) -> fun((name_raw()) -> avro_type()).
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
%% In case the schema is unnamed, the file basename is used as its
%% lookup name.
%% Extention ".avsc" or ".json" will be stripped,
%% Otherwise the full file basename is used.
%% e.g.
%%  "/path/to/com.klarna.test.x.avsc" to 'com.klarna.etst.x"
%%  "/path/to/com.klarna.test.x.json" to 'com.klarna.etst.x"
%%  "/path/to/com.klarna.test.x"      to 'com.klarna.etst.x"
%% @end
-spec import_file(filename(), store()) -> store().
import_file(File, Store) when ?IS_STORE(Store) ->
  case file:read_file(File) of
    {ok, Json} ->
      Name = parse_basename(File),
      import_schema_json(Name, Json, Store);
    {error, Reason} ->
      erlang:error({failed_to_read_schema_file, File, Reason})
  end.

%% @doc Decode avro schema JSON into erlavro records.
%% NOTE: Exception if the type is unnamed.
%% @end
-spec import_schema_json(binary(), store()) -> store().
import_schema_json(Json, Store) when ?IS_STORE(Store) ->
  import_schema_json(undefined, Json, Store).

%% @doc Delete the ets table.
-spec close(store()) -> ok.
close(Store) ->
  ets:delete(Store),
  ok.

%% @doc Add named type into the schema store.
%% NOTE: the type is flattened before inserting into the schema store.
%% i.e. named types nested in the given type are lifted up to root level.
%% @end
-spec add_type(avro_type(), store()) -> store().
add_type(Type, Store) when ?IS_STORE(Store) ->
  add_type(undefined, Type, Store).

%% @doc Add (maybe unnamed) type to schema store.
%% If the type is unnamed, the assigned name is used.
%% @end
-spec add_type(undefined | name_raw(), avro_type(), store()) -> store().
add_type(AssignedName, Type, Store) when ?IS_STORE(Store) ->
  {ConvertedType, ExtractedTypes} = extract_children_types(Type),
  case avro:is_named_type(Type) of
    true ->
      lists:foldl(fun do_add_type/2, Store,
                  [ConvertedType | ExtractedTypes]);
    false when AssignedName =/= undefined ->
      Store1 = do_add_type_by_names([?NAME(AssignedName)], Type, Store),
      lists:foldl(fun do_add_type/2, Store1, ExtractedTypes);
    false when AssignedName =:= undefined ->
      erlang:error({unnamed_type, Type})
  end.

%% @doc Lookup a type using its full name.
-spec lookup_type(name_raw(), store()) -> {ok, avro_type()} | false.
lookup_type(FullName, Store) when ?IS_STORE(Store) ->
  get_type_from_store(?NAME(FullName), Store).

%% @doc Get type and lookup sub-types recursively.
%% This should allow callers to write a root type to one avsc schema file
%% instead of scattering all named types to their own avsc files.
%% @end
-spec expand_type(avro_type_or_name(), store()) -> avro_type() | no_return().
expand_type(Type0, Store) when ?IS_STORE(Store) ->
  Type = avro_util:canonicalize_type_or_name(Type0),
  try
    %% Use process dictionary to keep the history of seen type names
    %% to simplify (comparing to a lists:foldr version) the loop functions.
    %%
    %% The ?SEEN names are 'previously' already resolved full names of types
    %% when traversing the type tree, so there is no need to resolve
    %% the 'current' type reference again.
    %%
    %% Otherwise the encoded JSON schema would be bloated with repeated
    %% type definitions.
    _ = erlang:put(?SEEN, []),
    case ?IS_NAME(Type) of
      true  -> do_expand_type(Type, Store);
      false -> expand(Type, Store)
    end
  after
    erlang:erase(?SEEN)
  end.

%% @doc Flatten out all named types, return the extracted format and all
%% (recursively extracted) children types in a list.
%% If the type is named (i.e. record type), the extracted format is its
%% full name, and the type itself is added to the extracted list.
%% @end
-spec flatten_type(name_raw() | avro_type()) ->
        {avro_type_or_name(), [avro_type()]}.
flatten_type(TypeName) when ?IS_NAME_RAW(TypeName) ->
  %% it's a reference, do nothing
  {?NAME(TypeName), []};
flatten_type(Type) when ?IS_AVRO_TYPE(Type) ->
  %% go deeper
  {NewType, Extracted} = extract_children_types(Type),
  case avro:is_named_type(NewType) of
    true  ->
      %% Named types are replaced by their fullnames.
      Fullname = avro:get_type_fullname(Type),
      {Fullname, [NewType | Extracted]};
    false ->
      {NewType, Extracted}
  end.

%%%_* Internal Functions =======================================================

%% @private Parse file basename. try to strip ".avsc" or ".json" extension.
-spec parse_basename(filename()) -> name().
parse_basename(FileName) ->
  BaseName0 = filename:basename(FileName),
  BaseName1 = filename:basename(FileName, ".avsc"),
  BaseName2 = filename:basename(FileName, ".json"),
  lists:foldl(
    fun(N, Shortest) ->
        BN = avro_util:ensure_binary(N),
        case size(BN) < size(Shortest) of
          true  -> BN;
          false -> Shortest
        end
    end, avro_util:ensure_binary(BaseName0), [BaseName1, BaseName2]).

%% @private Import JSON schema with assigned name.
-spec import_schema_json(name_raw(), binary(), store()) -> store().
import_schema_json(AssignedName, Json, Store) ->
  Schema = avro_json_decoder:decode_schema(Json),
  add_type(AssignedName, Schema, Store).

%% @private
-spec do_expand_type(fullname(), store()) ->
        fullname() | avro_type() | no_return().
do_expand_type(Fullname, Store) when ?IS_NAME(Fullname) ->
  Hist = erlang:get(?SEEN),
  case lists:member(Fullname, Hist) of
    true ->
      %% This type name has been resolved earlier
      %% do not go deeper for 2 reasons:
      %% 1. There is no need to duplicate the type definitions
      %% 2. To avoid stack overflow in case of recursive reference
      Fullname;
    false ->
      {ok, T} = lookup_type(Fullname, Store),
      erlang:put(?SEEN, [Fullname | Hist]),
      expand(T, Store)
  end.

%% @private
-spec expand(avro_type(), store()) -> avro_type() | no_return().
expand(#avro_record_type{fields = Fields} = T, Store) ->
  ResolvedFields =
    lists:map(
      fun(#avro_record_field{type = Type} = F) ->
        ResolvedType = expand(Type, Store),
        F#avro_record_field{type = ResolvedType}
      end, Fields),
  T#avro_record_type{fields = ResolvedFields};
expand(#avro_array_type{type = SubType} = T, Store) ->
  ResolvedSubType = expand(SubType, Store),
  T#avro_array_type{type = ResolvedSubType};
expand(#avro_map_type{type = SubType} = T, Store) ->
  ResolvedSubType = expand(SubType, Store),
  T#avro_map_type{type = ResolvedSubType};
expand(#avro_union_type{types = SubTypes} = T, Store) ->
  ResolvedSubTypes =
    lists:map(
      fun({Index, SubType}) ->
        ResolvedSubType = expand(SubType, Store),
        {Index, ResolvedSubType}
      end, SubTypes),
  T#avro_union_type{types = ResolvedSubTypes};
expand(Fullname, Store) when ?IS_NAME(Fullname) ->
  do_expand_type(Fullname, Store);
expand(T, _Store) when ?IS_AVRO_TYPE(T) ->
  T.

%% @private
-spec do_add_type(avro_type(), store()) -> store().
do_add_type(Type, Store) ->
  FullName = avro:get_type_fullname(Type),
  Aliases = avro:get_aliases(Type),
  do_add_type_by_names([FullName|Aliases], Type, Store).

%% @private
-spec do_add_type_by_names([fullname()], avro_type(), store()) ->
        store() | no_return().
do_add_type_by_names([], _Type, Store) -> Store;
do_add_type_by_names([Name|Rest], Type, Store) ->
  case get_type_from_store(Name, Store) of
    {ok, Type} ->
      Store;
    {ok, OtherType} ->
      %% Name can be an assigned name for unnamed types,
      %% This is why we raise error exception with name AND both
      %% old / new types.
      erlang:error({name_clash, Name, Type, OtherType});
    false   ->
      Store1 = put_type_to_store(Name, Type, Store),
      do_add_type_by_names(Rest, Type, Store1)
  end.

%% @private Recursively extract all children types from the type
%% replace extracted types with their full names as references.
%% @end
-spec extract_children_types(avro_type()) -> {avro_type(), [avro_type()]}.
extract_children_types(Primitive) when ?AVRO_IS_PRIMITIVE_TYPE(Primitive) ->
  {Primitive, []};
extract_children_types(Enum) when ?AVRO_IS_ENUM_TYPE(Enum) ->
  {Enum, []};
extract_children_types(Fixed) when ?AVRO_IS_FIXED_TYPE(Fixed) ->
  {Fixed, []};
extract_children_types(Record) when ?AVRO_IS_RECORD_TYPE(Record) ->
  {NewFields, ExtractedTypes} =
    lists:foldr(
      fun(Field, {FieldsAcc, ExtractedAcc}) ->
          {NewType, Extracted} =
            flatten_type(Field#avro_record_field.type),
          {[Field#avro_record_field{type = NewType}|FieldsAcc],
           Extracted ++ ExtractedAcc}
      end,
      {[], []},
      Record#avro_record_type.fields),
  {Record#avro_record_type{ fields = NewFields }, ExtractedTypes};
extract_children_types(Array) when ?AVRO_IS_ARRAY_TYPE(Array) ->
  ChildType = avro_array:get_items_type(Array),
  {NewChildType, Extracted} = flatten_type(ChildType),
  {avro_array:type(NewChildType), Extracted};
extract_children_types(Map) when ?AVRO_IS_MAP_TYPE(Map) ->
  ChildType = Map#avro_map_type.type,
  {NewChildType, Extracted} = flatten_type(ChildType),
  {Map#avro_map_type{type = NewChildType}, Extracted};
extract_children_types(Union) when ?AVRO_IS_UNION_TYPE(Union) ->
  ChildrenTypes = avro_union:get_types(Union),
  {NewChildren, ExtractedTypes} =
    lists:foldr(
      fun(ChildType, {FlattenAcc, ExtractedAcc}) ->
          {ChildType1, Extracted} = flatten_type(ChildType),
          {[ChildType1|FlattenAcc], Extracted ++ ExtractedAcc}
      end,
      {[], []},
      ChildrenTypes),
  {avro_union:type(NewChildren), ExtractedTypes}.

%%%===================================================================
%%% Low level store access
%%%===================================================================

%% @private
-spec put_type_to_store(fullname(), avro_type(), store()) -> store().
put_type_to_store(Name, Type, Store) ->
  true = ets:insert(Store, {Name, Type}),
  Store.

%% @private
-spec get_type_from_store(fullname(), store()) -> false | {ok, avro_type()}.
get_type_from_store(Name, Store) ->
  case ets:lookup(Store, Name) of
    []             -> false;
    [{Name, Type}] -> {ok, Type}
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
