%%%-----------------------------------------------------------------------------
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
        , get_all_types/1
        , ensure_store/1
        ]).

-include("avro_internal.hrl").

-opaque store() :: ets:tab().
-type option_key() :: access | name.
-type filename() :: file:filename_all().

-export_type([store/0]).

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
      Name1     -> {Name1, [named_table]}
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
import_files(Files, Store) ->
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
import_file(File, Store) ->
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
import_schema_json(Json, Store) ->
  import_schema_json(undefined, Json, Store).

%% @doc Delete the ets table.
-spec close(store()) -> ok.
close(Store) ->
  ets:delete(Store),
  ok.

%% @doc To make dialyzer happy.
-spec ensure_store(atom()) -> store().
ensure_store(A) ->
  true = is_atom(A),
  A.

%% @doc Add named type into the schema store.
%% NOTE: the type is flattened before inserting into the schema store.
%% i.e. named types nested in the given type are lifted up to root level.
%% @end
-spec add_type(avro_type(), store()) -> store().
add_type(Type, Store) ->
  add_type(undefined, Type, Store).

%% @doc Add (maybe unnamed) type to schema store.
%% If the type is unnamed, the assigned name is used.
%% For named types, the assigned name works like an alias.
%% @end
-spec add_type(undefined | name_raw(), avro_type(), store()) -> store().
add_type(AssignedName, Type0, Store) ->
  {Type, FlattenTypes} = avro:flatten_type(Type0),
  %% Exception when the root type is not named but assigned name is not given.
  case ?IS_TYPE_RECORD(Type) andalso AssignedName =:= undefined of
    true  -> erlang:error({unnamed_type, Type});
    false -> ok
  end,
  %% Add the root type with assigned name.
  %% Even when the flattened result is a name reference.
  Store1 = add_by_assigned_name(AssignedName, Type, Store),
  lists:foldl(fun do_add_type/2, Store1, FlattenTypes).

%% @doc Lookup a type using its full name.
-spec lookup_type(name_raw(), store()) -> {ok, avro_type()} | false.
lookup_type(FullName, Store) ->
  get_type_from_store(?NAME(FullName), Store).

%% @doc Fold schema
-spec get_all_types(store()) -> [avro_type()].
get_all_types(Store) ->
  [Type || {_Name, Type} <- ets:tab2list(Store), ?IS_TYPE_RECORD(Type)].

%%%_* Internal Functions =======================================================

%% @private Add type by an assigned name.
%% Except when assigned name is 'undefined'
%% @end
-spec add_by_assigned_name(undefined | name_raw(),
                           type_or_name(), store()) -> store().
add_by_assigned_name(undefined, _Type, Store) -> Store;
add_by_assigned_name(AssignedName, Type, Store) ->
  do_add_type_by_names([?NAME(AssignedName)], Type, Store).

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
