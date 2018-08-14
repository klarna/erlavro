%%%-----------------------------------------------------------------------------
%%%
%%% Copyright (c) 2013-2018 Klarna AB
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
%%% @doc General Avro handling code.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro).

-export([ decode_schema/1
        , decode_schema/2
        , encode_schema/1
        , encode_schema/2
        , expand_type/2
        , expand_type_bloated/2
        , flatten_type/1
        , get_aliases/1
        , get_custom_props/2
        , get_custom_props/1
        , get_type_name/1
        , get_type_namespace/1
        , get_type_fullname/1
        , is_compatible/2
        , is_named_type/1
        , is_same_type/2
        , make_decoder/2
        , make_encoder/2
        , make_simple_decoder/2
        , make_simple_encoder/2
        , make_lkup_fun/1
        , make_lkup_fun/2
        , resolve_fullname/2
        ]).

-export([ build_type_fullname/2
        , name2type/1
        , split_type_name/2
        ]).

-export([ cast/2
        , to_term/1
        ]).

-export([ decode/5
        , encode/4
        , encode_wrapped/4
        ]).

-export([ crc64_fingerprint/1
        , canonical_form_fingerprint/1
        ]).

-export_type([ array_type/0
             , avro_type/0
             , avro_value/0
             , canonicalized_value/0
             , codec_options/0
             , crc64_fingerprint/0
             , custom_prop/0
             , custom_prop_name/0
             , custom_prop_value/0
             , decode_fun/0
             , decoder_hook_fun/0
             , encode_fun/0
             , encoding/0
             , enum_index/0
             , enum_symbol/0
             , enum_symbol_raw/0
             , enum_type/0
             , fixed_type/0
             , fullname/0
             , in/0
             , lkup_fun/0
             , name/0
             , name_raw/0
             , namespace/0
             , namespace_raw/0
             , ordering/0
             , out/0
             , primitive_type/0
             , record_field/0
             , record_type/0
             , simple_decoder/0
             , simple_encoder/0
             , schema_all/0
             , schema_opts/0
             , schema_store/0
             , typedoc/0
             , type_prop_name/0
             , type_prop_value/0
             , type_props/0
             , type_or_name/0
             , union_index/0
             , union_type/0
             ]).

-include("avro_internal.hrl").

-type in() :: null
            | boolean()
            | integer()
            | float()
            | binary()
            | iolist()
            | [avro:in()]
            | [{name_raw(), avro:in()}].

-type out() :: null
             | boolean()
             | integer()
             | float()
             | binary()
             | [avro:out()]
             | [{name(), avro:out()}].

-type codec_options() :: [proplists:property()].
-type encode_fun() :: fun((type_or_name(), in()) -> iodata() | avro_value()).
-type simple_encoder() :: fun((in()) -> iodata()).
-type decode_fun() :: fun((type_or_name(), binary()) -> out()).
-type simple_decoder() :: fun((binary()) -> out()).
-type encoding() :: avro_encoding().
-type schema_opts() :: proplists:proplist().
-type schema_all() :: avro_type() | binary() | lkup_fun() | schema_store().
-type crc64_fingerprint() :: avro_fingerprint:crc64().

%% @doc Decode JSON format avro schema into `erlavro' internals.
-spec decode_schema(binary()) -> avro_type().
decode_schema(JSON) -> avro_json_decoder:decode_schema(JSON).

%% @doc Make type lookup function from type definition.
%% The given type is flattened then the flat types are keyed
%% by their fullnames and aliases.
%% @end
-spec make_lkup_fun(avro_type()) -> lkup_fun().
make_lkup_fun(Type) ->
  make_lkup_fun(?ASSIGNED_NAME, Type).

%% @doc Make type lookup function. The root type is also
%% keyed by the give assigned-name.
%% @end
-spec make_lkup_fun(name_raw(), avro_type()) -> lkup_fun().
make_lkup_fun(AssignedName, Type) ->
  avro_util:make_lkup_fun(AssignedName, Type).

%% @doc Decode JSON format avro schema into `erlavro' internals
%% Supported options:
%% * ignore_bad_default_values: `boolean()'
%%     Some library may produce invalid default values,
%%     if this option is set, bad default valus will be whatever values
%%     obtained from JSON decoder.
%%     However, the encoder built from this schema may crash in case bad default
%%     value is used (e.g. when a record field is missing from encoder input)
%% * allow_bad_references: `boolean()'
%%     This option is to allow referencing to a non-existing type.
%%     So types can be defined in multiple JSON schema files and all imported
%%     to schema store to construct a valid over-all schema.
%%  * allow_type_redefine: `boolean()'
%%     This option is to allow one type being defined more than once.
%% @end
-spec decode_schema(binary(), proplists:proplist()) -> avro_type().
decode_schema(JSON, Options) ->
  avro_json_decoder:decode_schema(JSON, Options).

%% @doc Encode `erlavro' internals type records into JSON format.
-spec encode_schema(avro_type()) -> binary().
encode_schema(Type) ->
  avro_json_encoder:encode_schema(Type).

%% @doc Encode `erlavro' internals type records into JSON format.
%% Supported options:
%% * canon: `boolean()'
%%     Encode parsing canonical form format
-spec encode_schema(avro_type(), schema_opts()) -> binary().
encode_schema(Type, Options) ->
  avro_json_encoder:encode_schema(Type, Options).

%% @doc Make a encoder function.
%% Supported codec options:
%% * `{encoding, avro_binary | avro_json}', default = avro_binary
%%   To get a encoder function for JSON or binary encoding
%% * wrapped | `{wrapped, true}', default = false
%%   when 'wrapped' is not in the option list, or `{wrapped, false}' is given,
%%   return encoded iodata() without type info wrapped around.
%%   A wrapped result can be used as 'already encoded' part to inline a
%%   wrapper object.
-spec make_encoder(schema_all(), codec_options()) -> encode_fun().
make_encoder(Schema, Options) ->
  Lkup = avro_util:ensure_lkup_fun(Schema),
  Encoding = proplists:get_value(encoding, Options, avro_binary),
  IsWrapped = proplists:get_bool(wrapped, Options),
  case IsWrapped of
    true ->
      fun(TypeOrName, Value) ->
        ?MODULE:encode_wrapped(Lkup, TypeOrName, Value, Encoding)
      end;
    false ->
      fun(TypeOrName, Value) ->
        ?MODULE:encode(Lkup, TypeOrName, Value, Encoding)
      end
  end.

%% @doc Make a encoder function.
%% Supported codec options:
%% * `{encoding, avro_binary | avro_json}', default = avro_binary
%%   To get a encoder function for JSON or binary encoding
%% A simple container can only be built from self-contained full schema.
%% And unlike a regular encoder (from `make_encoder/2'), a simple encoder
%% takes only one `avro:in()' input arg, and does not support `wrapped' option.
-spec make_simple_encoder(binary() | avro_type(), codec_options()) ->
        simple_encoder().
make_simple_encoder(JSON, Options) when is_binary(JSON) ->
  make_simple_encoder(decode_schema(JSON), Options);
make_simple_encoder(Type, Options) when ?IS_TYPE_RECORD(Type) ->
  Lkup = make_lkup_fun(Type),
  Encoding = proplists:get_value(encoding, Options, avro_binary),
  fun(Value) -> ?MODULE:encode(Lkup, Type, Value, Encoding) end.

%% @doc Make a decoder function.
%% Supported codec options:
%% * `{encoding, avro_binary | avro_json}', default = avro_binary
%%   To get a decoder function for JSON or binary encoded data
%% * hook, default = ?DEFAULT_DECODER_HOOK
%%   The default hook is a dummy one (does nothing).
%%   see `avro_decoder_hooks.erl' for details and examples of decoder hooks.
-spec make_decoder(schema_all(), codec_options()) -> decode_fun().
make_decoder(Schema, Options) ->
  Lkup = avro_util:ensure_lkup_fun(Schema),
  Encoding = proplists:get_value(encoding, Options, avro_binary),
  Hook = proplists:get_value(hook, Options, ?DEFAULT_DECODER_HOOK),
  fun(TypeOrName, Bin) ->
    ?MODULE:decode(Encoding, Bin, TypeOrName, Lkup, Hook)
  end.

%% @doc Make a decoder function.
%% Supported codec options:
%% * `{encoding, avro_binary | avro_json}', default = avro_binary
%%   To get a decoder function for JSON or binary encoded data
%% * hook, default = ?DEFAULT_DECODER_HOOK
%%   The default hook is a dummy one (does nothing).
%%   see `avro_decoder_hooks.erl' for details and examples of decoder hooks.
%% A simple decoder can only be built from self-contained full schema.
%% And unlike a regular decoder (from `make_decoder/2'), a simple decoder
%% takes only one `binary()' input arg.
-spec make_simple_decoder(avro_type() | binary(), codec_options()) ->
        simple_decoder().
make_simple_decoder(JSON, Options) when is_binary(JSON) ->
  make_simple_decoder(decode_schema(JSON), Options);
make_simple_decoder(Type, Options) when ?IS_TYPE_RECORD(Type) ->
  Lkup = make_lkup_fun(Type),
  Encoding = proplists:get_value(encoding, Options, avro_binary),
  Hook = proplists:get_value(hook, Options, ?DEFAULT_DECODER_HOOK),
  fun(Bin) -> ?MODULE:decode(Encoding, Bin, Type, Lkup, Hook) end.

%% @doc Encode value to json or binary format.
-spec encode(schema_store() | lkup_fun(), type_or_name(),
             term(), avro_encoding()) -> iodata().
encode(StoreOrLkup, Type, Value, avro_json) ->
  avro_json_encoder:encode(StoreOrLkup, Type, Value);
encode(StoreOrLkup, Type, Value, avro_binary) ->
  avro_binary_encoder:encode(StoreOrLkup, Type, Value).

%% @doc Encode value and return the result wrapped with type info.
%% The result can be used as a 'trusted' part of a higher level
%% wrapper structure. e.g. encode a big array of some complex type
%% and use the result as a field value of a parent record
%% @end
-spec encode_wrapped(schema_all(), type_or_name(),
                     term(), avro_encoding()) -> avro_value().
encode_wrapped(S, TypeOrName, Value, Encoding) when not is_function(S) ->
  Lkup = avro_util:ensure_lkup_fun(S),
  encode_wrapped(Lkup, TypeOrName, Value, Encoding);
encode_wrapped(Lkup, Name, Value, Encoding) when ?IS_NAME_RAW(Name) ->
  encode_wrapped(Lkup, Lkup(?NAME(Name)), Value, Encoding);
encode_wrapped(Lkup, Type0, Value, Encoding) when ?IS_TYPE_RECORD(Type0) ->
  Encoded = iolist_to_binary(encode(Lkup, Type0, Value, Encoding)),
  Type = case is_named_type(Type0) of
           true  -> get_type_fullname(Type0);
           false -> Type0
         end,
  case Encoding of
    avro_json   -> ?AVRO_ENCODED_VALUE_JSON(Type, Encoded);
    avro_binary -> ?AVRO_ENCODED_VALUE_BINARY(Type, Encoded)
  end.

%% @doc Decode value return unwarpped values.
-spec decode(avro_encoding(),
             Data :: binary(),
             type_or_name(),
             schema_store() | lkup_fun(),
             decoder_hook_fun()) -> term().
decode(avro_json, JSON, TypeOrName, StoreOrLkup, Hook) ->
  avro_json_decoder:decode_value(JSON, TypeOrName, StoreOrLkup,
                                 [{is_wrapped, false}], Hook);
decode(avro_binary, Bin, TypeOrName, StoreOrLkup, Hook) ->
  avro_binary_decoder:decode(Bin, TypeOrName, StoreOrLkup, Hook).

%% @doc Recursively resolve children type's fullname with enclosing
%% namespace passed down from ancestor types.
%% @end
-spec resolve_fullname(name() | avro_type(), namespace()) ->
        fullname() | avro_type().
resolve_fullname(Type, ?NS_GLOBAL) ->
  %% Do nothing if no enclosing namespace
  Type;
resolve_fullname(Name, Ns) when ?IS_NAME(Name) ->
  %% If it's a short name reference to another type
  %% make it a full name
  build_type_fullname(Name, Ns);
resolve_fullname(#avro_primitive_type{} = Type, _Ns) ->
  %% Primitive types has no full name
  Type;
resolve_fullname(#avro_array_type{} = Type, Ns) ->
  avro_array:resolve_fullname(Type, Ns);
resolve_fullname(#avro_enum_type{} = Type, Ns) ->
  avro_enum:resolve_fullname(Type, Ns);
resolve_fullname(#avro_fixed_type{} = Type, Ns) ->
  avro_fixed:resolve_fullname(Type, Ns);
resolve_fullname(#avro_map_type{} = Type, Ns) ->
  avro_map:resolve_fullname(Type, Ns);
resolve_fullname(#avro_record_type{} = Type, Ns) ->
  avro_record:resolve_fullname(Type, Ns);
resolve_fullname(#avro_union_type{} = Type, Ns) ->
  avro_union:resolve_fullname(Type, Ns).

%% @doc Create primitive type definiton from name.
%% In case the give name is not a primitive type name,
%% its canonicalized format is returned.
%% @end
-spec name2type(name_raw()) -> primitive_type() | name().
name2type(Name) ->
  try
    avro_primitive:type(Name, [])
  catch
    error : {unknown_name, _} ->
      ?NAME(Name)
  end.

%%%===================================================================
%%% API: Accessing types properties
%%%===================================================================

%% @doc Returns true if the type can have its own name defined in schema.
-spec is_named_type(avro_type()) -> boolean().
is_named_type(#avro_enum_type{})     -> true;
is_named_type(#avro_fixed_type{})    -> true;
is_named_type(#avro_record_type{})   -> true;
is_named_type(_)                     -> false.

%% @doc Returns the type's name. If the type is named then content of
%% its name field is returned which can be short name or full name,
%% depending on how the type was specified. If the type is unnamed
%% then Avro name of the type is returned.
%% @end
-spec get_type_name(avro_type()) -> name().
get_type_name(#avro_array_type{})                -> ?AVRO_ARRAY;
get_type_name(#avro_enum_type{name = Name})      -> Name;
get_type_name(#avro_fixed_type{name = Name})     -> Name;
get_type_name(#avro_map_type{})                  -> ?AVRO_MAP;
get_type_name(#avro_primitive_type{name = Name}) -> Name;
get_type_name(#avro_record_type{name = Name})    -> Name;
get_type_name(#avro_union_type{})                -> ?AVRO_UNION.

%% @doc Returns the type's namespace exactly as it is set in the type.
%% Depending on how the type was specified it could the namespace
%% or just an empty binary if the name contains namespace in it.
%% If the type can't have namespace then empty binary is returned.
%% @end
-spec get_type_namespace(avro_value() | avro_type()) -> namespace().
get_type_namespace(#avro_array_type{})                -> ?NS_GLOBAL;
get_type_namespace(#avro_enum_type{namespace = Ns})   -> Ns;
get_type_namespace(#avro_fixed_type{namespace = Ns})  -> Ns;
get_type_namespace(#avro_map_type{})                  -> ?NS_GLOBAL;
get_type_namespace(#avro_primitive_type{})            -> ?NS_GLOBAL;
get_type_namespace(#avro_record_type{namespace = Ns}) -> Ns;
get_type_namespace(#avro_union_type{})                -> ?NS_GLOBAL.

%% @doc Returns fullname stored inside the type.
%% For unnamed types their Avro name is returned.
%% @end
-spec get_type_fullname(type_or_name()) -> name() | fullname().
get_type_fullname(#avro_array_type{})                 -> ?AVRO_ARRAY;
get_type_fullname(#avro_enum_type{fullname = Name})   -> Name;
get_type_fullname(#avro_fixed_type{fullname = Name})  -> Name;
get_type_fullname(#avro_map_type{})                   -> ?AVRO_MAP;
get_type_fullname(#avro_primitive_type{name = Name})  -> Name;
get_type_fullname(#avro_record_type{fullname = Name}) -> Name;
get_type_fullname(#avro_union_type{})                 -> ?AVRO_UNION;
get_type_fullname(Name) when ?IS_NAME_RAW(Name)       -> ?NAME(Name).

%% @doc Returns aliases for the type.
%% Types without aliases defined are considered to have empty alias list.
%% All aliases have been canonicalized (as fullname).
%% @end
-spec get_aliases(avro_type()) -> [fullname()].
get_aliases(#avro_array_type{})                   -> [];
get_aliases(#avro_enum_type{aliases = Aliases})   -> Aliases;
get_aliases(#avro_fixed_type{aliases = Aliases})  -> Aliases;
get_aliases(#avro_map_type{})                     -> [];
get_aliases(#avro_primitive_type{})               -> [];
get_aliases(#avro_record_type{aliases = Aliases}) -> Aliases;
get_aliases(#avro_union_type{})                   -> [].

%% @doc Get custom type properties such as logical type info. Lookup fun
%% is called to retrieve the type definition from schema store in case it is
%% type name provided.
%% @end
-spec get_custom_props(type_or_name(), schema_all()) -> [custom_prop()].
get_custom_props(NameOrType, Store) when not is_function(Store) ->
  Lkup = avro_util:ensure_lkup_fun(Store),
  get_custom_props(NameOrType, Lkup);
get_custom_props(TypeName, Lkup) when ?IS_NAME_RAW(TypeName) ->
  get_custom_props(Lkup(?NAME(TypeName)), Lkup);
get_custom_props(Type, _Lkup) when ?IS_TYPE_RECORD(Type) ->
  get_custom_props(Type).

%% @doc Get custom type properties such as logical type info.
-spec get_custom_props(avro_type()) -> [custom_prop()].
get_custom_props(#avro_array_type{custom = C})     -> C;
get_custom_props(#avro_enum_type{custom = C})      -> C;
get_custom_props(#avro_fixed_type{custom = C})     -> C;
get_custom_props(#avro_map_type{custom = C})       -> C;
get_custom_props(#avro_primitive_type{custom = C}) -> C;
get_custom_props(#avro_record_type{custom = C})    -> C;
get_custom_props(#avro_union_type{})               -> [].

%% @equiv avro_util:flatten_type(Type)
-spec flatten_type(avro_type()) ->
        {avro_type() | fullname(), [avro_type()]} | none().
flatten_type(Type) -> avro_util:flatten_type(Type).

%% @equiv avro_util:expand_type(Type, Store, compact)
-spec expand_type(type_or_name(), schema_all()) ->
        avro_type() | none().
expand_type(Type, Sc) ->
  avro_util:expand_type(Type, Sc).

%% @equiv avro_util:expand_type(Type, Store, bloated)
-spec expand_type_bloated(type_or_name(), schema_all()) ->
        avro_type() | none().
expand_type_bloated(Type, Sc) ->
  avro_util:expand_type(Type, Sc, bloated).

%%%===================================================================
%%% API: Calculating of canonical short and full names of types
%%%===================================================================

%% @doc Splits type's name parts to its canonical short name and namespace.
-spec split_type_name(type_or_name(), name_raw()) -> {name(), namespace()}.
split_type_name(TypeName0, Namespace0) when ?IS_NAME_RAW(TypeName0) ->
  TypeName = ?NAME(TypeName0),
  Namespace = ?NAME(Namespace0),
  case split_fullname(TypeName) of
    {_, _} = N ->
      %% Not a short name
      N;
    false ->
      %% TypeName is a name without namespace
      {TypeName, Namespace}
  end;
split_type_name(Type, Namespace) ->
  split_type_name(get_type_fullname(Type), Namespace).

%% @doc Constructs the type's full name from provided name and namespace.
-spec build_type_fullname(name_raw(), namespace()) -> fullname().
build_type_fullname(TypeName, Namespace) when ?IS_NAME_RAW(TypeName) ->
  {ShortName, Ns} = split_type_name(TypeName, Namespace),
  make_fullname(ShortName, Ns).

%% @private Compare two types to check if they are the same.
%% The types can either be type records or type names.
%% @end
-spec is_same_type(type_or_name(), type_or_name()) -> boolean().
is_same_type(T1, T2) when ?IS_ARRAY_TYPE(T1) andalso
                          ?IS_ARRAY_TYPE(T2) ->
  %% For array, compare items type
  is_same_type(avro_array:get_items_type(T1),
               avro_array:get_items_type(T2));
is_same_type(T1, T2) when ?IS_MAP_TYPE(T1) andalso
                          ?IS_MAP_TYPE(T2) ->
  %% For map, compare items type
  is_same_type(avro_map:get_items_type(T1),
               avro_map:get_items_type(T2));
is_same_type(T1, T2) when ?IS_UNION_TYPE(T1) andalso
                          ?IS_UNION_TYPE(T2) ->
  %% For unions, compare the number of members and
  %% the order of the members
  Members1 = avro_union:get_types(T1),
  Members2 = avro_union:get_types(T2),
  length(Members1) =:= length(Members2) andalso
    lists:all(fun({Tt1, Tt2}) ->
                  is_same_type(Tt1, Tt2)
              end, lists:zip(Members1, Members2));
is_same_type(T1, T2) ->
  %% Named types and primitive types fall into this clause
  %% Should be enough to just compare their names
  get_type_fullname(T1) =:= get_type_fullname(T2).


%% @doc Check whether two schemas are compatible in sense that data
%% encoded with the writer schema can be decoded by the reader schema.
%% Schema compatibility is described here:
%% https://avro.apache.org/docs/1.8.1/spec.html#Schema+Resolution
%% An exception in this implementation is the comparison of two unions.
%% Check considers union member order changes to be incompatible. This
%% means that in order for a writer union to be compatible with the
%% reader union, the writer union must only append new members to it.
%% For example
%% is_compatible([int, string], [int, string]) = TRUE
%% is_compatible([string, int], [int, string]) = FALSE
%% is_compatible([string, int], [string]) = TRUE
%% is_compatible([string, int], [int]) = FALSE
%% is_compatible([int, string], [int, string, some_record]) = TRUE
-spec is_compatible(avro_type(), avro_type()) -> true | {not_compatible, _, _}.
is_compatible(ReaderSchema, WriterSchema) ->
  avro_util:is_compatible(ReaderSchema, WriterSchema).

%%%=============================================================================
%%% API: Checking correctness of names and types specifications
%%%=============================================================================

%% @doc Tries to cast a value (which can be another Avro value or some erlang
%% term) to the specified Avro type performing conversion if required.
%% @end
-spec cast(type_or_name(), avro:in()) ->
        {ok, avro_value()} | {error, term()}.
cast(T1, ?AVRO_VALUE(_, _) = V) -> cast_value(T1, V);
cast(Type, Value) when ?IS_TYPE_RECORD(Type) -> do_cast(Type, Value);
cast(Type, Value) when ?IS_NAME_RAW(Type) -> do_cast(name2type(Type), Value).

%% @doc Convert avro values to erlang term.
-spec to_term(avro_value()) -> out().
to_term(#avro_value{type = T} = V) -> to_term(T, V).


%%%=============================================================================
%%% API: Calculating Avro CRC 64 fingerprint
%%%=============================================================================

%% @doc Encode type into canonical form JSON schema
%% and return its crc64 fingerprint.
-spec canonical_form_fingerprint(avro_type()) -> crc64_fingerprint().
canonical_form_fingerprint(Type) ->
  JSON = encode_schema(Type, [{canon, true}]),
  crc64_fingerprint(JSON).

%% @doc Calculate hash using the Avro CRC 64 algorithm.
-spec crc64_fingerprint(binary()) -> crc64_fingerprint().
crc64_fingerprint(Bin) ->
    avro_fingerprint:crc64(Bin).

%%%_* Internal functions =======================================================

%% @private A specical case to cast wrapped value to union.
%% Other types, compare use is_same_type
%% @end
-spec cast_value(type_or_name(), avro_value()) ->
        {ok, avro_value()} | {error, any()}.
cast_value(T1, ?AVRO_VALUE(T2, _) = V) when ?IS_UNION_TYPE(T1) andalso
                                            not ?IS_UNION_TYPE(T2) ->
  %% Union, the value can be a member
  MemberTypeName = get_type_fullname(T2),
  %% Tag the value, delegate to avro_union
  avro_union:cast(T1, {MemberTypeName, V});
cast_value(T1, ?AVRO_VALUE(T2, _) = V) ->
  case is_same_type(T1, T2) of
    true  -> {ok, V};
    false -> {error, {type_mismatch, T1, T2}}
  end.

%% @private
-spec do_cast(avro_type(), avro:in()) -> {ok, avro_value()} | {error, any()}.
do_cast(#avro_primitive_type{} = T, V) -> avro_primitive:cast(T, V);
do_cast(#avro_record_type{} = T,    V) -> avro_record:cast(T, V);
do_cast(#avro_enum_type{} = T,      V) -> avro_enum:cast(T, V);
do_cast(#avro_array_type{} = T,     V) -> avro_array:cast(T, V);
do_cast(#avro_map_type{} = T,       V) -> avro_map:cast(T, V);
do_cast(#avro_union_type{} = T,     V) -> avro_union:cast(T, V);
do_cast(#avro_fixed_type{} = T,     V) -> avro_fixed:cast(T, V).

%% @private Splits FullName to {Name, Namespace} or returns false
%% if the given name is a short name (no dots).
%% @end
-spec split_fullname(fullname()) -> false | {name(), namespace()}.
split_fullname(FullNameBin) when is_binary(FullNameBin) ->
  %% Avro names are always ascii chars, no need for utf8 caring.
  FullName = binary_to_list(FullNameBin),
  case string:rchr(FullName, $.) of
    0 ->
      %% Dot not found
      false;
    DotPos ->
      { ?NAME(string:substr(FullName, DotPos + 1))
      , ?NAME(string:substr(FullName, 1, DotPos-1))
      }
  end.

%% @private
-spec to_term(type_or_name(), out() | avro_value()) -> out().
to_term(#avro_primitive_type{}, V) -> avro_primitive:get_value(V);
to_term(#avro_record_type{}, V)    -> avro_record:to_term(V);
to_term(#avro_enum_type{}, V)      -> avro_enum:get_value(V);
to_term(#avro_array_type{}, V)     -> avro_array:to_term(V);
to_term(#avro_map_type{}, V)       -> avro_map:to_term(V);
to_term(#avro_union_type{}, V)     -> avro_union:to_term(V);
to_term(#avro_fixed_type{}, V)     -> avro_fixed:get_value(V).

%% @private
-spec make_fullname(name_raw(), namespace_raw()) -> name().
make_fullname(Name, ?NS_GLOBAL) -> Name;
make_fullname(Name, Namespace) ->
  iolist_to_binary([?NAME(Namespace), ".", ?NAME(Name)]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
