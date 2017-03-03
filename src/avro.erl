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
%%% @doc General Avro handling code.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro).

-export([ expand_type/2
        , flatten_type/1
        , get_type_name/1
        , get_type_namespace/1
        , get_type_fullname/1
        , get_aliases/1
        , is_named_type/1
        , make_decoder/2
        , make_encoder/2
        , resolve_fullname/2
        ]).

-export([ split_fullname/1
        , split_type_name/2
        , build_type_fullname/2
        ]).

-export([ read_schema/1
        ]).

-export([cast/2]).

-export([to_term/1]).

-export([ decode/5
        , encode/4
        , encode_wrapped/4
        ]).

-export_type([ avro_type/0
             , avro_value/0
             , codec_options/0
             , canonicalize_primitive_value/0
             , decode_fun/0
             , encode_fun/0
             , enum_symbol/0
             , enum_symbol_raw/0
             , enum_index/0
             , fullname/0
             , in/0
             , name/0
             , name_raw/0
             , namespace/0
             , namespace_raw/0
             , out/0
             , typedoc/0
             , type_prop_name/0
             , type_prop_value/0
             , type_props/0
             , union_index/0
             ]).

-include("avro_internal.hrl").

-type in() :: null
            | boolean()
            | integer()
            | float()
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
-type encode_fun() ::
        fun((avro_type_or_name(), term()) -> iodata() | avro_value()).
-type decode_fun() ::
        fun((avro_type_or_name(), binary()) -> term()).

%% @doc Make a encoder function.
%% Supported codec options:
%% * {encoding, avro_binary | avro_json}, default = avro_binary
%%   To get a encoder function for JSON or binary encoding
%% * wrapped | {wrapped, true}, default = false
%%   when 'wrapped' is not in the option list, or {wrapped, false} is given,
%%   return encoded iodata() without type info wrapped around.
%% @end
-spec make_encoder(schema_store() | lkup_fun(), codec_options()) ->
        encode_fun().
make_encoder(StoreOrLkupFun, Options) ->
  Encoding = proplists:get_value(encoding, Options, avro_binary),
  IsWrapped = proplists:get_bool(wrapped, Options),
  case IsWrapped of
    true ->
      fun(TypeOrName, Value) ->
        ?MODULE:encode_wrapped(StoreOrLkupFun, TypeOrName, Value, Encoding)
      end;
    false ->
      fun(TypeOrName, Value) ->
        ?MODULE:encode(StoreOrLkupFun, TypeOrName, Value, Encoding)
      end
  end.

%% @doc Make a decoder function.
%% Supported codec options:
%% * {encoding, avro_binary | avro_json}, default = avro_binary
%%   To get a decoder function for JSON or binary encoded data
%% * hook, default = ?DEFAULT_DECODER_HOOK
%%   The default hook is a dummy one (does nothing).
%%   see `avro_decoder_hooks.erl' for details and examples of decoder hooks.
%% @end
-spec make_decoder(schema_store() | lkup_fun(), codec_options()) ->
        decode_fun().
make_decoder(StoreOrLkupFun, Options) ->
  Encoding = proplists:get_value(encoding, Options, avro_binary),
  Hook = proplists:get_value(hook, Options, ?DEFAULT_DECODER_HOOK),
  fun(TypeOrName, Bin) ->
    ?MODULE:decode(Encoding, Bin, TypeOrName, StoreOrLkupFun, Hook)
  end.

%% @doc Encode value to json or binary format.
-spec encode(schema_store() | lkup_fun(), avro_type_or_name(),
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
-spec encode_wrapped(schema_store() | lkup_fun(), avro_type_or_name(),
                     term(), avro_encoding()) -> avro_value().
encode_wrapped(S, TypeOrName, Value, Encoding) when not is_function(S) ->
  Lkup = ?AVRO_SCHEMA_LOOKUP_FUN(S),
  encode_wrapped(Lkup, TypeOrName, Value, Encoding);
encode_wrapped(Lkup, Name, Value, Encoding) when ?IS_NAME(Name) ->
  Type = Lkup(Name),
  encode_wrapped(Lkup, Type, Value, Encoding);
encode_wrapped(Lkup, Type, Value, Encoding) ->
  Encoded = iolist_to_binary(encode(Lkup, Type, Value, Encoding)),
  case Encoding of
    avro_json   -> ?AVRO_ENCODED_VALUE_JSON(Type, Encoded);
    avro_binary -> ?AVRO_ENCODED_VALUE_BINARY(Type, Encoded)
  end.

%% @doc Decode value return unwarpped values.
-spec decode(avro_encoding(),
             Data :: binary(),
             avro_type_or_name(),
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

%%%===================================================================
%%% API: Accessing types properties
%%%===================================================================

%% @doc Returns true if the type can have its own name defined in schema.
-spec is_named_type(avro_type()) -> boolean().
is_named_type(#avro_record_type{})   -> true;
is_named_type(#avro_enum_type{})     -> true;
is_named_type(#avro_fixed_type{})    -> true;
is_named_type(_)                     -> false.

%% @doc Returns the type's name. If the type is named then content of
%% its name field is returned which can be short name or full name,
%% depending on how the type was specified. If the type is unnamed
%% then Avro name of the type is returned.
%% @end
-spec get_type_name(avro_type()) -> name().
get_type_name(#avro_primitive_type{name = Name}) -> Name;
get_type_name(#avro_record_type{name = Name})    -> Name;
get_type_name(#avro_enum_type{name = Name})      -> Name;
get_type_name(#avro_array_type{})                -> ?AVRO_ARRAY;
get_type_name(#avro_map_type{})                  -> ?AVRO_MAP;
get_type_name(#avro_union_type{})                -> ?AVRO_UNION;
get_type_name(#avro_fixed_type{name = Name})     -> Name.

%% @doc Returns the type's namespace exactly as it is set in the type.
%% Depending on how the type was specified it could the namespace
%% or just an empty binary if the name contains namespace in it.
%% If the type can't have namespace then empty binary is returned.
%% @end
-spec get_type_namespace(avro_value() | avro_type()) -> namespace().
get_type_namespace(#avro_primitive_type{})            -> ?NS_GLOBAL;
get_type_namespace(#avro_record_type{namespace = Ns}) -> Ns;
get_type_namespace(#avro_enum_type{namespace = Ns})   -> Ns;
get_type_namespace(#avro_array_type{})                -> ?NS_GLOBAL;
get_type_namespace(#avro_map_type{})                  -> ?NS_GLOBAL;
get_type_namespace(#avro_union_type{})                -> ?NS_GLOBAL;
get_type_namespace(#avro_fixed_type{namespace = Ns})  -> Ns.

%% @doc Returns fullname stored inside the type.
%% For unnamed types their Avro name is returned.
%% @end
-spec get_type_fullname(avro_type()) -> name() | fullname().
get_type_fullname(#avro_primitive_type{name = Name})  -> Name;
get_type_fullname(#avro_record_type{fullname = Name}) -> Name;
get_type_fullname(#avro_enum_type{fullname = Name})   -> Name;
get_type_fullname(#avro_array_type{})                 -> ?AVRO_ARRAY;
get_type_fullname(#avro_map_type{})                   -> ?AVRO_MAP;
get_type_fullname(#avro_union_type{})                 -> ?AVRO_UNION;
get_type_fullname(#avro_fixed_type{fullname = Name})  -> Name.

%% @doc Returns aliases for the type.
%% Types without aliases defined are considered to have empty alias list.
%% All aliases have been canonicalized (as fullname).
%% @end
-spec get_aliases(avro_type()) -> [fullname()].
get_aliases(#avro_primitive_type{})               -> [];
get_aliases(#avro_record_type{aliases = Aliases}) -> Aliases;
get_aliases(#avro_enum_type{aliases = Aliases})   -> Aliases;
get_aliases(#avro_array_type{})                   -> [];
get_aliases(#avro_map_type{})                     -> [];
get_aliases(#avro_union_type{})                   -> [];
get_aliases(#avro_fixed_type{aliases = Aliases})  -> Aliases.

%% @see avro_schema_store:flatten_type/1
-spec flatten_type(avro_type()) ->
        {avro_type() | fullname(), [avro_type()]} | none().
flatten_type(Type) ->
  avro_schema_store:flatten_type(Type).

%% @see avro_schema_store:expand_type/2
-spec expand_type(fullname() | avro_type(), schema_store()) ->
        avro_type() | none().
expand_type(Type, Store) ->
  avro_schema_store:expand_type(Type, Store).

%%%===================================================================
%%% API: Reading schema from json file
%%%===================================================================

-spec read_schema(file:filename()) -> {ok, avro_type()} | {error, any()}.
read_schema(File) ->
  case file:read_file(File) of
    {ok, Data} ->
      {ok, avro_json_decoder:decode_schema(Data)};
    Error ->
      Error
  end.

%%%===================================================================
%%% API: Calculating of canonical short and full names of types
%%%===================================================================

%% @doc Splits type's name parts to its canonical short name and namespace.
-spec split_type_name(name_raw(), name_raw()) -> {name(), namespace()}.
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

%% @doc Splits FullName to {Name, Namespace} or returns false
%% if FullName is not a full name.
%% @end
-spec split_fullname(fullname()) -> {name(), namespace()}.
split_fullname(FullNameBin) when is_binary(FullNameBin) ->
  split_fullname(binary_to_list(FullNameBin));
split_fullname(FullName) when is_list(FullName) ->
  case string:rchr(FullName, $.) of
    0 ->
      %% Dot not found
      false;
    DotPos ->
      { ?NAME(string:substr(FullName, DotPos + 1))
      , ?NAME(string:substr(FullName, 1, DotPos-1))
      }
  end.

%%%=============================================================================
%%% API: Checking correctness of names and types specifications
%%%=============================================================================

%% @doc Tries to cast a value (which can be another Avro value or some erlang
%% term) to the specified Avro type performing conversion if required.
%% @end
-spec cast(avro_type_or_name(), term()) -> {ok, avro_value()} | {error, term()}.
cast(T, ?AVRO_VALUE(T, _) = V) ->
  %% When casting to same type just return the original value
  {ok, V};
cast(TypeName0, Value) when ?IS_NAME_RAW(TypeName0) ->
  TypeName = ?NAME(TypeName0),
  case type_from_name(TypeName) of
    undefined ->
      %% If the type specified by its name then in most cases
      %% we don't know which module should handle it. The only
      %% thing which we can do here is to compare full name of
      %% Type and typeof(Value) and return Value if they are equal.
      %% This assumes also that plain erlang values can't be casted
      %% to types specified only by their names.
      case ?IS_AVRO_VALUE(Value) of
        true ->
          ValueType = ?AVRO_VALUE_TYPE(Value),
          case has_fullname(ValueType, TypeName) of
            true  -> {ok, Value};
            false -> {error, {type_name_mismatch, TypeName0, ValueType}}
          end;
        false ->
          %% Erlang terms can't be casted to names
          {error, {cast_erlang_term_to_name, TypeName0, Value}}
      end;
    Type ->
      %% The only exception are primitive types names because we know
      %% corresponding types and can cast to them.
      cast(Type, Value)
  end;
%% For all other combinations call corresponding cast functions
%% in type modules
cast(#avro_primitive_type{} = T, V) -> avro_primitive:cast(T, V);
cast(#avro_record_type{} = T,    V) -> avro_record:cast(T, V);
cast(#avro_enum_type{} = T,      V) -> avro_enum:cast(T, V);
cast(#avro_array_type{} = T,     V) -> avro_array:cast(T, V);
cast(#avro_map_type{} = T,       V) -> avro_map:cast(T, V);
cast(#avro_union_type{} = T,     V) -> avro_union:cast(T, V);
cast(#avro_fixed_type{} = T,     V) -> avro_fixed:cast(T, V);
cast(Type, _)                       -> {error, {unknown_type, Type}}.

%% @doc Convert avro values to erlang term.
-spec to_term(avro_value()) -> term().
to_term(#avro_value{type = T} = V) -> to_term(T, V).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%% @private
to_term(#avro_primitive_type{}, V) -> avro_primitive:get_value(V);
to_term(#avro_record_type{}, V)    -> avro_record:to_term(V);
to_term(#avro_enum_type{}, V)      -> avro_enum:get_value(V);
to_term(#avro_array_type{}, V)     -> avro_array:to_term(V);
to_term(#avro_map_type{}, V)       -> avro_map:to_term(V);
to_term(#avro_union_type{}, V)     -> avro_union:to_term(V);
to_term(#avro_fixed_type{}, V)     -> avro_fixed:get_value(V);
to_term(T, _)                      -> erlang:error({unknown_type, T}).

%% @private
-spec make_fullname(name_raw(), namespace_raw()) -> name().
make_fullname(Name, ?NS_GLOBAL) -> Name;
make_fullname(Name, Namespace) ->
  ?NAME([?NAME(Namespace), ".", ?NAME(Name)]).

%%%===================================================================
%%% Internal functions: casting
%%%===================================================================

%% @private Checks if the type has specified full name.
-spec has_fullname(avro_type(), fullname()) -> boolean().
has_fullname(Type, FullName) ->
  is_named_type(Type) andalso get_type_fullname(Type) =:= FullName.

%% @private
-spec type_from_name(name()) -> undefined | primitive_type().
type_from_name(?AVRO_NULL)   -> avro_primitive:null_type();
type_from_name(?AVRO_INT)    -> avro_primitive:int_type();
type_from_name(?AVRO_LONG)   -> avro_primitive:long_type();
type_from_name(?AVRO_FLOAT)  -> avro_primitive:float_type();
type_from_name(?AVRO_DOUBLE) -> avro_primitive:double_type();
type_from_name(?AVRO_BYTES)  -> avro_primitive:bytes_type();
type_from_name(?AVRO_STRING) -> avro_primitive:string_type();
type_from_name(_)            -> undefined.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
