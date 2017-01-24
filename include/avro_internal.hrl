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
%%%-----------------------------------------------------------------------------

-ifndef(AVRO_INTERNAL_HRL).
-define(AVRO_INTERNAL_HRL, true).

-include("erlavro.hrl").

%% Names of primitive types
-define(AVRO_NULL,    <<"null">>).
-define(AVRO_BOOLEAN, <<"boolean">>).
-define(AVRO_INT,     <<"int">>).
-define(AVRO_LONG,    <<"long">>).
-define(AVRO_FLOAT,   <<"float">>).
-define(AVRO_DOUBLE,  <<"double">>).
-define(AVRO_BYTES,   <<"bytes">>).
-define(AVRO_STRING,  <<"string">>).

%% Other reserved types names
-define(AVRO_RECORD,  <<"record">>).
-define(AVRO_ENUM,    <<"enum">>).
-define(AVRO_ARRAY,   <<"array">>).
-define(AVRO_MAP,     <<"map">>).
-define(AVRO_UNION,   <<"union">>).
-define(AVRO_FIXED,   <<"fixed">>).

-define(INT4_MIN, -2147483648).
-define(INT4_MAX,  2147483647).

-define(INT8_MIN, -9223372036854775808).
-define(INT8_MAX,  9223372036854775807).

-define(NS_GLOBAL, <<"">>).
-define(NO_DOC, <<"">>).

-define(REQUIRED, erlang:error({required_field_missed, ?MODULE, ?LINE})).

-type avro_ordering() :: ascending | descending | ignore.

-record(avro_record_field,
        { name      = ?REQUIRED  :: name()
        , doc       = ?NS_GLOBAL :: typedoc()
        , type      = ?REQUIRED  :: avro_type_or_name()
        , default                :: avro_value() | undefined
        , order     = ascending  :: avro_ordering()
        , aliases   = []         :: [name()]
        }).

-type record_field() :: #avro_record_field{}.

%% fullname of a primitive types is always equal to its name
-record(avro_primitive_type,
        { name      = ?REQUIRED :: name()
        }).

-record(avro_record_type,
        { name      = ?REQUIRED  :: name()
        , namespace = ?NS_GLOBAL :: namespace()
        , doc       = ?NO_DOC    :: typedoc()
        , aliases   = []         :: [name()]
        , fields    = ?REQUIRED  :: [record_field()]
        %% -- service fields --
        , fullname  = ?REQUIRED  :: fullname()
        }).

-record(avro_enum_type,
        { name      = ?REQUIRED  :: name()
        , namespace = ?NS_GLOBAL :: namespace()
        , aliases   = []         :: [name()]
        , doc       = ?NO_DOC    :: typedoc()
        , symbols   = ?REQUIRED  :: [enum_symbol()]
        %% -- service fields --
        , fullname  = ?REQUIRED  :: fullname()
        }).

-record(avro_array_type,
        { type = ?REQUIRED :: avro_type_or_name()
        }).

-record(avro_map_type,
        { type = ?REQUIRED :: avro_type_or_name()
        }).

-record(avro_union_type,
        { types = ?REQUIRED :: [{union_index(), avro_type_or_name()}]
          %% Precached dictionary of types inside the union,
          %% helps to speed up types lookups for big unions.
          %% Dictionary is filled only for big unions (>10)
          %% when dictionary lookup is more efficient than
          %% sequential scan, so it is normal if the dictionary
          %% is set to undefined.
        , types_dict :: undefined | avro_union:types_dict()
        }).

-record(avro_fixed_type,
        { name      = ?REQUIRED  :: name()
        , namespace = ?NS_GLOBAL :: namespace()
        , aliases   = []         :: [name()]
        , size      = ?REQUIRED  :: integer()
        %% -- service fields --
        , fullname  = ?REQUIRED  :: fullname()
        }).

-type primitive_type() :: #avro_primitive_type{}.
-type array_type() :: #avro_array_type{}.
-type enum_type() :: #avro_enum_type{}.
-type fixed_type() :: #avro_fixed_type{}.
-type map_type() :: #avro_map_type{}.
-type record_type() :: #avro_record_type{}.
-type union_type() :: #avro_union_type{}.

-type avro_type()  :: primitive_type()
                    | array_type()
                    | enum_type()
                    | fixed_type()
                    | map_type()
                    | record_type()
                    | union_type().

-type avro_type_or_name() :: avro_type() | name_raw().

-define(IS_PRIMITIVE_NAME(N),
        (N =:= ?AVRO_NULL    orelse
         N =:= ?AVRO_BOOLEAN orelse
         N =:= ?AVRO_INT     orelse
         N =:= ?AVRO_LONG    orelse
         N =:= ?AVRO_STRING  orelse
         N =:= ?AVRO_FLOAT   orelse
         N =:= ?AVRO_DOUBLE  orelse
         N =:= ?AVRO_BYTES)).

-define(IS_AVRO_TYPE(T), is_tuple(T)).

-define(NAME(X), avro_util:canonicalize_name(X)).
-define(DOC(X), unicode:characters_to_binary(X, utf8)).

-define(IS_NAME_RAW(N), (is_atom(N) orelse is_list(N) orelse is_binary(N))).
-define(IS_NAME(N), is_binary(N)).

-type name_raw() :: atom() | string() | binary().
-type namespace_raw() :: atom() | string() | binary().
-type enum_symbol_raw() :: atom() | string() | binary().

%% name, namespace and enum symbols are
%% canonicalized to binary() internally
-type name() :: binary().
-type namespace() :: binary().
-type enum_symbol() :: binary().
-type fullname() :: binary().
-type typedoc() :: iolist().
-type enum_index() :: non_neg_integer().
-type union_index() :: non_neg_integer().
-type lkup_fun() :: fun((fullname()) -> avro_type()).
-type schema_store() :: avro_schema_store:store().
-type canonicalize_primitive_value() :: integer() | binary().

-type type_prop_name() :: namespace | doc | aliases | enclosing_ns.
-type type_prop_value() :: namespace() | typedoc() | [name()].
-type type_props() :: [{type_prop_name(), type_prop_value()}].

-type avro_json() :: jsone:json_object().
-type avro_binary() :: iolist().

%% Type checks

-define(AVRO_IS_PRIMITIVE_TYPE(Type), is_record(Type, avro_primitive_type)).

-define(AVRO_IS_NULL_TYPE(Type),
        ?AVRO_IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_NULL).

-define(AVRO_IS_BOOLEAN_TYPE(Type),
        ?AVRO_IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_BOOLEAN).

-define(AVRO_IS_INT_TYPE(Type),
        ?AVRO_IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_INT).

-define(AVRO_IS_LONG_TYPE(Type),
        ?AVRO_IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_LONG).

-define(AVRO_IS_FLOAT_TYPE(Type),
        ?AVRO_IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_FLOAT).

-define(AVRO_IS_DOUBLE_TYPE(Type),
        ?AVRO_IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_DOUBLE).

-define(AVRO_IS_BYTES_TYPE(Type),
        ?AVRO_IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_BYTES).

-define(AVRO_IS_STRING_TYPE(Type),
        ?AVRO_IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_STRING).

-define(AVRO_IS_RECORD_TYPE(Type), is_record(Type, avro_record_type)).

-define(AVRO_IS_ENUM_TYPE(Type), is_record(Type, avro_enum_type)).

-define(AVRO_IS_ARRAY_TYPE(Type), is_record(Type, avro_array_type)).

-define(AVRO_IS_MAP_TYPE(Type), is_record(Type, avro_map_type)).

-define(AVRO_IS_UNION_TYPE(Type), is_record(Type, avro_union_type)).

-define(AVRO_IS_FIXED_TYPE(Type), is_record(Type, avro_fixed_type)).

%% Values checks

-define(AVRO_IS_NULL_VALUE(Value), ?AVRO_IS_NULL_TYPE(
                                      ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_BOOLEAN_VALUE(Value), ?AVRO_IS_BOOLEAN_TYPE(
                                         ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_INT_VALUE(Value), ?AVRO_IS_INT_TYPE(
                                     ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_LONG_VALUE(Value), ?AVRO_IS_LONG_TYPE(
                                      ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_FLOAT_VALUE(Value), ?AVRO_IS_FLOAT_TYPE(
                                       ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_DOUBLE_VALUE(Value), ?AVRO_IS_DOUBLE_TYPE(
                                        ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_BYTES_VALUE(Value), ?AVRO_IS_BYTES_TYPE(
                                       ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_STRING_VALUE(Value), ?AVRO_IS_STRING_TYPE(
                                        ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_RECORD_VALUE(Value), ?AVRO_IS_RECORD_TYPE(
                                        ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_ENUM_VALUE(Value), ?AVRO_IS_ENUM_TYPE(
                                      ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_ARRAY_VALUE(Value), ?AVRO_IS_ARRAY_TYPE(
                                       ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_MAP_VALUE(Value), ?AVRO_IS_MAP_TYPE(
                                     ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_UNION_VALUE(Value), ?AVRO_IS_UNION_TYPE(
                                       ?AVRO_VALUE_TYPE(Value))).

-define(AVRO_IS_FIXED_VALUE(Value), ?AVRO_IS_FIXED_TYPE(
                                       ?AVRO_VALUE_TYPE(Value))).

%% avro_encoded_value() can be used as a nested inner value of
%% a parent avor_value(), but can not be used for further update or
%% inspection using APIs in avro_xxx modules.
-type avro_encoded_value() :: #avro_value{}.

-type dec_in() :: term(). %% binary() | decoded json struct / raw value
-type dec_out() :: term(). %% decoded raw value or #avro_value{}

-type decoder_hook_fun() ::
        fun((avro_type(), name() | integer(), dec_in(),
            fun((dec_in()) -> dec_out())) -> dec_out()).

%% By default, the hook fun does nothing else but calling the decode function.
-define(DEFAULT_DECODER_HOOK,
        fun(__Type__, __SubNameOrId__, Data, DecodeFun) -> DecodeFun(Data) end).

-define(AVRO_SCHEMA_LOOKUP_FUN(Store), avro_schema_store:to_lookup_fun(Store)).

-define(AVRO_ENCODED_VALUE_JSON(Type, Value),
        ?AVRO_VALUE(Type, {json, Value})).
-define(AVRO_ENCODED_VALUE_BINARY(Type, Value),
        ?AVRO_VALUE(Type, {binary, Value})).

%% Throw an exception in case the value is already encoded.
-define(ASSERT_AVRO_VALUE(VALUE),
        case VALUE of
          {json, _}   -> erlang:throw({value_already_encoded, VALUE});
          {binary, _} -> erlang:throw({value_already_encoded, VALUE});
          _           -> ok
        end).

-define(ERROR_IF(Cond, Err),
        case Cond of
            true  -> erlang:error(Err);
            false -> ok
        end).

-define(ERROR_IF_NOT(Cond, Err), ?ERROR_IF(not (Cond), Err)).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
