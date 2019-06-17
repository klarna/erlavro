%%%-----------------------------------------------------------------------------
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
%%%-----------------------------------------------------------------------------

-ifndef(AVRO_INTERNAL_HRL).
-define(AVRO_INTERNAL_HRL, true).

-include("erlavro.hrl").
-include("avro_stacktrace.hrl").

-define(INT4_MIN, -2147483648).
-define(INT4_MAX,  2147483647).

-define(INT8_MIN, -9223372036854775808).
-define(INT8_MAX,  9223372036854775807).

-define(NS_GLOBAL, ?AVRO_NS_GLOBAL).
-define(NO_DOC, ?AVRO_NO_DOC).

-type ordering() :: ascending | descending | ignore.

-define(NO_VALUE, undefined).

-record(avro_record_field,
        { name      = ?AVRO_REQUIRED :: name()
        , doc       = ?NS_GLOBAL     :: typedoc()
        , type      = ?AVRO_REQUIRED :: type_or_name()
        , default                    :: ?NO_VALUE | avro:in() | avro_value()
        , order     = ascending      :: ordering()
        , aliases   = []             :: [name()]
        }).

-type record_field() :: #avro_record_field{}.

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

-type type_or_name() :: avro_type() | name_raw().

-type custom_prop_name() :: binary().
-type custom_prop_value() :: jsone:json_value().
-type custom_prop() :: {custom_prop_name(), custom_prop_value()}.

-define(ASSIGNED_NAME, <<"_erlavro_assigned">>).

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
-type typedoc() :: string() | binary().
-type enum_index() :: non_neg_integer().
-type union_index() :: non_neg_integer().
-type lkup_fun() :: fun((fullname()) -> avro_type()).
-type schema_store() :: avro_schema_store:store().
-type canonicalized_value() :: null | boolean() | integer() | float() | binary().

-type type_prop_name() :: namespace | doc | aliases | custom_prop_name().
-type type_prop_value() :: namespace() | typedoc() | [name()] | custom_prop_value().
-type type_props() :: [{type_prop_name(), type_prop_value()}].

-type avro_json() :: jsone:json_object().
-type avro_binary() :: iolist().

-type avro_codec() :: null | deflate.

%% Type checks

-define(IS_PRIMITIVE_TYPE(Type), is_record(Type, avro_primitive_type)).

-define(IS_NULL_TYPE(Type),
        ?IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_NULL).

-define(IS_BOOLEAN_TYPE(Type),
        ?IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_BOOLEAN).

-define(IS_INT_TYPE(Type),
        ?IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_INT).

-define(IS_LONG_TYPE(Type),
        ?IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_LONG).

-define(IS_FLOAT_TYPE(Type),
        ?IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_FLOAT).

-define(IS_DOUBLE_TYPE(Type),
        ?IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_DOUBLE).

-define(IS_BYTES_TYPE(Type),
        ?IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_BYTES).

-define(IS_STRING_TYPE(Type),
        ?IS_PRIMITIVE_TYPE(Type) andalso
        Type#avro_primitive_type.name =:= ?AVRO_STRING).

-define(IS_RECORD_TYPE(Type), is_record(Type, avro_record_type)).
-define(IS_ENUM_TYPE(Type),   is_record(Type, avro_enum_type)).
-define(IS_ARRAY_TYPE(Type),  is_record(Type, avro_array_type)).
-define(IS_MAP_TYPE(Type),    is_record(Type, avro_map_type)).
-define(IS_UNION_TYPE(Type),  is_record(Type, avro_union_type)).
-define(IS_FIXED_TYPE(Type),  is_record(Type, avro_fixed_type)).

-define(IS_TYPE_RECORD(Type), is_tuple(Type)).

-define(IS_PRIMITIVE_VALUE(Value), ?IS_PRIMITIVE_TYPE(?AVRO_VALUE_TYPE(Value))).
-define(IS_RECORD_VALUE(Value),    ?IS_RECORD_TYPE(?AVRO_VALUE_TYPE(Value))).
-define(IS_ENUM_VALUE(Value),      ?IS_ENUM_TYPE(?AVRO_VALUE_TYPE(Value))).
-define(IS_ARRAY_VALUE(Value),     ?IS_ARRAY_TYPE(?AVRO_VALUE_TYPE(Value))).
-define(IS_MAP_VALUE(Value),       ?IS_MAP_TYPE(?AVRO_VALUE_TYPE(Value))).
-define(IS_UNION_VALUE(Value),     ?IS_UNION_TYPE(?AVRO_VALUE_TYPE(Value))).
-define(IS_FIXED_VALUE(Value),     ?IS_FIXED_TYPE(?AVRO_VALUE_TYPE(Value))).

-type decoder_hook_fun() ::
        fun((avro_type(), name() | integer(), avro:in(),
            fun((avro:in()) -> avro:out())) -> avro:out()).

%% By default, the hook fun does nothing else but calling the decode function.
-define(DEFAULT_DECODER_HOOK, ?AVRO_DEFAULT_DECODER_HOOK).

-type decoder_options() :: #{ encoding := avro_binary | avro_json
                            , map_type := proplist | map
                            , record_type := proplist | map
                            , is_wrapped := boolean()
                            , hook := decoder_hook_fun()
                            }.

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

-define(ENC_ERR(Reason, Context),
        {'$avro_encode_error', Reason, Context}).

-define(RAISE_ENC_ERR(EXCEPTION_CLASS, EXCEPTION_REASON, THIS_CONTEXT, STACK),
        begin
          {Reason, Context} =
            case EXCEPTION_REASON of
              ?ENC_ERR(ReasonX, ContextX) ->
                {ReasonX, THIS_CONTEXT ++ ContextX};
              _ ->
                {EXCEPTION_REASON, THIS_CONTEXT}
            end,
          erlang:raise(EXCEPTION_CLASS, ?ENC_ERR(Reason, Context), STACK)
        end).
-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
