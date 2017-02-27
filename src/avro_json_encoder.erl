%% coding: latin-1
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
%%% @doc
%%%
%%% Schema is written following parsing canonical form recommendations
%%% but keeps all information (attributes are kept even if they are
%%% not relevant for parsing).
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro_json_encoder).

%% API
-export([encode_type/1]).
-export([encode_value/1]).
-export([encode/3]).

-include("avro_internal.hrl").

-type json_value() :: jsone:json_value().
-define(INLINE(JSON), {{json, JSON}}).

%%%_* APIs =====================================================================

%% @doc Encode avro schema in JSON format.
%% @end
-spec encode_type(avro_type()) -> iodata().
encode_type(Type) ->
  encode_json(do_encode_type(Type, _Namespace = ?NS_GLOBAL)).

%% @doc Encode avro value in JSON format.
%% @end
-spec encode_value(avro_value() | avro_encoded_value()) -> iodata().
encode_value(Value) ->
  encode_json(do_encode_value(Value)).

%% @doc Encode unwrapped (raw) values directly without (possibilly
%% recursive) type info wrapped with values.
%% i.e. data can be recursive, but recursive types are resolved by
%% schema lookup
%% @end
-spec encode(schema_store() | lkup_fun(), avro_type_or_name(), avro:in()) ->
        iodata().
encode(Store, TypeOrName, Value) when not is_function(Store) ->
  Lkup = ?AVRO_SCHEMA_LOOKUP_FUN(Store),
  encode(Lkup, TypeOrName, Value);
encode(Lkup, TypeOrName, Value) ->
  encode_json(do_encode(Lkup, TypeOrName, Value)).

%%%_* Internal functions =======================================================

%% @private
-spec encode_json(json_value()) -> iodata().
encode_json(Input) -> jsone:encode(Input, [native_utf8]).

%% @private
-spec do_encode(lkup_fun(), avro_type_or_name(), avro_value() | avro:in()) ->
        json_value().
do_encode(_Lkup, Type, #avro_value{type = Type} = V) ->
  do_encode_value(V);
do_encode(Lkup, TypeName, Value) when ?IS_NAME_RAW(TypeName) ->
  do_encode(Lkup, Lkup(?NAME(TypeName)), Value);
do_encode(_Lkup, Type, Value) when ?AVRO_IS_PRIMITIVE_TYPE(Type) ->
  {ok, AvroValue} = avro_primitive:cast(Type, Value),
  do_encode_value(AvroValue);
do_encode(Lkup, Type, Value) when ?AVRO_IS_RECORD_TYPE(Type) ->
  avro_record:encode(Type, Value,
    fun({FN, FT, FV}) -> {encode_string(FN), do_encode(Lkup, FT, FV)} end);
do_encode(_Lkup, Type, Value) when ?AVRO_IS_ENUM_TYPE(Type) ->
  encode_string(Value);
do_encode(Lkup, Type, Value) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  avro_array:encode(Type, Value,
    fun(IType, Item) -> do_encode(Lkup, IType, Item) end);
do_encode(Lkup, Type, Value) when ?AVRO_IS_MAP_TYPE(Type) ->
  avro_map:encode(Type, Value,
    fun(IType, K, V) -> {encode_string(K), do_encode(Lkup, IType, V)} end);
do_encode(_Lkup, Type, Value) when ?AVRO_IS_FIXED_TYPE(Type) ->
  ?INLINE(encode_binary(Value));
do_encode(_Lkup, Type, null) when ?AVRO_IS_UNION_TYPE(Type) ->
  null; %do not encode null
do_encode(Lkup, Type, Union) when ?AVRO_IS_UNION_TYPE(Type) ->
  Encoded = avro_union:encode(Type, Union,
    fun(MemberT, Value, _UnionIndex) ->
      {
        encode_string(avro:get_type_fullname(MemberT)),
        do_encode(Lkup, MemberT, Value)
      }
    end),
  [Encoded].

%% @private
optional_field(_Key, Default, Default, _MappingFun) -> [];
optional_field(Key, Value, _Default, MappingFun) -> [{Key, MappingFun(Value)}].

%% @private
do_encode_type(Name, EnclosingNamespace) when ?IS_NAME(Name) ->
  MaybeShortName =
    case avro:split_type_name(Name, EnclosingNamespace) of
      {ShortName, EnclosingNamespace} -> ShortName;
      {_ShortName, _AnotherNamespace} -> Name
    end,
  encode_string(MaybeShortName);
do_encode_type(#avro_primitive_type{name = Name}, _Namespace) ->
  encode_string(Name);
do_encode_type(#avro_record_type{} = T, EnclosingNamespace) ->
  #avro_record_type{ name      = Name
                   , namespace = Namespace
                   , doc       = Doc
                   , aliases   = Aliases
                   , fields    = Fields
                   } = T,
  {Name, NewEnclosingNamespace} = avro:split_type_name(T, EnclosingNamespace),
  SchemaObjectFields =
    [ optional_field(namespace, ns(Namespace, EnclosingNamespace),
                     ?NS_GLOBAL, fun encode_string/1)
    , {type,   encode_string(?AVRO_RECORD)}
    , {name,   encode_string(Name)}
    , optional_field(doc,       Doc,  ?NO_DOC, fun encode_string/1)
    , optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
    , {fields, lists:map(fun(F) -> encode_field(F, NewEnclosingNamespace) end,
                         Fields)}
    ],
  lists:flatten(SchemaObjectFields);
do_encode_type(#avro_enum_type{} = T, EnclosingNamespace) ->
  #avro_enum_type{ name      = Name
                 , namespace = Namespace
                 , aliases   = Aliases
                 , doc       = Doc
                 , symbols   = Symbols} = T,
  SchemaObjectFields =
    [ optional_field(namespace, ns(Namespace, EnclosingNamespace),
                     ?NS_GLOBAL, fun encode_string/1)
    , {type,    encode_string(?AVRO_ENUM)}
    , {name,    encode_string(Name)}
    , optional_field(doc,       Doc,  ?NO_DOC, fun encode_string/1)
    , optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
    , {symbols, lists:map(fun encode_string/1, Symbols)}
    ],
  lists:flatten(SchemaObjectFields);
do_encode_type(#avro_array_type{type = Type}, EnclosingNamespace) ->
  [ {type,  encode_string(?AVRO_ARRAY)}
  , {items, do_encode_type(Type, EnclosingNamespace)}
  ];
do_encode_type(#avro_map_type{type = Type}, EnclosingNamespace) ->
  [ {type,   encode_string(?AVRO_MAP)}
  , {values, do_encode_type(Type, EnclosingNamespace)}
  ];
do_encode_type(#avro_union_type{types = Types}, EnclosingNamespace) ->
  F = fun({_Index, Type}) -> do_encode_type(Type, EnclosingNamespace) end,
  lists:map(F, Types);
do_encode_type(#avro_fixed_type{} = T, EnclosingNamespace) ->
  #avro_fixed_type{ name = Name
                  , namespace = Namespace
                  , aliases = Aliases
                  , size = Size} = T,
  SchemaObjectFields =
    [ optional_field(namespace, ns(Namespace, EnclosingNamespace),
                     ?NS_GLOBAL, fun encode_string/1)
    , {type, encode_string(?AVRO_FIXED)}
    , {name, encode_string(Name)}
    , {size, encode_integer(Size)}
    , optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
    ],
  lists:flatten(SchemaObjectFields).

%% @private
encode_field(Field, EnclosingNamespace) ->
  #avro_record_field{ name    = Name
                    , doc     = Doc
                    , type    = Type
                    , default = Default
                    , order   = Order
                    , aliases = Aliases} = Field,
  [ {name, encode_string(Name)}
  , {type, do_encode_type(Type, EnclosingNamespace)}
  ]
  ++ optional_field(default, Default, undefined, fun do_encode_value/1)
  ++ optional_field(doc,     Doc,     ?NO_DOC,   fun encode_string/1)
  ++ optional_field(order,   Order,   ascending, fun encode_order/1)
  ++ optional_field(aliases, Aliases, [],        fun encode_aliases/1).

%% @private Get namespace to encode.
%% Ignore namespace to encode if it is the same as enclosing namesapce
%% @end
-spec ns(namespace(), namespace()) -> namespace().
ns(Namespace, Namespace)           -> ?NS_GLOBAL;
ns(Namespace, _EnclosingNamespace) -> Namespace.

%% @private
encode_string(String) ->
  erlang:iolist_to_binary(String).

%% @private
encode_integer(Int) when is_integer(Int) ->
  Int.

%% @private
encode_aliases(Aliases) ->
  lists:map(fun encode_string/1, Aliases).

%% @private Never have to encode ascending because that's default.
encode_order(descending) -> <<"descending">>;
encode_order(ignore)     -> <<"ignore">>.

%% @private
-spec do_encode_value(avro_value()) -> json_value().
do_encode_value(?AVRO_ENCODED_VALUE_JSON(_Type, _Value = Encoded)) ->
  ?INLINE(Encoded);
do_encode_value(Value) when ?AVRO_IS_NULL_VALUE(Value) ->
  null;
do_encode_value(Value) when ?AVRO_IS_BOOLEAN_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value);
do_encode_value(Value) when ?AVRO_IS_INT_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value);
do_encode_value(Value) when ?AVRO_IS_LONG_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value);
do_encode_value(Value) when ?AVRO_IS_FLOAT_VALUE(Value) ->
  encode_float(?AVRO_VALUE_DATA(Value));
do_encode_value(Value) when ?AVRO_IS_DOUBLE_VALUE(Value) ->
  encode_float(?AVRO_VALUE_DATA(Value));
do_encode_value(Value) when ?AVRO_IS_BYTES_VALUE(Value) ->
  %% jsone treats binary as utf8 string
  ?INLINE(encode_binary(?AVRO_VALUE_DATA(Value)));
do_encode_value(Value) when ?AVRO_IS_STRING_VALUE(Value) ->
  encode_string(?AVRO_VALUE_DATA(Value));
do_encode_value(Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
  FieldsAndValues = avro_record:to_list(Record),
  lists:map(fun encode_field_with_value/1, FieldsAndValues);
do_encode_value(Enum) when ?AVRO_IS_ENUM_VALUE(Enum) ->
  encode_string(?AVRO_VALUE_DATA(Enum));
do_encode_value(Array) when ?AVRO_IS_ARRAY_VALUE(Array) ->
  lists:map(fun do_encode_value/1, ?AVRO_VALUE_DATA(Array));
do_encode_value(Map) when ?AVRO_IS_MAP_VALUE(Map) ->
  L = avro_map:to_list(Map),
  lists:map(fun encode_field_with_value/1, L);
do_encode_value(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  Data = ?AVRO_VALUE_DATA(Union),
  case ?AVRO_IS_NULL_VALUE(Data) of
    true ->
      null; %% Nulls don't need a type to be specified
    false ->
      TypeName = encode_string(avro:get_type_fullname(?AVRO_VALUE_TYPE(Data))),
      [{TypeName, do_encode_value(Data)}]
  end;
do_encode_value(Fixed) when ?AVRO_IS_FIXED_VALUE(Fixed) ->
  %% jsone treats binary as utf8 string
  ?INLINE(encode_binary(?AVRO_VALUE_DATA(Fixed))).

%% @private
encode_field_with_value({FieldName, Value}) ->
  {encode_string(FieldName), do_encode_value(Value)}.

%% @private
encode_binary(Bin) ->
  [$", encode_binary_body(Bin), $"].

%% @private
encode_binary_body(<<>>) ->
  "";
encode_binary_body(<<H1:4, H2:4, Rest/binary>>) ->
  [$\\, $u, $0, $0, to_hex(H1), to_hex(H2) |encode_binary_body(Rest)].

%% @private
to_hex(D) when D >= 0 andalso D =< 9 ->
  D + $0;
to_hex(D) when D >= 10 andalso D =< 15 ->
  D - 10 + $a.

%% @private
encode_float(Number) ->
  ?INLINE(iolist_to_binary(io_lib:format("~p", [Number]))).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
