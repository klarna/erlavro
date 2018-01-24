%% coding: latin-1
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
-export([ encode_schema/1
        , encode_schema/2
        , encode_type/1
        , encode_value/1
        , encode/3
        ]).

-include("avro_internal.hrl").

-type json_value() :: jsone:json_value().
-define(INLINE(JSON), {{json, JSON}}).

%%%_* APIs =====================================================================

%% @doc Encode avro schema in JSON format.
%% @end
-spec encode_schema(avro_type()) -> iodata().
encode_schema(Type) ->
    encode_schema(Type, #{}).

-spec encode_schema(avro_type(), map()) -> iodata().
encode_schema(Type0, Options) ->
  Type1 = avro_util:resolve_duplicated_refs(Type0),
  Lkup = avro:make_lkup_fun(?ASSIGNED_NAME, Type1),
  Type = avro_util:encode_defaults(Type1, Lkup),
  encode_json(do_encode_type(Type, _Namespace = ?NS_GLOBAL, Options)).

%% @doc Encode avro schema in JSON format.
-spec encode_type(avro_type()) -> iodata().
encode_type(Type) -> encode_schema(Type).

%% @doc Encode avro value in JSON format.
%% @end
-spec encode_value(avro_value()) -> iodata().
encode_value(Value) ->
  encode_json(do_encode_value(Value)).

%% @doc Encode unwrapped (raw) values directly without (possibilly
%% recursive) type info wrapped with values.
%% i.e. data can be recursive, but recursive types are resolved by
%% schema lookup
%% @end
-spec encode(avro:schema_all(), type_or_name(), avro:in()) -> iodata().
encode(Sc, TypeOrName, Value) ->
  Lkup = avro_util:ensure_lkup_fun(Sc),
  encode_json(do_encode(Lkup, TypeOrName, Value)).

%%%_* Internal functions =======================================================

%% @private
-spec encode_json(json_value()) -> iodata().
encode_json(Input) -> jsone:encode(Input, [native_utf8]).

%% @private
-spec do_encode(lkup_fun(), type_or_name(), avro_value() | avro:in()) ->
        json_value().
do_encode(Lkup, Type, #avro_value{type = T} = V) ->
  case avro:is_same_type(Type, T) of
    true  -> do_encode_value(V);
    false -> enc(Lkup, Type, V) %% try deeper
  end;
do_encode(Lkup, TypeName, Value) when ?IS_NAME_RAW(TypeName) ->
  enc(Lkup, Lkup(?NAME(TypeName)), Value);
do_encode(Lkup, Type, Value) ->
  enc(Lkup, Type, Value).

%% @private
-spec enc(lkup_fun(), type_or_name(), avro_value() | avro:in()) ->
        json_value().
enc(_Lkup, Type, Value) when ?IS_PRIMITIVE_TYPE(Type) ->
  {ok, AvroValue} = avro_primitive:cast(Type, Value),
  do_encode_value(AvroValue);
enc(Lkup, Type, Value) when ?IS_RECORD_TYPE(Type) ->
  avro_record:encode(Type, Value,
    fun(FN, FT, FV) -> {encode_string(FN), do_encode(Lkup, FT, FV)} end);
enc(_Lkup, Type, Value) when ?IS_ENUM_TYPE(Type) ->
  {ok, ?AVRO_VALUE(_, Str)} = avro_enum:cast(Type, Value),
  encode_string(Str);
enc(Lkup, Type, Value) when ?IS_ARRAY_TYPE(Type) ->
  avro_array:encode(Type, Value,
    fun(IType, Item) -> do_encode(Lkup, IType, Item) end);
enc(Lkup, Type, Value) when ?IS_MAP_TYPE(Type) ->
  avro_map:encode(Type, Value,
    fun(IType, K, V) -> {encode_string(K), do_encode(Lkup, IType, V)} end);
enc(_Lkup, Type, Value) when ?IS_FIXED_TYPE(Type) ->
  ?INLINE(encode_binary(Value));
enc(_Lkup, Type, null) when ?IS_UNION_TYPE(Type) ->
  {ok, _} = avro_union:lookup_type(null, Type), %% assert
  null; %do not encode null
enc(Lkup, Type, Union) when ?IS_UNION_TYPE(Type) ->
  Encoded = avro_union:encode(Type, Union,
    fun(MemberT, Value, _UnionIndex) ->
      {
        encode_string(avro:get_type_fullname(MemberT)),
        do_encode(Lkup, MemberT, Value)
      }
    end),
  [Encoded].

%% @private
optional_field(aliases,   _, _, _, #{canon := true}) -> [];
optional_field(default,   _, _, _, #{canon := true}) -> [];
optional_field(doc,       _, _, _, #{canon := true}) -> [];
optional_field(namespace, _, _, _, #{canon := true}) -> [];
optional_field(order,     _, _, _, #{canon := true}) -> [];
optional_field(_Key, Default, Default, _MappingFun, _Options) -> [];
optional_field(Key, Value, _Default, MappingFun, _Options) -> [{Key, MappingFun(Value)}].

% type, name, fields, symbols, items, values, size

%% @private
do_encode_type(Name, EnclosingNamespace, #{canon := true} = Options) when ?IS_NAME(Name) ->
  {ShortName, Namespace} = case split_fullname(Name) of
                            {_, _} = N ->
                               %% Fullname
                               N;
                            false ->
                               %% Name without without namespace
                               {Name, EnclosingNamespace}
                          end,
  encode_name(ShortName, Namespace, Options);
do_encode_type(Name, EnclosingNamespace, _Options) when ?IS_NAME(Name) ->
  MaybeShortName =
    case avro:split_type_name(Name, EnclosingNamespace) of
      {ShortName, EnclosingNamespace} -> ShortName;
      {_ShortName, _AnotherNamespace} -> Name
    end,
  encode_string(MaybeShortName);
do_encode_type(#avro_primitive_type{name = Name, custom = []}, _Ns, _Options) ->
  encode_string(Name);
do_encode_type(#avro_primitive_type{name = Name, custom = Custom}, _Ns, _Options) ->
  [ {type, encode_string(Name)}
  | Custom
  ];
do_encode_type(#avro_record_type{} = T, EnclosingNamespace, Options) ->
  #avro_record_type{ name      = Name
                   , namespace = Namespace
                   , doc       = Doc
                   , aliases   = Aliases
                   , fields    = Fields
                   , custom    = CustomProps
                   } = T,
  {Name, NextLevelEnclosingNs} = avro:split_type_name(T, Namespace),
  SchemaObjectFields =
    [ optional_field(namespace, ns(Namespace, EnclosingNamespace),
                     ?NS_GLOBAL, fun encode_string/1, Options)
    , {name, encode_name(Name, canon_ns(Namespace, EnclosingNamespace), Options)}
    , {type, encode_string(?AVRO_RECORD)}
    , optional_field(doc,       Doc,  ?NO_DOC, fun encode_string/1, Options)
    , optional_field(aliases,   Aliases,   [], fun encode_aliases/1, Options)
    , {fields, lists:map(fun(F) ->
                             encode_field(F, NextLevelEnclosingNs, Options)
                         end, Fields)}
    | CustomProps
    ],
  lists:flatten(SchemaObjectFields);
do_encode_type(#avro_enum_type{} = T, EnclosingNamespace, Options) ->
  #avro_enum_type{ name      = Name
                 , namespace = Namespace
                 , aliases   = Aliases
                 , doc       = Doc
                 , symbols   = Symbols
                 , custom    = CustomProps
                 } = T,
  SchemaObjectFields =
    [ optional_field(namespace, ns(Namespace, EnclosingNamespace),
                     ?NS_GLOBAL, fun encode_string/1, Options)
    , {name, encode_name(Name, canon_ns(Namespace, EnclosingNamespace), Options)}
    , {type, encode_string(?AVRO_ENUM)}
    , optional_field(doc,       Doc,  ?NO_DOC, fun encode_string/1, Options)
    , optional_field(aliases,   Aliases,   [], fun encode_aliases/1, Options)
    , {symbols, lists:map(fun encode_string/1, Symbols)}
    | CustomProps
    ],
  lists:flatten(SchemaObjectFields);
do_encode_type(#avro_array_type{ type   = Type
                               , custom = CustomProps
                               }, EnclosingNamespace, Options) ->
  [ {type,  encode_string(?AVRO_ARRAY)}
  , {items, do_encode_type(Type, EnclosingNamespace, Options)}
  | CustomProps
  ];
do_encode_type(#avro_map_type{ type   = Type
                             , custom = CustomProps
                             }, EnclosingNamespace, Options) ->
  [ {type,   encode_string(?AVRO_MAP)}
  , {values, do_encode_type(Type, EnclosingNamespace, Options)}
  | CustomProps
  ];
do_encode_type(#avro_union_type{} = T, EnclosingNamespace, Options) ->
  Members = avro_union:get_types(T),
  F = fun(Type) -> do_encode_type(Type, EnclosingNamespace, Options) end,
  lists:map(F, Members);
do_encode_type(#avro_fixed_type{} = T, EnclosingNamespace, Options) ->
  #avro_fixed_type{ name      = Name
                  , namespace = Namespace
                  , aliases   = Aliases
                  , size      = Size
                  , custom    = CustomProps
                  } = T,
  SchemaObjectFields =
    [ optional_field(namespace, ns(Namespace, EnclosingNamespace),
                     ?NS_GLOBAL, fun encode_string/1, Options)
    , {name, encode_name(Name, canon_ns(Namespace, EnclosingNamespace), Options)}
    , {type, encode_string(?AVRO_FIXED)}
    , {size, encode_integer(Size)}
    , optional_field(aliases,   Aliases,   [], fun encode_aliases/1, Options)
    | CustomProps
    ],
  lists:flatten(SchemaObjectFields).

%% @private
encode_field(Field, EnclosingNamespace, Options) ->
  #avro_record_field{ name    = Name
                    , doc     = Doc
                    , type    = Type
                    , default = Default
                    , order   = Order
                    , aliases = Aliases} = Field,
  [ {name, encode_name(Name, EnclosingNamespace, Options)}
  , {type, do_encode_type(Type, EnclosingNamespace, Options)}
  ]
  ++ optional_field(default, Default, ?NO_VALUE, fun(X) -> ?INLINE(X) end, Options)
  ++ optional_field(doc,     Doc,     ?NO_DOC,   fun encode_string/1, Options)
  ++ optional_field(order,   Order,   ascending, fun encode_order/1, Options)
  ++ optional_field(aliases, Aliases, [],        fun encode_aliases/1, Options).

%% @private Get namespace to encode.
%% Ignore namespace to encode if it is the same as enclosing namesapce
%% @end
-spec ns(namespace(), namespace()) -> namespace().
ns(Namespace, Namespace)           -> ?NS_GLOBAL;
ns(Namespace, _EnclosingNamespace) -> Namespace.

%% @private Get namespace to encode for Canonical Parsing Form.
%% @end
-spec canon_ns(namespace(), namespace()) -> namespace().
canon_ns(Namespace, Namespace) -> Namespace;
canon_ns(Namespace, ?NS_GLOBAL) -> Namespace;
canon_ns(?NS_GLOBAL, Namespace) -> Namespace;
canon_ns(Namespace, _EnclosingNamespace) -> Namespace.

%% @private
encode_string(String) ->
  erlang:iolist_to_binary(String).

%% @private
encode_name(Name, ?NS_GLOBAL, #{canon := true}) ->
  erlang:iolist_to_binary(Name);
encode_name(Name, Namespace, #{canon := true}) ->
  erlang:iolist_to_binary([Namespace, <<".">>, Name]);
encode_name(Name, _Namespace, _Options) ->
  erlang:iolist_to_binary(Name).

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
do_encode_value(Value) when ?IS_PRIMITIVE_VALUE(Value) ->
  #avro_primitive_type{name = Name} = ?AVRO_VALUE_TYPE(Value),
  encode_primitive(Name, ?AVRO_VALUE_DATA(Value));
do_encode_value(Record) when ?IS_RECORD_VALUE(Record) ->
  FieldsAndValues = avro_record:to_list(Record),
  lists:map(fun encode_field_with_value/1, FieldsAndValues);
do_encode_value(Enum) when ?IS_ENUM_VALUE(Enum) ->
  encode_string(?AVRO_VALUE_DATA(Enum));
do_encode_value(Array) when ?IS_ARRAY_VALUE(Array) ->
  lists:map(fun do_encode_value/1, ?AVRO_VALUE_DATA(Array));
do_encode_value(Map) when ?IS_MAP_VALUE(Map) ->
  L = avro_map:to_list(Map),
  lists:map(fun encode_field_with_value/1, L);
do_encode_value(Fixed) when ?IS_FIXED_VALUE(Fixed) ->
  %% jsone treats binary as utf8 string
  ?INLINE(encode_binary(?AVRO_VALUE_DATA(Fixed)));
do_encode_value(Union) when ?IS_UNION_VALUE(Union) ->
  Data = ?AVRO_VALUE_DATA(Union),
  MemberType = ?AVRO_VALUE_TYPE(Data),
  case ?IS_NULL_TYPE(MemberType) of
    true ->
      null; %% Nulls don't need a type to be specified
    false ->
      TypeName = encode_string(avro:get_type_fullname(MemberType)),
      [{TypeName, do_encode_value(Data)}]
  end.

%% @private
encode_primitive(?AVRO_NULL, _)        -> null;
encode_primitive(?AVRO_BOOLEAN, Bool)  -> Bool;
encode_primitive(?AVRO_INT, Int)       -> Int;
encode_primitive(?AVRO_LONG, Long)     -> Long;
encode_primitive(?AVRO_FLOAT, Float)   -> encode_float(Float);
encode_primitive(?AVRO_DOUBLE, Double) -> encode_float(Double);
encode_primitive(?AVRO_BYTES, Bytes)   -> ?INLINE(encode_binary(Bytes));
encode_primitive(?AVRO_STRING, String) -> encode_string(String).

%% @private
encode_field_with_value({FieldName, Value}) ->
  {encode_string(FieldName), do_encode_value(Value)}.

%% @private jsone treats binary as utf8 string.
%% encode per avro spec.
%% @end
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

%% TODO: this was taken from avro.erl, where it is private
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

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
