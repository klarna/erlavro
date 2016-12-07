%% coding: latin-1
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
%%% @doc
%%% Encodes Avro schemas and values to JSON format using mochijson3
%%% as an encoder.
%%%
%%% Schema is written following parsing canonical form recommendations
%%% but keeps all information (attributes are kept even if they are
%%% not relevant for parsing).
%%% @end
%%%-------------------------------------------------------------------
-module(avro_json_encoder).

%% API
-export([encode_type/1]).
-export([encode_value/1]).
-export([encode_value/2]).

-include("avro_internal.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Encode avro schema in JSON format.
%% We do not expect any failure in avro schema encoding.
%% @end
-spec encode_type(avro_type()) -> iodata().
encode_type(Type) ->
  jsonx:encode(do_encode_type(Type, _Namespace = ?NAMESPACE_NONE)).

%% @doc Encode avro value in JSON format, use jsonx as default encoder.
%% fallback to mochijson3 in case of failure
%% @end
-spec encode_value(avro_value() | avro_encoded_value()) -> iodata().
encode_value(Value) ->
  try encode_value(Value, jsonx)
  catch _ : _ -> encode_value(Value, mochijson3)
  end.

%% @doc Allow caller to choose encoder so it can fallback to another
%% in case of falure etc.
%% @end
-spec encode_value(avro_value(), jsonx | mochijson3) -> iodata().
encode_value(Value, jsonx) ->
  jsonx:encode(do_encode_value(Value));
encode_value(Value, mochijson3) ->
  Encoder = mochijson3:encoder([{utf8, true}]),
  Encoder(do_encode_value(Value)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

optional_field(_Key, Default, Default, _MappingFun) -> [];
optional_field(Key, Value, _Default, MappingFun) -> [{Key, MappingFun(Value)}].

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
                     ?NAMESPACE_NONE, fun encode_string/1)
    , {type,   encode_string("record")}
    , {name,   encode_string(Name)}
    , optional_field(doc,       Doc,       "", fun encode_string/1)
    , optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
    , {fields, lists:map(fun(F) -> encode_field(F, NewEnclosingNamespace) end,
                         Fields)}
    ],
  {struct, lists:flatten(SchemaObjectFields)};
do_encode_type(#avro_enum_type{} = T, EnclosingNamespace) ->
  #avro_enum_type{ name      = Name
                 , namespace = Namespace
                 , aliases   = Aliases
                 , doc       = Doc
                 , symbols   = Symbols} = T,
  SchemaObjectFields =
    [ optional_field(namespace, ns(Namespace, EnclosingNamespace),
                     ?NAMESPACE_NONE, fun encode_string/1)
    , {type,    encode_string("enum")}
    , {name,    encode_string(Name)}
    , optional_field(doc,       Doc,       "", fun encode_string/1)
    , optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
    , {symbols, lists:map(fun encode_string/1, Symbols)}
    ],
  {struct, lists:flatten(SchemaObjectFields)};
do_encode_type(#avro_array_type{type = Type}, EnclosingNamespace) ->
  { struct
  , [ {type,  encode_string("array")}
    , {items, do_encode_type(Type, EnclosingNamespace)}
    ]
  };
do_encode_type(#avro_map_type{type = Type}, EnclosingNamespace) ->
  { struct
  , [ {type,   encode_string("map")}
    , {values, do_encode_type(Type, EnclosingNamespace)}
    ]
  };
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
                     ?NAMESPACE_NONE, fun encode_string/1)
    , {type, encode_string("fixed")}
    , {name, encode_string(Name)}
    , {size, encode_integer(Size)}
    , optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
    ],
  {struct, lists:flatten(SchemaObjectFields)}.

encode_field(Field, EnclosingNamespace) ->
  #avro_record_field{ name    = Name
                    , doc     = Doc
                    , type    = Type
                    , default = Default
                    , order   = Order
                    , aliases = Aliases} = Field,
  { struct
  , [ {name, encode_string(Name)}
    , {type, do_encode_type(Type, EnclosingNamespace)}
    ]
    ++ optional_field(default, Default, undefined, fun do_encode_value/1)
    ++ optional_field(doc,     Doc,     "",        fun encode_string/1)
    ++ optional_field(order,   Order,   ascending, fun encode_order/1)
    ++ optional_field(aliases, Aliases, [],        fun encode_aliases/1)
  }.

%% @private Get namespace to encode.
%% Ignore namespace to encode if it is the same as enclosing namesapce
%% @end
-spec ns(namespace(), namespace()) -> namespace().
ns(Namespace, Namespace)           -> ?NAMESPACE_NONE;
ns(Namespace, _EnclosingNamespace) -> Namespace.

encode_string(String) ->
  erlang:list_to_binary(String).

encode_integer(Int) when is_integer(Int) ->
  Int.

encode_aliases(Aliases) ->
  lists:map(fun encode_string/1, Aliases).

encode_order(ascending)  -> <<"ascending">>;
encode_order(descending) -> <<"descending">>;
encode_order(ignore)     -> <<"ignore">>.

do_encode_value(?AVRO_ENCODED_VALUE_JSON(_Type, _Value = Encoded)) ->
  {json, Encoded};
do_encode_value(Value) when ?AVRO_IS_NULL_VALUE(Value) ->
  null;
do_encode_value(Value) when ?AVRO_IS_BOOLEAN_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value);
do_encode_value(Value) when ?AVRO_IS_INT_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value);
do_encode_value(Value) when ?AVRO_IS_LONG_VALUE(Value) ->
  %% mochijson3 encodes more than 4-bytes integers as floats
  {json, integer_to_list(?AVRO_VALUE_DATA(Value))};
do_encode_value(Value) when ?AVRO_IS_FLOAT_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value);
do_encode_value(Value) when ?AVRO_IS_DOUBLE_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value);
do_encode_value(Value) when ?AVRO_IS_BYTES_VALUE(Value) ->
  %% mochijson3 doesn't support Avro style of encoding binaries
  {json, encode_binary(?AVRO_VALUE_DATA(Value))};
do_encode_value(Value) when ?AVRO_IS_STRING_VALUE(Value) ->
  encode_string(?AVRO_VALUE_DATA(Value));
do_encode_value(Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
  FieldsAndValues = avro_record:to_list(Record),
  { struct
  , lists:map(fun encode_field_with_value/1, FieldsAndValues)
  };
do_encode_value(Enum) when ?AVRO_IS_ENUM_VALUE(Enum) ->
  encode_string(?AVRO_VALUE_DATA(Enum));
do_encode_value(Array) when ?AVRO_IS_ARRAY_VALUE(Array) ->
  lists:map(fun do_encode_value/1, ?AVRO_VALUE_DATA(Array));
do_encode_value(Map) when ?AVRO_IS_MAP_VALUE(Map) ->
  L = dict:to_list(avro_map:to_dict(Map)),
  { struct
  , lists:map(fun encode_field_with_value/1, L)
  };
do_encode_value(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  Data = avro_union:get_value(Union),
  case ?AVRO_IS_NULL_VALUE(Data) of
    true  -> null; %% Nulls don't need a type to be specified
    false ->
      { struct
      , [{encode_string(avro:get_type_fullname(?AVRO_VALUE_TYPE(Data))),
          do_encode_value(Data)}]
      }
  end;
do_encode_value(Fixed) when ?AVRO_IS_FIXED_VALUE(Fixed) ->
  %% mochijson3 doesn't support Avro style of encoding binaries
  {json, encode_binary(?AVRO_VALUE_DATA(Fixed))}.

encode_field_with_value({FieldName, Value}) ->
  {encode_string(FieldName), do_encode_value(Value)}.

encode_binary(Bin) ->
  [$", encode_binary_body(Bin), $"].

encode_binary_body(<<>>) ->
  "";
encode_binary_body(<<H1:4, H2:4, Rest/binary>>) ->
  [$\\, $u, $0, $0, to_hex(H1), to_hex(H2) |encode_binary_body(Rest)].

to_hex(D) when D >= 0 andalso D =< 9 ->
  D + $0;
to_hex(D) when D >= 10 andalso D =< 15 ->
  D - 10 + $a.


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
