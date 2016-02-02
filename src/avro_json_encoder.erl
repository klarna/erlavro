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

-include("erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Encode avro schema in JSON format.
%% We do not expect any failure in avro schema encoding.
%% @end
-spec encode_type(avro_type()) -> iodata().
encode_type(Type) ->
  jsonx:encode(do_encode_type(Type)).

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

do_encode_type(Name) when is_list(Name) ->
  encode_string(Name);

do_encode_type(#avro_primitive_type{name = Name}) ->
  encode_string(Name);

do_encode_type(#avro_record_type{} = T) ->
  #avro_record_type{ name      = Name
                   , namespace = Namespace
                   , doc       = Doc
                   , aliases   = Aliases
                   , fields    = Fields
                   } = T,
  { struct
  , [ {type,   encode_string("record")}
    , {name,   encode_string(Name)}
    , {fields, lists:map(fun encode_field/1, Fields)}
    ]
    ++ optional_field(namespace, Namespace, "", fun encode_string/1)
    ++ optional_field(doc,       Doc,       "", fun encode_string/1)
    ++ optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
  };

do_encode_type(#avro_enum_type{} = T) ->
  #avro_enum_type{ name      = Name
                 , namespace = Namespace
                 , aliases   = Aliases
                 , doc       = Doc
                 , symbols   = Symbols} = T,
  { struct
  , [ {type,    encode_string("enum")}
    , {name,    encode_string(Name)}
    , {symbols, lists:map(fun encode_string/1, Symbols)}
    ]
    ++ optional_field(namespace, Namespace, "", fun encode_string/1)
    ++ optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
    ++ optional_field(doc,       Doc,       "", fun encode_string/1)
  };

do_encode_type(#avro_array_type{type = Type}) ->
  { struct
  , [ {type,  encode_string("array")}
    , {items, do_encode_type(Type)}
    ]
  };

do_encode_type(#avro_map_type{type = Type}) ->
  { struct
  , [ {type,   encode_string("map")}
    , {values, do_encode_type(Type)}
    ]
  };

do_encode_type(#avro_union_type{types = Types}) ->
  lists:map(fun do_encode_type/1, Types);

do_encode_type(#avro_fixed_type{} = T) ->
  #avro_fixed_type{ name = Name
                  , namespace = Namespace
                  , aliases = Aliases
                  , size = Size} = T,
  { struct
  , [ {type, encode_string("fixed")}
    , {name, encode_string(Name)}
    , {size, encode_integer(Size)}
    ]
    ++ optional_field(namespace, Namespace, "", fun encode_string/1)
    ++ optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
  }.

encode_field(Field) ->
  #avro_record_field{ name    = Name
                    , doc     = Doc
                    , type    = Type
                    , default = Default
                    , order   = Order
                    , aliases = Aliases} = Field,
  { struct
  , [ {name, encode_string(Name)}
    , {type, do_encode_type(Type)}
    ]
    ++ optional_field(default, Default, undefined, fun do_encode_value/1)
    ++ optional_field(doc,     Doc,     "",        fun encode_string/1)
    ++ optional_field(order,   Order,   ascending, fun encode_order/1)
    ++ optional_field(aliases, Aliases, [],        fun encode_aliases/1)
  }.

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

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

to_string(Json) ->
  binary_to_list(iolist_to_binary(Json)).

sample_record_type() ->
    avro_record:type(
      "SampleRecord",
      [ avro_record:define_field("bool", avro_primitive:boolean_type(),
                                 [ {doc, "bool f"}
                                 , {default, avro_primitive:boolean(false)}
                                 ])
      , avro_record:define_field("int", avro_primitive:int_type(),
                                 [ {doc, "int f"}
                                 , {default, avro_primitive:int(0)}
                                 ])
      , avro_record:define_field("long", avro_primitive:long_type(),
                                 [ {doc, "long f"}
                                 , {default, avro_primitive:long(42)}
                                 ])
      , avro_record:define_field("float", avro_primitive:float_type(),
                                 [ {doc, "float f"}
                                 , {default, avro_primitive:float(3.14)}
                                 ])
      , avro_record:define_field("double", avro_primitive:double_type(),
                                 [ {doc, "double f"}
                                 , {default, avro_primitive:double(6.67221937)}
                                 ])
      , avro_record:define_field("bytes", avro_primitive:bytes_type(),
                                 [ {doc, "bytes f"}
                                 ])
      , avro_record:define_field("string", avro_primitive:string_type(),
                                 [ {doc, "string f"}
                                 , {default,
                                    avro_primitive:string("string value")}
                                 ])
      ],
      [ {namespace, "com.klarna.test.bix"}
      , {doc, "Record documentation"}]).

sample_record() ->
  avro_record:new(sample_record_type(),
                  [ {"string", "string value"}
                  , {"double", 3.14159265358}
                  , {"long",   123456789123456789}
                  , {"bool",   true}
                  , {"int",    100}
                  , {"float",  2.718281828}
                  , {"bytes",  <<"bytes value">>}
                  ]).

encode_null_type_test() ->
    Json = encode_type(avro_primitive:null_type()),
    ?assertEqual("\"null\"", to_string(Json)).

encode_null_test() ->
    Json = encode_value(avro_primitive:null()),
    ?assertEqual("null", to_string(Json)).

encode_boolean_type_test() ->
    Json = encode_type(avro_primitive:boolean_type()),
    ?assertEqual("\"boolean\"", to_string(Json)).

encode_boolean_test() ->
    JsonTrue = encode_value(avro_primitive:boolean(true)),
    JsonFalse = encode_value(avro_primitive:boolean(false)),
    ?assertEqual("true", to_string(JsonTrue)),
    ?assertEqual("false", to_string(JsonFalse)).

encode_int_type_test() ->
    Json = encode_type(avro_primitive:int_type()),
    ?assertEqual("\"int\"", to_string(Json)).

encode_int_test() ->
    Json = encode_value(avro_primitive:int(1)),
    ?assertEqual("1", to_string(Json)).

encode_long_type_test() ->
    Json = encode_type(avro_primitive:long_type()),
    ?assertEqual("\"long\"", to_string(Json)).

encode_long_test() ->
    Json = encode_value(avro_primitive:long(12345678901)),
    ?assertEqual("12345678901", to_string(Json)).

encode_float_type_test() ->
    Json = encode_type(avro_primitive:float_type()),
    ?assertEqual("\"float\"", to_string(Json)).

encode_float_test() ->
    Json = encode_value(avro_primitive:float(3.14159265358)),
    ?assertEqual("3.14159265358", to_string(Json)).

encode_float_precision_lost_test() ->
    %% Warning: implementation of doubles in erlang loses
    %% precision on such numbers.
    ?assertEqual(<<"1e+16">>,
      encode_value(avro_primitive:float(10000000000000001), jsonx)),
    ?assertEqual("1.0e+16",
      encode_value(avro_primitive:float(10000000000000001), mochijson3)).

encode_integer_float_test() ->
    ?assertEqual(<<"314159265358">>,
      encode_value(avro_primitive:float(314159265358), jsonx)),
    ?assertEqual("314159265358.0",
      encode_value(avro_primitive:float(314159265358), mochijson3)).

encode_double_type_test() ->
    Json = encode_type(avro_primitive:double_type()),
    ?assertEqual("\"double\"", to_string(Json)).

encode_double_test() ->
    Json = encode_value(avro_primitive:double(3.14159265358)),
    ?assertEqual("3.14159265358", to_string(Json)).

encode_integer_double_test() ->
    ?assertEqual(<<"314159265358">>,
      encode_value(avro_primitive:double(314159265358), jsonx)),
    ?assertEqual("314159265358.0",
      encode_value(avro_primitive:double(314159265358), mochijson3)).

encode_bytes_type_test() ->
    Json = encode_type(avro_primitive:bytes_type()),
    ?assertEqual("\"bytes\"", to_string(Json)).

encode_empty_bytes_test() ->
    Json = encode_value(avro_primitive:bytes(<<>>)),
    ?assertEqual("\"\"", to_string(Json)).

encode_bytes_test() ->
    Json = encode_value(avro_primitive:bytes(<<0,1,100,255>>)),
    ?assertEqual("\"\\u0000\\u0001\\u0064\\u00ff\"", to_string(Json)).

encode_string_type_test() ->
    Json = encode_type(avro_primitive:string_type()),
    ?assertEqual("\"string\"", to_string(Json)).

encode_string_test() ->
    Json = encode_value(avro_primitive:string("Hello, Avro!")),
    ?assertEqual("\"Hello, Avro!\"", to_string(Json)).

encode_string_with_quoting_test() ->
    Json = encode_value(avro_primitive:string("\"\\")),
    ?assertEqual("\"\\\"\\\\\"", to_string(Json)).

encode_utf8_string_test() ->
    S = xmerl_ucs:to_utf8("Avro är populär"),
    Json = encode_value(avro_primitive:string(S)),
    ?assertEqual("\"Avro " ++ [195,164] ++ "r popul"++ [195,164] ++ "r\"",
                 to_string(Json)).

encode_record_type_test() ->
    Json = encode_type(sample_record_type()),
    ?assertEqual("{\"type\":\"record\","
                  "\"name\":\"SampleRecord\","
                  "\"fields\":["
                    "{\"name\":\"bool\","
                    "\"type\":\"boolean\","
                    "\"default\":false,"
                    "\"doc\":\"bool f\"},"
                    "{\"name\":\"int\","
                    "\"type\":\"int\","
                    "\"default\":0,"
                    "\"doc\":\"int f\"},"
                    "{\"name\":\"long\","
                    "\"type\":\"long\","
                    "\"default\":42,"
                    "\"doc\":\"long f\"},"
                    "{\"name\":\"float\","
                    "\"type\":\"float\","
                    "\"default\":3.14,"
                    "\"doc\":\"float f\"},"
                    "{\"name\":\"double\","
                    "\"type\":\"double\","
                    "\"default\":6.67221937,"
                    "\"doc\":\"double f\"},"
                    "{\"name\":\"bytes\","
                    "\"type\":\"bytes\","
                    "\"doc\":\"bytes f\"},"
                    "{\"name\":\"string\","
                    "\"type\":\"string\","
                    "\"default\":\"string value\","
                    "\"doc\":\"string f\"}],"
                  "\"namespace\":\"com.klarna.test.bix\","
                  "\"doc\":\"Record documentation\"}",
                 to_string(Json)).

encode_record_test() ->
    Json = encode_value(sample_record()),
    Expected = "{"
               "\"bool\":true,"
               "\"int\":100,"
               "\"long\":123456789123456789,"
               "\"float\":2.718281828,"
               "\"double\":3.14159265358,"
               "\"bytes\":\"\\u0062\\u0079\\u0074\\u0065\\u0073\\u0020\\u0076"
                           "\\u0061\\u006c\\u0075\\u0065\","
               "\"string\":\"string value\""
               "}",
    ?assertEqual(Expected, to_string(Json)).

encode_enum_type_test() ->
  EnumType =
    avro_enum:type("Enum",
                   ["A", "B", "C"],
                   [{namespace, "com.klarna.test.bix"}]),
  EnumTypeJson = encode_type(EnumType),
  ?assertEqual("{\"type\":\"enum\","
               "\"name\":\"Enum\","
               "\"symbols\":[\"A\",\"B\",\"C\"],"
               "\"namespace\":\"com.klarna.test.bix\"}",
               to_string(EnumTypeJson)).

encode_enum_test() ->
  EnumType =
    avro_enum:type("Enum",
                   ["A", "B", "C"],
                   [{namespace, "com.klarna.test.bix"}]),
  EnumValue = ?AVRO_VALUE(EnumType, "B"),
  EnumValueJson = encode_value(EnumValue),
  ?assertEqual("\"B\"", to_string(EnumValueJson)).

encode_union_type_test() ->
  UnionType = avro_union:type([ avro_primitive:string_type()
                              , avro_primitive:int_type()]),
  Json = encode_type(UnionType),
  ?assertEqual("[\"string\",\"int\"]", to_string(Json)).

encode_union_test() ->
  UnionType = avro_union:type([ avro_primitive:string_type()
                              , avro_primitive:int_type()]),
  Value = avro_union:new(UnionType, avro_primitive:int(10)),
  Json = encode_value(Value),
  ?assertEqual("{\"int\":10}", to_string(Json)).

encode_union_with_null_test() ->
  UnionType = avro_union:type([ avro_primitive:string_type()
                              , avro_primitive:null_type()]),
  Value = avro_union:new(UnionType, avro_primitive:null()),
  Json = encode_value(Value),
  ?assertEqual("null", to_string(Json)).

encode_array_type_test() ->
  Type = avro_array:type(avro_primitive:string_type()),
  Json = encode_type(Type),
  ?assertEqual("{\"type\":\"array\",\"items\":\"string\"}", to_string(Json)).

encode_array_test() ->
  Type = avro_array:type(avro_primitive:string_type()),
  Value = avro_array:new(Type,
                         [ avro_primitive:string("a")
                         , avro_primitive:string("b")]),
  Json = encode_value(Value),
  ?assertEqual("[\"a\",\"b\"]", to_string(Json)).

encode_map_type_test() ->
  MapType = avro_map:type(avro_union:type(
                            [avro_primitive:int_type(),
                             avro_primitive:null_type()])),
  Json = encode_type(MapType),
  ?assertEqual("{\"type\":\"map\",\"values\":[\"int\",\"null\"]}",
               to_string(Json)).

encode_map_test() ->
  MapType = avro_map:type(avro_union:type(
                            [avro_primitive:int_type(),
                             avro_primitive:null_type()])),
  MapValue = avro_map:new(MapType,
                          [{"v1", 1}, {"v2", null}, {"v3", 2}]),
  Json = encode_value(MapValue),
  ?assertEqual("{\"v3\":{\"int\":2},\"v1\":{\"int\":1},\"v2\":null}",
               to_string(Json)).

encode_fixed_type_test() ->
  Type = avro_fixed:type("FooBar", 2,
                         [ {namespace, "name.space"}
                         , {aliases, ["Alias1", "Alias2"]}
                         ]),
  Json = encode_type(Type),
  ?assertEqual("{\"type\":\"fixed\","
               "\"name\":\"FooBar\","
               "\"size\":2,"
               "\"namespace\":\"name.space\","
               "\"aliases\":[\"name.space.Alias1\",\"name.space.Alias2\"]}",
               to_string(Json)).

encode_fixed_value_test() ->
  Type = avro_fixed:type("FooBar", 2),
  Value = avro_fixed:new(Type, <<1,127>>),
  Json = encode_value(Value),
  ?assertEqual("\"\\u0001\\u007f\"", to_string(Json)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
