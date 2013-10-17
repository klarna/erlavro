%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%% Writes Avro schemas and values to files using JSON format and
%%% mochijson3 as a writer.
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

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

encode_type(Type) ->
  to_json(do_encode_type(Type)).

encode_value(Value) ->
  to_json(do_encode_value(Value)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_encoder() ->
  mochijson3:encoder([{utf8, false}]).

to_json(Data) ->
  (get_encoder())(Data).

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
  #avro_field{ name    = Name
             , doc     = Doc
             , type    = Type
             , default = Default
             , order   = Order
             , aliases = Aliases} = Field,
  { struct
  , [ {name, encode_string(Name)}
    , {type, do_encode_type(Type)}
    ]
    ++ optional_field(default, Default, undefined, fun encode_value/1)
    ++ optional_field(doc,     Doc,     "",        fun encode_string/1)
    ++ optional_field(order,   Order,   ascending, fun encode_order/1)
    ++ optional_field(aliases, Aliases, [],        fun encode_aliases/1)
  }.

encode_string(String) ->
  erlang:list_to_binary(String).

encode_utf8_string(String) ->
  encode_string(xmerl_ucs:to_utf8(String)).

encode_integer(Int) when is_integer(Int) ->
  Int.

encode_aliases(Aliases) ->
  lists:map(fun encode_string/1, Aliases).

encode_order(ascending)  -> <<"ascending">>;
encode_order(descending) -> <<"descending">>;
encode_order(ignore)     -> <<"ignore">>.


do_encode_value(Value) when ?AVRO_IS_NULL_VALUE(Value) ->
  null;

do_encode_value(Value) when ?AVRO_IS_INT_VALUE(Value) ->
  ?AVRO_VALUE_DATA(Value);

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
  encode_utf8_string(?AVRO_VALUE_DATA(Value));

do_encode_value(Record) when ?AVRO_IS_RECORD_VALUE(Record) ->
  FieldsAndValues = avro_record:to_list(Record),
  { struct
  , lists:map(fun encode_record_field_with_value/1, FieldsAndValues)
  };

do_encode_value(Enum) when ?AVRO_IS_ENUM_VALUE(Enum) ->
  %% I suppose that enum symbols can contain any characters,
  %% since the specification doesn't say anything about this.
  encode_utf8_string(?AVRO_VALUE_DATA(Enum));

do_encode_value(Array) when ?AVRO_IS_ARRAY_VALUE(Array) ->
  lists:map(fun do_encode_value/1, ?AVRO_VALUE_DATA(Array));

%% TODO: map

do_encode_value(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  Data = ?AVRO_VALUE_DATA(Union),
  { struct
  , [{avro:get_fullname(?AVRO_VALUE_TYPE(Data), ""), %% TODO: enclosing ns
      do_encode_value(?AVRO_VALUE_DATA(Data))}]
  };

do_encode_value(Fixed) when ?AVRO_IS_FIXED_VALUE(Fixed) ->
  %% mochijson3 doesn't support Avro style of encoding binaries
  {json, encode_binary(?AVRO_VALUE_DATA(Fixed))}.



encode_record_field_with_value({FieldName, Value}) ->
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
      "com.klarna.test.bix",
      "Record documentation",
      [ avro_record:field("bool",   avro_primitive:boolean_type(), "bool f")
      , avro_record:field("int",    avro_primitive:int_type(),     "int f")
      , avro_record:field("long",   avro_primitive:long_type(),    "long f")
      , avro_record:field("float",  avro_primitive:float_type(),   "float f")
      , avro_record:field("double", avro_primitive:double_type(),  "double f")
      , avro_record:field("bytes",  avro_primitive:bytes_type(),   "bytes f")
      , avro_record:field("string", avro_primitive:string_type(),  "string f")
      %% , avro_record:field("record", avro_primitive:int_type(), "record f")
      %% , avro_record:field("enum", avro_primitive:int_type(),   "i f")
      %% , avro_record:field("union", avro_primitive:int_type(), "union f")
      %% , avro_record:field("fixed", avro_primitive:int_type(), "fixed f")
      ]).

sample_record() ->
    Rec = avro_record:new(sample_record_type()),
    avro_record:set(
      [ {"string", avro_primitive:string("string value")}
      , {"double", avro_primitive:double(3.14159265358)}
      , {"long",   avro_primitive:long(123456789123456789)}
      , {"bool",   avro_primitive:boolean(true)}
      , {"int",    avro_primitive:int(100)}
      , {"float",  avro_primitive:float(2.718281828)}
      , {"bytes",  avro_primitive:bytes(<<"bytes value">>)}
      ],
      Rec).

encode_null_test() ->
    Json = encode_value(avro_primitive:null()),
    ?assertEqual("null", to_string(Json)).

encode_boolean_test() ->
    JsonTrue = encode_value(avro_primitive:boolean(true)),
    JsonFalse = encode_value(avro_primitive:boolean(false)),
    ?assertEqual("true", to_string(JsonTrue)),
    ?assertEqual("false", to_string(JsonFalse)).

encode_int_test() ->
    Json = encode_value(avro_primitive:int(1)),
    ?assertEqual("1", to_string(Json)).

encode_long_test() ->
    Json = encode_value(avro_primitive:long(12345678901)),
    ?assertEqual("12345678901", to_string(Json)).

encode_float_test() ->
    Json = encode_value(avro_primitive:float(3.14159265358)),
    ?assertEqual("3.14159265358", to_string(Json)).

encode_double_test() ->
    Json = encode_value(avro_primitive:double(3.14159265358)),
    ?assertEqual("3.14159265358", to_string(Json)).

encode_empty_bytes_test() ->
    Json = encode_value(avro_primitive:bytes(<<>>)),
    ?assertEqual("\"\"", to_string(Json)).

encode_bytes_test() ->
    Json = encode_value(avro_primitive:bytes(<<0,1,100,255>>)),
    ?assertEqual("\"\\u0000\\u0001\\u0064\\u00ff\"", to_string(Json)).

encode_string_test() ->
    Json = encode_value(avro_primitive:string("Hello, Avro!")),
    ?assertEqual("\"Hello, Avro!\"", to_string(Json)).

encode_string_with_quoting_test() ->
    Json = encode_value(avro_primitive:string("\"\\")),
    ?assertEqual("\"\\\"\\\\\"", to_string(Json)).

encode_utf8_string_test() ->
    S = "Avro är popular",
    Json = encode_value(avro_primitive:string(S)),
    ?assertEqual("\"Avro \\u00e4r popular\"", to_string(Json)).

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

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
