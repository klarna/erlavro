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
%%%-------------------------------------------------------------------
-module(avro_tests).

-include("avro_internal.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PRIMITIVE_VALUE(Name, Value),
        #avro_value{ type = #avro_primitive_type{name = Name}
                   , data = Value
                   }).

get_test_type(Name, Namespace) ->
  avro_fixed:type(Name, 16, [{namespace, Namespace}]).

split_type_name_test() ->
  ?assertEqual({<<"tname">>, <<"">>},
               avro:split_type_name("tname", "")),
  ?assertEqual({<<"tname">>, <<"name.space">>},
               avro:split_type_name("tname", "name.space")),
  ?assertEqual({<<"tname">>, <<"name.space">>},
               avro:split_type_name("name.space.tname", "name1.space1")).

get_type_fullname_test() ->
  ?assertEqual(<<"name.space.tname">>,
               avro:get_type_fullname(get_test_type("tname", "name.space"))),
  ?assertEqual(<<"int">>,
               avro:get_type_fullname(avro_primitive:int_type())).

cast_primitive_test() ->
  ?assertEqual({ok, ?PRIMITIVE_VALUE(?AVRO_STRING, <<"abc">>)},
               avro:cast(avro_primitive:string_type(), "abc")),
  ?assertEqual({ok, ?PRIMITIVE_VALUE(?AVRO_INT, 1)}, avro:cast("int", 1)),
  ?assertEqual({ok, ?PRIMITIVE_VALUE(?AVRO_LONG, 1)}, avro:cast("long", 1)).

get_aliases_test() ->
  ?assertEqual([], avro:get_aliases(avro_primitive:null_type())),
  ?assertEqual([], avro:get_aliases(avro_union:type([int]))),
  ?assertEqual([], avro:get_aliases(avro_array:type(string))),
  ?assertEqual([], avro:get_aliases(avro_map:type(long))).

get_type_namespace_test() ->
  ?assertEqual(?NS_GLOBAL, avro:get_type_namespace(avro_primitive:null_type())),
  ?assertEqual(?NS_GLOBAL, avro:get_type_namespace(avro_union:type([int]))),
  ?assertEqual(?NS_GLOBAL, avro:get_type_namespace(avro_array:type(int))),
  ?assertEqual(?NS_GLOBAL, avro:get_type_namespace(avro_map:type(int))).

get_type_name_test() ->
  lists:foreach(fun(Name) ->
                    ?assertEqual(Name, avro:get_type_name(avro:name2type(Name)))
                end, [?AVRO_BOOLEAN, ?AVRO_BYTES, ?AVRO_DOUBLE,
                      ?AVRO_FLOAT, ?AVRO_LONG, ?AVRO_INT,
                      ?AVRO_STRING, ?AVRO_NULL]),
  ?assertEqual(?AVRO_UNION, avro:get_type_name(avro_union:type([int]))),
  ?assertEqual(?AVRO_ARRAY, avro:get_type_name(avro_array:type(int))),
  ?assertEqual(?AVRO_MAP,   avro:get_type_name(avro_map:type(int))).

resolve_fullname_test() ->
  %% only to expect no exception when there is no namespace
  %% given for records
  MyEnum = avro_enum:type("ab", ["a", "b"]),
  MyFixed = avro_fixed:type("fix", 2),
  MyRecordType =
    avro_record:type(
      <<"MyRecord">>,
      [ avro_record:define_field(f1, MyEnum)
      , avro_record:define_field(f2, MyFixed)
      ], []),
  _ = avro_record:type("wrapper",
                       [avro_record:define_field("x", MyRecordType)]),
  ok.

encode_wrapped_test() ->
  MyRecordType =
    avro_record:type(
      <<"MyRecord">>,
      [avro_record:define_field(f1, int),
       avro_record:define_field(f2, string)],
      [{namespace, 'com.example'}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Decoder = avro:make_decoder(Store, []),
  Term = [{<<"f1">>, 1}, {<<"f2">>, <<"my string">>}],
  ?AVRO_VALUE(_, {binary, Bin}) =
    avro:encode_wrapped(Store, "com.example.MyRecord", Term, avro_binary),
  [{<<"f1">>, 1}, {<<"f2">>, <<"my string">>}] =
    Decoder("com.example.MyRecord", Bin),
  ok.

readme_load_schmea_test() ->
  SchemaFilename = filename:join(priv_dir(), "interop.avsc"),
  OcfFilename = filename:join(priv_dir(), "interop.ocf"),
  Store = avro_schema_store:new([], [SchemaFilename]),
  Encoder = avro:make_encoder(Store, []),
  Decoder = avro:make_decoder(Store, []),
  Term = hd(element(3, avro_ocf:decode_file(OcfFilename))),
  Encoded = iolist_to_binary(Encoder("org.apache.avro.Interop", Term)),
  Term = Decoder("org.apache.avro.Interop", Encoded),
  ok.

readme_binary_encode_decode_test() ->
  MyRecordType =
    avro_record:type(
      <<"MyRecord">>,
      [avro_record:define_field(f1, int),
       avro_record:define_field(f2, string)],
      [{namespace, 'com.example'}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:make_encoder(Store, []),
  Decoder = avro:make_decoder(Store, []),
  Term = [{<<"f1">>, 1}, {<<"f2">>, <<"my string">>}],
  Bin = Encoder("com.example.MyRecord", Term),
  [{<<"f1">>, 1}, {<<"f2">>, <<"my string">>}] =
    Decoder("com.example.MyRecord", Bin),
  ok.

readme_json_encode_decode_test() ->
  MyRecordType =
    avro_record:type(
      "MyRecord",
      [avro_record:define_field("f1", int),
       avro_record:define_field("f2", string)],
      [{namespace, "com.example"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:make_encoder(Store, [{encoding, avro_json}]),
  Decoder = avro:make_decoder(Store, [{encoding, avro_json}]),
  Term = [{<<"f1">>, 1}, {<<"f2">>, <<"my string">>}],
  JSON = Encoder("com.example.MyRecord", Term),
  Term = Decoder("com.example.MyRecord", JSON),
  ok.

readme_encode_wrapped_test_() ->
  [ {"json encoder", fun() -> encode_wrapped([{encoding, avro_json}]) end}
  , {"binary encoder", fun() -> encode_wrapped([]) end}
  ].

primitive_cast_error_test() ->
  IntType = avro_primitive:int_type(),
  ?assertException(error, {type_mismatch, IntType, "foo"},
                   avro_primitive:int("foo")).

get_custom_props_test() ->
  Date = avro_primitive:type(int, [{logicalType, "Date"}, {"p", "v"}]),
  Union = avro_union:type([null, int]),
  Array = avro_array:type(int, [{"p", "v"}, {tag, true}]),
  Enum = avro_enum:type("abc", ["a", "b", "c"], [{"p", "v"}]),
  Fixed  = avro_fixed:type("twobytes", 2, [{key, value}]),
  Map = avro_map:type(int, [{key, value}]),
  MyRecordType =
    avro_record:type(
      <<"MyRecord">>,
      [ avro_record:define_field(k1, int)
      , avro_record:define_field(k2, Union)
      , avro_record:define_field(date, Date)
      , avro_record:define_field(array, Array)
      , avro_record:define_field(enum, Enum)
      , avro_record:define_field(fixed, Fixed)
      , avro_record:define_field(map, Map)
      ],
      [ {namespace, 'com.example'}
      , {key_fields, [k1, k2]}
      ]),
  ?assertEqual([{<<"key_fields">>, [<<"k1">>, <<"k2">>]}],
               avro:get_custom_props(MyRecordType, store)),
  FieldTypeProps =
    fun(Fn) ->
      FieldType = avro_record:get_field_type(Fn, MyRecordType),
      avro:get_custom_props(FieldType)
    end,
  ?assertEqual([], FieldTypeProps(k1)),
  ?assertEqual([], FieldTypeProps(k2)),
  ?assertEqual([{<<"logicalType">>, <<"Date">>},
                {<<"p">>, <<"v">>}],
               FieldTypeProps(date)),
  ?assertEqual([{<<"p">>, <<"v">>}, {<<"tag">>, true}], FieldTypeProps(array)),
  ?assertEqual([{<<"p">>, <<"v">>}], FieldTypeProps(enum)),
  ?assertEqual([{<<"key">>, <<"value">>}], FieldTypeProps(fixed)),
  ?assertEqual([{<<"key">>, <<"value">>}], FieldTypeProps(map)),
  JSON = avro_json_encoder:encode_schema(MyRecordType),
  ?assertEqual(MyRecordType,
               avro_json_decoder:decode_schema(iolist_to_binary(JSON))),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  ?assertEqual([{<<"key_fields">>, [<<"k1">>, <<"k2">>]}],
               avro:get_custom_props("com.example.MyRecord", Store)),
  ok.

encode_wrapped(CodecOptions) ->
  NullableInt = avro_union:type([null, int]),
  MyRecordType1 =
    avro_record:type(
      "MyRecord1",
      [avro_record:define_field("f1", NullableInt),
       avro_record:define_field("f2", string)],
      [{namespace, "com.example"}]),
  MyRecordType2 =
    avro_record:type(
      "MyRecord2",
      [avro_record:define_field("f1", string),
       avro_record:define_field("f2", NullableInt)],
      [{namespace, "com.example"}]),
  MyUnion = avro_union:type([MyRecordType1, MyRecordType2]),
  MyArray = avro_array:type(MyUnion),
  Lkup = fun(_) -> erlang:error("not expecting type lookup because "
                                "all types are fully constructed. "
                                "i.e. no name references") end,
  %% Encode Records with type info wrapped
  %% so they can be used as a drop-in part of wrapper object
  WrappedEncoder = avro:make_encoder(Lkup, [wrapped | CodecOptions]),
  T1 = [{"f1", null}, {"f2", <<"str1">>}],
  T2 = [{"f1", <<"str2">>}, {"f2", 2}],
  %% Encode the records with type info wrapped
  R1 = WrappedEncoder(MyRecordType1, T1),
  R2 = WrappedEncoder(MyRecordType2, T2),
  %% Tag the union values for better encoding performance
  U1 = {"com.example.MyRecord1", R1},
  U2 = {"com.example.MyRecord2", R2},
  %% This encoder returns iodata result without type info wrapped
  BinaryEncoder = avro:make_encoder(Lkup, CodecOptions),
  %% Construct the array from encoded elements
  Bin = iolist_to_binary(BinaryEncoder(MyArray, [U1, U2])),
  %% Tag the decoded values
  Hook = avro_decoder_hooks:tag_unions(),
  Decoder = avro:make_decoder(Lkup, [{hook, Hook} | CodecOptions]),
  [ {<<"com.example.MyRecord1">>, [{<<"f1">>, null}, {<<"f2">>, <<"str1">>}]}
  , {<<"com.example.MyRecord2">>, [{<<"f1">>, <<"str2">>}, {<<"f2">>, 2}]}
  ] = Decoder(MyArray, Bin),
  ok.

encode_wrapped_unnamed_test() ->
  Rec1 = avro_record:type("rec1", [avro_record:define_field("f1", int)]),
  Rec2 = avro_record:type("rec2", [avro_record:define_field("f1", int)]),
  Lkup = fun(<<"rec1">>) -> Rec1;
            (<<"rec2">>) -> Rec2 end,
  Union = avro_union:type(["rec1", "rec2"]),
  EncRec2JSON = avro:encode_wrapped(Lkup, Rec2, [{"f1", 1}], avro_json),
  EncJSON = avro:encode_wrapped(Lkup, Union, EncRec2JSON, avro_json),
  ExpectedJSON = <<"{\"rec2\":{\"f1\":1}}">>,
  ?assertMatch(?AVRO_ENCODED_VALUE_JSON(_, ExpectedJSON),
               EncJSON),
  EncRec2Bin = avro:encode_wrapped(Lkup, Rec2, [{"f1", 1}], avro_binary),
  EncRec2BinTagged = {"rec2", EncRec2Bin},
  EncBin1 = avro:encode_wrapped(Lkup, Union, EncRec2Bin, avro_binary),
  EncBin2 = avro:encode_wrapped(Lkup, Union, EncRec2BinTagged, avro_binary),
  ?assertEqual(EncBin1, EncBin2),
  ?assertMatch(?AVRO_ENCODED_VALUE_BINARY(_, _),
               EncBin1),
  ok.

%% one may use encoded-wrapped values to construct parent objects
wrapped_union_cast_test() ->
  Rec1 = avro_record:type("rec1", [avro_record:define_field("f1", int)]),
  Rec2 = avro_record:type("rec2", [avro_record:define_field("f1", int)]),
  Lkup = fun(<<"rec1">>) -> Rec1;
            (<<"rec2">>) -> Rec2 end,
  Union = avro_union:type(["rec1", "rec2"]),
  Rec1Val = avro:encode_wrapped(Lkup, "rec2", [{"f1", 1}], avro_binary),
  {ok, UnionVal} = avro:cast(Union, Rec1Val),
  Bin1 = avro_binary_encoder:encode_value(UnionVal),
  Bin2 = avro:encode(Lkup, Union, Rec1Val, avro_binary),
  ?assertEqual(Bin1, Bin2),
  Decoded = avro:decode(avro_binary, Bin1, Union, Lkup, ?DEFAULT_DECODER_HOOK),
  ?assertEqual([{<<"f1">>, 1}], Decoded).

wrapped_map_cast_test() ->
  Map = avro_map:type(int),
  Wrapped = avro:encode_wrapped(undefined, Map, [{"key", 33}], avro_binary),
  {ok, Cast} = avro:cast(Map, Wrapped),
  ?assertEqual(Wrapped, Cast).

array_of_union_cast_test() ->
  Rec1 = avro_record:type("rec1", [avro_record:define_field("f", int)]),
  Rec2 = avro_record:type("rec2", [avro_record:define_field("f", int)]),
  Lkup = fun(<<"rec1">>) -> Rec1;
            (<<"rec2">>) -> Rec2 end,
  Union = avro_union:type(["rec1", "rec2"]),
  Array = avro_array:type(Union),
  Rec1Val = avro:encode_wrapped(Lkup, Union, {"rec1", [{"f", 1}]}, avro_binary),
  Rec2Val = avro:encode_wrapped(Lkup, Union, {"rec2", [{"f", 1}]}, avro_binary),
  {ok, #avro_value{ type = Array
                  , data = [ #avro_value{type = Union, data = B1}
                           , #avro_value{type = Union, data = B2}
                           ]}} = avro:cast(Array, [Rec1Val, Rec2Val]),
  ?assertEqual({binary, <<0,2>>}, B1),
  ?assertEqual({binary, <<2,2>>}, B2),
  Int = avro_primitive:type(int, []),
  EncodedInt = avro:encode_wrapped(Lkup, Int, 1, avro_binary),
  ?assertMatch({error, {unknown_member, Union, <<"int">>}},
               avro:cast(Array, [EncodedInt])),
  ok.

%% @private
priv_dir() ->
  case code:priv_dir(erlavro) of
    {error, bad_name} ->
      %% application is not loaded, try dirty way
      case filelib:is_dir(filename:join(["..", priv])) of
        true -> filename:join(["..", priv]);
        _    -> "./priv"
      end;
    Dir ->
      Dir
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
