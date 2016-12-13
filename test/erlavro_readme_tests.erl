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
-module(erlavro_readme_tests).

-include("avro_internal.hrl").
-include_lib("eunit/include/eunit.hrl").

load_schmea_test() ->
  SchemaFilename = filename:join(priv_dir(), "interop.avsc"),
  OcfFilename = filename:join(priv_dir(), "interop.ocf"),
  Store = avro_schema_store:new([], [SchemaFilename]),
  Encoder = avro:get_encoder(Store, []),
  Decoder = avro:get_decoder(Store, []),
  Term = hd(element(3, avro_ocf:decode_file(OcfFilename))),
  Encoded = iolist_to_binary(Encoder(Term, "org.apache.avro.Interop")),
  Term = Decoder(Encoded, "org.apache.avro.Interop"),
  ok.

binary_encode_decode_test() ->
  MyRecordType =
    avro_record:type(
      "MyRecord",
      [avro_record:define_field("f1", avro_primitive:int_type()),
       avro_record:define_field("f2", avro_primitive:string_type())],
      [{namespace, "my.com"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:get_encoder(Store, []),
  Decoder = avro:get_decoder(Store, []),
  Term = [{"f1", 1},{"f2","my string"}],
  Bin = Encoder(Term, "my.com.MyRecord"),
  Term = Decoder(Bin, "my.com.MyRecord"),
  ok.

json_encode_decode_test() ->
  MyRecordType =
    avro_record:type(
      "MyRecord",
      [avro_record:define_field("f1", avro_primitive:int_type()),
       avro_record:define_field("f2", avro_primitive:string_type())],
      [{namespace, "my.com"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:get_encoder(Store, [{encoding, avro_json}]),
  Decoder = avro:get_decoder(Store, [{encoding, avro_json}]),
  Term = [{"f1", 1},{"f2", "my string"}],
  JSON = Encoder(Term, "my.com.MyRecord"),
  Term = Decoder(JSON, "my.com.MyRecord"),
  io:put_chars(user, JSON),
  ok.

encode_wrapped_test_() ->
  [ {"json encoder", fun() -> encode_wrapped([{encoding, avro_json}]) end}
  , {"binary encoder", fun() -> encode_wrapped([]) end}
  ].

encode_wrapped(CodecOptions) ->
  NullableInt = avro_union:type([avro_primitive:null_type(),
                                 avro_primitive:int_type()]),
  MyRecordType1 =
    avro_record:type(
      "MyRecord1",
      [avro_record:define_field("f1", NullableInt),
       avro_record:define_field("f2", avro_primitive:string_type())],
      [{namespace, "my.com"}]),
  MyRecordType2 =
    avro_record:type(
      "MyRecord2",
      [avro_record:define_field("f1", avro_primitive:string_type()),
       avro_record:define_field("f2", NullableInt)],
      [{namespace, "my.com"}]),
  MyUnion = avro_union:type([MyRecordType1, MyRecordType2]),
  MyArray = avro_array:type(MyUnion),
  Lkup = fun(_) -> erlang:error("not expecting type lookup because "
                                "all types are fully constructed. "
                                "i.e. no name references") end,
  %% Encode Records with type info wrapped
  %% so they can be used as a drop-in part of wrapper object
  WrappedEncoder = avro:get_encoder(Lkup, [wrapped | CodecOptions]),
  T1 = [{"f1", null}, {"f2", "str1"}],
  T2 = [{"f1", "str2"}, {"f2", 2}],
  %% Encode the records with type info wrapped
  R1 = WrappedEncoder(T1, MyRecordType1),
  R2 = WrappedEncoder(T2, MyRecordType2),
  %% Tag the union values for better encoding performance
  U1 = {"my.com.MyRecord1", R1},
  U2 = {"my.com.MyRecord2", R2},
  %% This encoder returns iodata result without type info wrapped
  BinaryEncoder = avro:get_encoder(Lkup, CodecOptions),
  %% Construct the array from encoded elements
  Bin = iolist_to_binary(BinaryEncoder([U1, U2], MyArray)),
  %% Tag the decoded values
  Hook = avro_decoder_hooks:tag_unions_fun(),
  Decoder = avro:get_decoder(Lkup, [{hook, Hook} | CodecOptions]),
  [ {"my.com.MyRecord1", [{"f1", null}, {"f2", "str1"}]}
  , {"my.com.MyRecord2", [{"f1", "str2"}, {"f2", {"int", 2}}]}
  ] = Decoder(Bin, MyArray),
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
