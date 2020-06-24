%% coding: latin-1
%%%-------------------------------------------------------------------
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
%%%-------------------------------------------------------------------
-module(avro_ocf_tests).

-include("avro_internal.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(header, { magic
                , meta
                , sync
                }).

interop_test() ->
  InteropOcfFile = test_data("interop.ocf"),
  {Header, Schema, Objects} = avro_ocf:decode_file(InteropOcfFile),
  Lkup = avro:make_lkup_fun(Schema),
  MyFile = test_data("interop.ocf.test"),
  {ok, Fd} = file:open(MyFile, [write]),
  try
    ok = avro_ocf:write_header(Fd, Header),
    ok = avro_ocf:append_file(Fd, Header, Lkup, Schema, Objects)
  after
    file:close(Fd)
  end,
  {Header1, Schema1, Objects1} = avro_ocf:decode_file(MyFile),
  ?assertEqual(Header#header{meta = []}, Header1#header{meta = []}),
  ?assertEqual(lists:keysort(1, Header#header.meta),
               lists:keysort(1, Header1#header.meta)),
  ?assertEqual(Schema, Schema1),
  ?assertEqual(Objects, Objects1).

decode_deflate_file_test() ->
  InteropOcfFile = test_data("interop_deflate.ocf"),
  {Header, _Schema, Objects} = avro_ocf:decode_file(InteropOcfFile),
  ?assertEqual(<<"deflate">>,
               proplists:get_value(<<"avro.codec">>, Header#header.meta)),
  ?assertEqual(<<"hey">>,
               proplists:get_value(<<"stringField">>, hd(Objects))).

decode_no_codec_file_test() ->
  InteropOcfFile = test_data("interop_no_codec.ocf"),
  {Header, _Schema, Objects} = avro_ocf:decode_file(InteropOcfFile),
  ?assertEqual(undefined,
               proplists:get_value(<<"avro.codec">>, Header#header.meta)),
  ?assertEqual(<<"hey">>,
               proplists:get_value(<<"stringField">>, hd(Objects))).

write_file_test() ->
  OcfFile = test_data("my.ocf.test"),
  Store = undefined, %% should not require lookup
  Fields = [ avro_record:define_field("f1", int, [])
           , avro_record:define_field("f2", string, [])
           ],
  Type = avro_record:type("rec", Fields, [{namespace, "my.ocf.test"}]),
  Obj = [{"f1", 1}, {"f2", "foo"}],
  ok = avro_ocf:write_file(OcfFile, Store, Type, [Obj]),
  {_Header, Type, Objs} = avro_ocf:decode_file(OcfFile),
  ?assertEqual([[{<<"f1">>, 1}, {<<"f2">>, <<"foo">>}]], Objs).

write_deflate_file_test() ->
  OcfFile = test_data("deflate.ocf.test"),
  Store = undefined, %% should not require lookup
  Fields = [ avro_record:define_field("f1", int, [])
           , avro_record:define_field("f2", string, [])
           ],
  Type = avro_record:type("rec", Fields, [{namespace, "defalate.ocf.test"}]),
  Obj = [{"f1", 1}, {"f2", "foo"}],
  Meta = [{<<"avro.codec">>, <<"deflate">>}],
  ok = avro_ocf:write_file(OcfFile, Store, Type, [Obj], Meta),
  {_Header, Type, Objs} = avro_ocf:decode_file(OcfFile),
  ?assertEqual([[{<<"f1">>, 1}, {<<"f2">>, <<"foo">>}]], Objs).

root_level_union_test() ->
  OcfFile = test_data("union.ocf.test"),
  Store = undefined, %% should not require lookup
  Fields = [ avro_record:define_field("f1", int, [])
           , avro_record:define_field("f2", string, [])
           ],
  Type1 = avro_record:type("rec", Fields, [{namespace, "my.ocf.test"}]),
  Type2 = avro_primitive:type(int, []),
  Type  = avro_union:type([Type1, Type2]),
  Obj1 = [{"f1", 1}, {"f2", "foo"}],
  Obj2 = 42,
  ok = avro_ocf:write_file(OcfFile, Store, Type, [Obj1, Obj2],
                           [{"my-meta", <<0>>}]),
  {_Header, TypeDecoded, Objs} = avro_ocf:decode_file(OcfFile),
  ?assertEqual(Type, TypeDecoded),
  ?assertEqual([[{<<"f1">>, 1}, {<<"f2">>, <<"foo">>}], 42], Objs).

meta_test() ->
  ?assertError({reserved_meta_key, "avro.x"},
               avro_ocf:make_header(ignore, [{"avro.x", ignore}])),
  ?assertError({bad_meta_value, atom},
               avro_ocf:make_header(ignore, [{"x", atom}])),
  ?assertError({bad_codec, <<"lzw">>},
               avro_ocf:make_header(ignore, [{"avro.codec", <<"lzw">>}])),
  _ = avro_ocf:make_header(<<"long">>),
  _ = avro_ocf:make_header(<<"int">>, [{"a", <<"b">>}]),
  _ = avro_ocf:make_header(<<"int">>, [{<<"avro.codec">>, <<"null">>}]),
  ok.

make_ocf_test() ->
  L = lists:seq(1, 1000),
  Type = avro_primitive:type(long, []),
  Header = avro_ocf:make_header(Type),
  Encoder = avro:make_simple_encoder(Type, []),
  Objects = [Encoder(I) || I <- L],
  Bin = iolist_to_binary(avro_ocf:make_ocf(Header, Objects)),
  {_, _, DecodedL} = avro_ocf:decode_binary(Bin),
  ?assertEqual(L, DecodedL).

test_data(FileName) ->
  filename:join([code:lib_dir(erlavro, test), "data", FileName]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
