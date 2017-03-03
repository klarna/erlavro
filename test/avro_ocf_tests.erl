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
-module(avro_ocf_tests).

-include("avro_internal.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(header, { magic
                , meta
                , sync
                }).

interop_test() ->
  PrivDir = priv_dir(),
  InteropOcfFile = filename:join([PrivDir, "interop.ocf"]),
  {Header, Schema, Objects} = avro_ocf:decode_file(InteropOcfFile),
  SchemaStore = avro_ocf:init_schema_store(Schema),
  MyFile = filename:join([PrivDir, "interop.ocf.test"]),
  %% re-use the old header
  ok = avro_ocf:write_header(MyFile, Header),
  {ok, Fd} = file:open(MyFile, [write, append]),
  try
    ok = avro_ocf:append_file(Fd, Header, SchemaStore, Schema, Objects)
  after
    file:close(Fd)
  end,
  {Header1, Schema1, Objects1} = avro_ocf:decode_file(MyFile),
  ?assertEqual(Header#header{meta = []}, Header1#header{meta = []}),
  ?assertEqual(lists:keysort(1, Header#header.meta),
               lists:keysort(1, Header1#header.meta)),
  ?assertEqual(Schema, Schema1),
  ?assertEqual(Objects, Objects1).

write_file_test() ->
  OcfFile = filename:join([priv_dir(), "my.ocf.test"]),
  Store = undefined, %% should not require lookup
  Fields = [ avro_record:define_field("f1", int, [])
           , avro_record:define_field("f2", string, [])
           ],
  Type = avro_record:type("rec", Fields, [{namespace, "my.ocf.test"}]),
  Obj = [{"f1", 1}, {"f2", "foo"}],
  ok = avro_ocf:write_file(OcfFile, Store, Type, [Obj]),
  {_Header, Type, Objs} = avro_ocf:decode_file(OcfFile),
  ?assertEqual([[{<<"f1">>, 1}, {<<"f2">>, <<"foo">>}]], Objs).

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
