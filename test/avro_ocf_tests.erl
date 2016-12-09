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
%%% @doc Avro Json decoder
%%% @end
%%%-------------------------------------------------------------------
-module(avro_ocf_tests).
-author("tihon").

-include("avro_internal.hrl").
-include_lib("eunit/include/eunit.hrl").

interop_test() ->
  PrivDir = priv_dir(),
  InteropOcfFile = filename:join([PrivDir, "interop.ocf"]),
  {Header, Schema, Objects} = avro_ocf:decode_file(InteropOcfFile),
  SchemaStore = avro_ocf:init_schema_store(Schema),
  MyFile = filename:join([PrivDir, "interop.ocf.test"]),
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

priv_dir() ->
  case filelib:is_dir(filename:join(["..", priv])) of
    true -> filename:join(["..", priv]);
    _    -> "./priv"
  end.