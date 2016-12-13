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
-module(avro_decoder_hooks_tests).

-include("avro_internal.hrl").
-include_lib("eunit/include/eunit.hrl").

debug_hook_test() ->
  LogFun = fun(IoData) -> io:put_chars(user, IoData) end,
  HistLen = 10,
  Hook = avro_decoder_hooks:binary_decoder_debug_trace(LogFun, HistLen),
  MyRecordType =
    avro_record:type(
      "MyRecord",
      [avro_record:define_field("f1", avro_primitive:int_type()),
       avro_record:define_field("f2", avro_primitive:string_type())],
      [{namespace, "my.com"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:get_encoder(Store, []),
  Term = [{"f1", 1},{"f2","my string"}],
  Bin = iolist_to_binary(Encoder("my.com.MyRecord", Term)),
  %% Mkae a corrupted binary to decode
  BadSize = size(Bin) - 1,
  CorruptedBin = <<Bin:BadSize/binary>>,
  Decoder = avro:get_decoder(Store, [{hook, Hook}]),
  ?assertException(
    _Class,
    {'$hook-raised', _},
    Decoder(CorruptedBin, "my.com.MyRecord")),
  ok.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
