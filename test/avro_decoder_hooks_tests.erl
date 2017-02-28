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

debug_hook_test_() ->
  [ {"json", fun() -> test_debug_hook(avro_json) end}
  , {"binary", fun() -> test_debug_hook(avro_binary) end}
  ].

test_debug_hook(Encoding) ->
  CodecOptions = [{encoding, Encoding}],
  LogFun = fun(IoData) -> io:put_chars(user, IoData) end,
  HistLen = 10,
  Hook = avro_decoder_hooks:print_debug_trace(LogFun, HistLen),
  MyRecordType =
    avro_record:type("MyRecord",
                     [ define_field("f1", avro_primitive:int_type())
                     , define_field("f2", avro_primitive:string_type())],
                     [{namespace, "com.example"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:make_encoder(Store, CodecOptions),
  Term = [{"f1", 1}, {"f2", "my-string"}],
  Bin = iolist_to_binary(Encoder("com.example.MyRecord", Term)),
  %% Mkae a corrupted binary to decode
  CorruptedBin = corrupt_encoded(Encoding, Bin),
  Decoder = avro:make_decoder(Store, [{hook, Hook} | CodecOptions]),
  ?assertException(_Class, {'$hook-raised', _},
                   Decoder("com.example.MyRecord", CorruptedBin)),
  ok.

%% @private
corrupt_encoded(avro_binary, Bin) ->
  %% for binary format, chopping off the last byte should corrupt the data
  %% because the last element is a string, the missing byte
  %% should violate the encoded string length check
  BadSize = size(Bin) - 1,
  <<Bin:BadSize/binary>>;
corrupt_encoded(avro_json, Bin) ->
  %% for json, replace the last string with an integer
  %% to violate the type check
  binary:replace(Bin, <<"\"my-string\"">>, <<"42">>).

%% @private
define_field(Name, Type) -> avro_record:define_field(Name, Type).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
