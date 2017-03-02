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
  Union = avro_union:type([avro_primitive:null_type(),
                           avro_primitive:int_type()]),
  MyRecordType =
    avro_record:type("MyRecord",
                     [ define_field("f1", avro_primitive:int_type())
                     , define_field("f2", Union)
                     , define_field("f3", avro_primitive:string_type())
                     ],
                     [{namespace, "com.example"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:make_encoder(Store, CodecOptions),
  Term = [{"f1", 1}, {"f3", "my-string"}, {"f2", 32}],
  Bin = iolist_to_binary(Encoder("com.example.MyRecord", Term)),
  %% Mkae a corrupted binary to decode
  CorruptedBin = corrupt_encoded(Encoding, Bin),
  Decoder = avro:make_decoder(Store, [{hook, Hook} | CodecOptions]),
  ?assertException(_Class, {'$hook-raised', _},
                   Decoder("com.example.MyRecord", CorruptedBin)),
  ok.

tag_unions_test() ->
  CodecOptions = [{encoding, avro_binary}],
  Hook = avro_decoder_hooks:tag_unions(),
  Fixed = avro_fixed:type("myfixed", 2),
  Map = avro_map:type(avro_primitive:int_type()),
  Array = avro_array:type(avro_primitive:int_type()),
  Field = avro_record:define_field("rf1", avro_primitive:int_type()),
  Record = avro_record:type("MySubRec", [Field]),
  Union = avro_union:type([ avro_primitive:null_type()
                          , Fixed
                          , Map
                          , Array
                          , Record
                          ]),
  ArrayOfUnion = avro_array:type(Union),
  MyRecordType =
    avro_record:type("MyRecord",
                     [ define_field("f1", Array)
                     , define_field("f2", ArrayOfUnion)
                     ],
                     [{namespace, "com.example"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:make_encoder(Store, CodecOptions),
  Input = [ {"f1", [1, 2, 3]}
          , {"f2", [ null                                    %% null
                   , <<"32">>                                %% fixed
                   , {"com.example.MySubRec", [{"rf1", 42}]} %% record
                   , {"com.example.MySubRec", [{"rf1", 43}]} %% record
                   , [4, 5, 6]                               %% array
                   , [{"k", 1}]                              %% map
                   ]}
          ],
  Bin = iolist_to_binary(Encoder("com.example.MyRecord", Input)),
  Decoder = avro:make_decoder(Store, [{hook, Hook} | CodecOptions]),
  ?assertEqual(
     [ {<<"f1">>, [1, 2, 3]}
     , {<<"f2">>, [ null
                  , {<<"com.example.myfixed">>, <<"32">>}
                  , {<<"com.example.MySubRec">>, [{<<"rf1">>, 42}]}
                  , {<<"com.example.MySubRec">>, [{<<"rf1">>, 43}]}
                  , [4, 5, 6]
                  , [{<<"k">>, 1}]
                  ]}
     ], Decoder("com.example.MyRecord", Bin)).

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
