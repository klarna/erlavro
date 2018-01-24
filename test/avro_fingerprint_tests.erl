%% coding: latin-1
%%%-------------------------------------------------------------------
%%% Copyright (c) 2018 Klarna AB
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
%%%-------------------------------------------------------------------
-module(avro_fingerprint_tests).

-include_lib("eunit/include/eunit.hrl").

% Run Java test cases from the Avro project:
% https://github.com/apache/avro/blob/master/share/test/data/schema-tests.txt
% The Java tests use literal signed integers, so we convert to binary when
% comparing so we can use the same literal.

bin(Value) ->
  <<Value:8/unsigned-integer-unit:8>>.

crc64(Bin) ->
  bin(avro:crc64_fingerprint(Bin)).

java_000_test() ->
  ?assertEqual(bin(7195948357588979594), crc64(<<"\"null\"">>)).

java_002_test() ->
  ?assertEqual(bin(-6970731678124411036), crc64(<<"\"boolean\"">>)).

java_004_test() ->
  ?assertEqual(bin(8247732601305521295), crc64(<<"\"int\"">>)).

java_006_test() ->
  ?assertEqual(bin(-3434872931120570953), crc64(<<"\"long\"">>)).

java_010_test() ->
  ?assertEqual(bin(-8181574048448539266), crc64(<<"\"double\"">>)).

java_012_test() ->
  ?assertEqual(bin(5746618253357095269), crc64(<<"\"bytes\"">>)).

java_014_test() ->
  ?assertEqual(bin(-8142146995180207161), crc64(<<"\"string\"">>)).

java_016_test() ->
  ?assertEqual(bin(-1241056759729112623), crc64(<<"[]">>)).

java_017_test() ->
  ?assertEqual(bin(-5232228896498058493), crc64(<<"[\"int\"]">>)).

java_018_test() ->
  ?assertEqual(bin(5392556393470105090), crc64(<<"[\"int\",\"boolean\"]">>)).

java_019_test() ->
  ?assertEqual(bin(-4824392279771201922),
    crc64(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[]}">>)).

java_020_test() ->
  ?assertEqual(bin(5916914534497305771),
    crc64(<<"{\"name\":\"x.y.foo\",\"type\":\"record\",\"fields\":[]}">>)).

java_021_test() ->
  ?assertEqual(bin(-4616218487480524110),
    crc64(<<"{\"name\":\"a.b.foo\",\"type\":\"record\",\"fields\":[]}">>)).

java_022_test() ->
  ?assertEqual(bin(-4824392279771201922),
    crc64(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[]}">>)).

java_025_test() ->
  ?assertEqual(bin(7843277075252814651),
    crc64(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":["
            "{\"name\":\"f1\",\"type\":\"boolean\"}]}">>)).

java_026_test() ->
  ?assertEqual(bin(-4860222112080293046),
    crc64(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":["
            "{\"name\":\"f1\",\"type\":\"boolean\"},"
            "{\"name\":\"f2\",\"type\":\"int\"}]}">>)).

java_027_test() ->
  ?assertEqual(bin(-6342190197741309591),
    crc64(<<"{\"name\":\"foo\",\"type\":\"enum\",\"symbols\":[\"A1\"]}">>)).

java_028_test() ->
  ?assertEqual(bin(-4448647247586288245),
    crc64(<<"{\"name\":\"x.y.z.foo\",\"type\":\"enum\",\"symbols\":["
            "\"A1\",\"A2\"]}">>)).

java_029_test() ->
  ?assertEqual(bin(1756455273707447556),
    crc64(<<"{\"name\":\"foo\",\"type\":\"fixed\",\"size\":15}">>)).

java_030_test() ->
  ?assertEqual(bin(-3064184465700546786),
    crc64(<<"{\"name\":\"x.y.z.foo\",\"type\":\"fixed\","
            "\"size\":32}">>)).

java_031_test() ->
  ?assertEqual(bin(-589620603366471059),
    crc64(<<"{\"type\":\"array\",\"items\":\"null\"}">>)).

java_032_test() ->
  ?assertEqual(bin(-8732877298790414990),
    crc64(<<"{\"type\":\"map\",\"values\":\"string\"}">>)).

java_033_test() ->
  ?assertEqual(bin(-1759257747318642341),
    crc64(<<"{\"name\":\"PigValue\",\"type\":\"record\",\"fields\":["
            "{\"name\":\"value\",\"type\":["
            "\"null\",\"int\",\"long\",\"PigValue\"]}]}">>)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
