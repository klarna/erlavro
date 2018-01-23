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

-export([crc64/1]). % silence warning about unused function

% Run Java test cases from the Avro project:
% https://github.com/apache/avro/blob/master/share/test/data/schema-tests.txt
% The Java tests use literal signed integers, so we convert to binary when
% comparing so we can use the same literal.

bin(Value) ->
    <<Value:8/unsigned-integer-unit:8>>.

crc64(Bin) ->
    bin(avro_fingerprint:crc64(Bin)).

java_test() ->
    ?assertEqual(crc64(<<"\"null\"">>), bin(7195948357588979594)),        % 000
    ?assertEqual(crc64(<<"\"boolean\"">>), bin(-6970731678124411036)),    % 002
    ?assertEqual(crc64(<<"\"int\"">>), bin(8247732601305521295)),         % 004
    ?assertEqual(crc64(<<"\"long\"">>), bin(-3434872931120570953)),       % 006
    ?assertEqual(crc64(<<"\"double\"">>), bin(-8181574048448539266)),     % 010
    ?assertEqual(crc64(<<"\"bytes\"">>), bin(5746618253357095269)),       % 012
    ?assertEqual(crc64(<<"\"string\"">>), bin(-8142146995180207161)),     % 014
    ?assertEqual(crc64(<<"[]">>), bin(-1241056759729112623)),             % 016
    ?assertEqual(crc64(<<"[\"int\"]">>), bin(-5232228896498058493)),      % 017
    ?assertEqual(crc64(<<"[\"int\",\"boolean\"]">>), bin(5392556393470105090)), % 018
    ?assertEqual(crc64(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[]}">>),
                 bin(-4824392279771201922)), % 019
    ?assertEqual(crc64(<<"{\"name\":\"x.y.foo\",\"type\":\"record\",\"fields\":[]}">>),
                 bin(5916914534497305771)), % 020
    ?assertEqual(crc64(<<"{\"name\":\"a.b.foo\",\"type\":\"record\",\"fields\":[]}">>),
                 bin(-4616218487480524110)), % 021
    ?assertEqual(crc64(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[]}">>),
                 bin(-4824392279771201922)), % 022
    ?assertEqual(crc64(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[{\"name\":\"f1\",\"type\":\"boolean\"}]}">>),
                 bin(7843277075252814651)), % 025
    ?assertEqual(crc64(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[{\"name\":\"f1\",\"type\":\"boolean\"},{\"name\":\"f2\",\"type\":\"int\"}]}">>),
                 bin(-4860222112080293046)), % 026
    ?assertEqual(crc64(<<"{\"name\":\"foo\",\"type\":\"enum\",\"symbols\":[\"A1\"]}">>),
                 bin(-6342190197741309591)), % 027
    ?assertEqual(crc64(<<"{\"name\":\"x.y.z.foo\",\"type\":\"enum\",\"symbols\":[\"A1\",\"A2\"]}">>),
                 bin(-4448647247586288245)), % 028
    ?assertEqual(crc64(<<"{\"name\":\"foo\",\"type\":\"fixed\",\"size\":15}">>),
                 bin(1756455273707447556)), % 029
    ?assertEqual(crc64(<<"{\"name\":\"x.y.z.foo\",\"type\":\"fixed\",\"size\":32}">>),
                 bin(-3064184465700546786)), % 030
    ?assertEqual(crc64(<<"{\"type\":\"array\",\"items\":\"null\"}">>),
                 bin(-589620603366471059)), % 031
    ?assertEqual(crc64(<<"{\"type\":\"map\",\"values\":\"string\"}">>),
                 bin(-8732877298790414990)), % 032
    ?assertEqual(crc64(<<"{\"name\":\"PigValue\",\"type\":\"record\",\"fields\":[{\"name\":\"value\",\"type\":[\"null\",\"int\",\"long\",\"PigValue\"]}]}">>),
                 bin(-1759257747318642341)). % 033

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
