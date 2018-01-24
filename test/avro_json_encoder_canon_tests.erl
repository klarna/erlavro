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
-module(avro_json_encoder_canon_tests).

-include_lib("eunit/include/eunit.hrl").

% Run Java test cases from the Avro project:
% https://github.com/apache/avro/blob/master/share/test/data/schema-tests.txt

% -export([canon/1]). % Silence unused function warning

canon(Json) ->
  Schema = avro_json_decoder:decode_schema(Json),
  avro:encode_schema(Schema, #{canon => true}).

java_primitive_test() ->
  ?assertEqual(<<"\"null\"">>, canon(<<"\"null\"">>)),                % 000
  ?assertEqual(<<"\"null\"">>, canon(<<"{\"type\":\"null\"}">>)),     % 001
  ?assertEqual(<<"\"boolean\"">>, canon(<<"\"boolean\"">>)),          % 002
  ?assertEqual(<<"\"boolean\"">>, canon(<<"{\"type\":\"boolean\"}">>)), % 003
  ?assertEqual(<<"\"int\"">>, canon(<<"\"int\"">>)),                  % 004
  ?assertEqual(<<"\"int\"">>, canon(<<"{\"type\":\"int\"}">>)),       % 005
  ?assertEqual(<<"\"long\"">>,canon(<<"\"long\"">>)),                 % 006
  ?assertEqual(<<"\"long\"">>, canon(<<"{\"type\":\"long\"}">>)),     % 007
  ?assertEqual(<<"\"float\"">>, canon(<<"\"float\"">>)),              % 008
  ?assertEqual(<<"\"float\"">>, canon(<<"{\"type\":\"float\"}">>)),   % 009
  ?assertEqual(<<"\"double\"">>, canon(<<"\"double\"">>)),            % 010
  ?assertEqual(<<"\"double\"">>, canon(<<"{\"type\":\"double\"}">>)), % 011
  ?assertEqual(<<"\"bytes\"">>, canon(<<"\"bytes\"">>)),              % 012
  ?assertEqual(<<"\"bytes\"">>, canon(<<"{\"type\":\"bytes\"}">>)),   % 013
  ?assertEqual(<<"\"string\"">>, canon(<<"\"string\"">>)),            % 014
  ?assertEqual(<<"\"string\"">>, canon(<<"{\"type\":\"string\"}">>)), % 015
  % This gives an error because avro_json_decoder:decode_schema/1 considers it invalid
  % ?assertEqual(<<"[]">>, canon(<<"[  ]">>)),                        % 016
  ?assertEqual(<<"[\"int\"]">>, canon(<<"[ \"int\"  ]">>)),           % 017
  ?assertEqual(<<"[\"int\",\"boolean\"]">>,
               canon(<<"[ \"int\" , {\"type\":\"boolean\"} ]">>)).    % 018

java_019_test() ->
  ?assertEqual(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[]}">>,
               canon(<<"{\"fields\":[], "
                       "\"type\":\"record\", \"name\":\"foo\"}">>)).

java_020_test() ->
  ?assertEqual(<<"{\"name\":\"x.y.foo\",\"type\":\"record\",\"fields\":[]}">>,
               canon(<<"{\"fields\":[], \"type\":\"record\", \"name\":\"foo\", "
                       "\"namespace\":\"x.y\"}">>)).

java_021_test() ->
  ?assertEqual(<<"{\"name\":\"a.b.foo\",\"type\":\"record\",\"fields\":[]}">>,
               canon(<<"{\"fields\":[], \"type\":\"record\", "
                       "\"name\":\"a.b.foo\", \"namespace\":\"x.y\"}">>)).

java_022_test() ->
  ?assertEqual(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[]}">>,
               canon(<<"{\"fields\":[], \"type\":\"record\", "
                       "\"name\":\"foo\", \"doc\":\"Useful info\"}">>)).

java_023_test() ->
  ?assertEqual(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[]}">>,
               canon(<<"{\"fields\":[], \"type\":\"record\", "
                       "\"name\":\"foo\", \"aliases\":[\"foo\",\"bar\"]}">>)).

java_024_test() ->
    ?assertEqual(canon(<<"{\"fields\":[], \"type\":\"record\", "
                         "\"name\":\"foo\", \"doc\":\"foo\", "
                         "\"aliases\":[\"foo\",\"bar\"]}">>),
                 <<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":[]}">>).

java_025_test() ->
    ?assertEqual(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":["
                   "{\"name\":\"f1\",\"type\":\"boolean\"}]}">>,
                 canon(<<"{\"fields\":[{\"type\":{\"type\":\"boolean\"}, "
                         "\"name\":\"f1\"}], \"type\":\"record\", "
                         "\"name\":\"foo\"}">>)).

java_026_test() ->
    ?assertEqual(<<"{\"name\":\"foo\",\"type\":\"record\",\"fields\":["
                   "{\"name\":\"f1\",\"type\":\"boolean\"},"
                   "{\"name\":\"f2\",\"type\":\"int\"}]}">>,
                canon(<<"{ \"fields\":[{\"type\":\"boolean\", "
                          "\"aliases\":[], \"name\":\"f1\", "
                          "\"default\":true},", 10:8,
                        "            {\"order\":\"descending\",\"name\":\"f2\","
                        "\"doc\":\"Hello\",\"type\":\"int\"}],", 10:8,
                        "  \"type\":\"record\", \"name\":\"foo\"", 10:8,
                        "}", 10:8
                 >>)).

java_027_test() ->
    ?assertEqual(<<"{\"name\":\"foo\",\"type\":\"enum\","
                   "\"symbols\":[\"A1\"]}">>,
                 canon(<<"{\"type\":\"enum\", \"name\":\"foo\", "
                         "\"symbols\":[\"A1\"]}">>)).

java_028_test() ->
    ?assertEqual(<<"{\"name\":\"x.y.z.foo\",\"type\":\"enum\","
                   "\"symbols\":[\"A1\",\"A2\"]}">>,
                 canon(<<"{\"namespace\":\"x.y.z\", \"type\":\"enum\", "
                         "\"name\":\"foo\", \"doc\":\"foo bar\", "
                         "\"symbols\":[\"A1\", \"A2\"]}">>)).

java_029_test() ->
    ?assertEqual(<<"{\"name\":\"foo\",\"type\":\"fixed\",\"size\":15}">>,
                 canon(<<"{\"name\":\"foo\",\"type\":\"fixed\","
                         "\"size\":15}">>)).

java_030_test() ->
    ?assertEqual(<<"{\"name\":\"x.y.z.foo\",\"type\":\"fixed\",\"size\":32}">>,
                 canon(<<"{\"namespace\":\"x.y.z\", \"type\":\"fixed\", "
                         "\"name\":\"foo\", \"doc\":\"foo bar\", \"size\":32}">>)).

java_031_test() ->
    ?assertEqual(<<"{\"type\":\"array\",\"items\":\"null\"}">>,
                 canon(<<"{ \"items\":{\"type\":\"null\"}, "
                         "\"type\":\"array\"}">>)).

java_032_test() ->
    ?assertEqual(canon(<<"{ \"values\":\"string\", \"type\":\"map\"}">>),
                 <<"{\"type\":\"map\",\"values\":\"string\"}">>).
java_033_test() ->
    ?assertEqual(<<"{\"name\":\"PigValue\",\"type\":\"record\",\"fields\":["
                   "{\"name\":\"value\",\"type\":[\"null\",\"int\",\"long\","
                   "\"PigValue\"]}]}">>,
       canon(<<"  {\"name\":\"PigValue\",\"type\":\"record\",", 10:8,
               "   \"fields\":[{\"name\":\"value\", \"type\":[\"null\", "
               "\"int\", \"long\", \"PigValue\"]}]}", 10:8>>)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
