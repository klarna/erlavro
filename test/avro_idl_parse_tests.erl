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
-module(avro_idl_parse_tests).

-include("../src/idl.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_empty_protocol_test() ->
    ?assertEqual(
       #protocol{name = "MyProto"},
       parse_idl("empty_protocol")).

parse_annotations_test() ->
    ?assertEqual(
       #protocol{
          name = "MyProto",
          annotations =
              [#annotation{name = "version",
                           value = "1.0"},
               #annotation{name = "aliases",
                           value = ["ns.Proto1", "ns.Proto2"]}
              ],
          definitions =
              [#enum{name = "MyEnum",
                     annotations =
                         [#annotation{name = "namespace",
                                      value = "enums"}],
                     variants = ["A", "B", "C"]},
               #fixed{name = "MyFixed",
                      annotations =
                          [#annotation{name = "namespace",
                                       value = "fixeds"}],
                     size = 16},
               #error{name = "MyError",
                      annotations =
                          [#annotation{name = "namespace",
                                       value = "errors"}],
                      fields =
                          [#field{name = "my_err_field",
                                  annotations =
                                      [#annotation{name = "order",
                                                   value = "ignore"}],
                                  type = string}]},
               #record{name = "MyRecord",
                       annotations =
                           [#annotation{name = "namespace",
                                        value = "records"}],
                       fields =
                           [#field{name = "my_record_field",
                                   annotations =
                                       [#annotation{name = "order",
                                                    value = "ignore"},
                                        #annotation{name = "aliases",
                                                    value = ["my_alias"]}],
                                   type = string}]}]
         },
       parse_idl("annotations")).

full_protocol_test() ->
    ?assertMatch(
      #protocol{name = "Simple",
                definitions =
                    [#enum{name = "Kind"},
                     #fixed{name = "MD5"},
                     #record{name = "TestRecord"},
                     #error{name = "TestError"},
                     #function{name = "hello"},
                     #function{name = "echo"},
                     #function{name = "add"},
                     #function{name = "echoBytes"},
                     #function{name = "error"},
                     #function{name = "ping"}]},
      parse_idl("full_protocol")).

protocol_with_typedeffs_test() ->
    ?assertMatch(
      #protocol{name = "MyProto",
                definitions =
                    [{import, idl, "foo.avdl"},
                     {import, protocol, "bar.avpr"},
                     {import, schema, "baz.avsc"},
                     #enum{name = "MyEnum1"},
                     #enum{name = "MyEnum2"},
                     #fixed{name = "MyFix"},
                     #record{name = "MyRec"},
                     #record{name = "MyAnnotated"},
                     #error{name = "MyError"},
                     #function{name = "mul"},
                     #function{name = "append"},
                     #function{name = "gen_server_cast"},
                     #function{name = "ping"}]},
       parse_idl("protocol_with_typedefs")).

parse_idl(Name) ->
    File = "test/data/" ++ Name ++ ".avdl",
    {ok, B} = file:read_file(File),
    {ok, T, _} =  avro_idl_lexer:string(binary_to_list(B)),
    NoComments = lists:filter(
                  fun({doc_v, _, _}) -> false;
                     ({comment_v, _, _}) -> false;
                     (_) -> true
                  end, T),
    {ok, Tree} = avro_idl_parser:parse(NoComments),
    Tree.
