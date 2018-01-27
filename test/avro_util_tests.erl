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
-module(avro_util_tests).

-import(avro_util, [ canonicalize_aliases/2
                   , verify_type/1
                   , tokens_ex/2
                   , is_valid_name/1
                   , is_valid_dotted_name/1
                   ]).

-include_lib("eunit/include/eunit.hrl").

flatten_primitive_type_test() ->
  Type = avro_primitive:int_type(),
  ?assertEqual({Type, []}, avro:flatten_type(Type)).

flatten_nested_primitive_type_test() ->
  Type = avro_array:type(int),
  ?assertEqual({Type, []}, avro:flatten_type(Type)).

flatten_named_type_test() ->
  Type = avro_array:type("com.klarna.test.bix.SomeType"),
  ?assertEqual({Type, []}, avro:flatten_type(Type)).

tokens_ex_test() ->
  ?assertEqual([""], tokens_ex("", $.)),
  ?assertEqual(["ab"], tokens_ex("ab", $.)),
  ?assertEqual(["", ""], tokens_ex(".", $.)),
  ?assertEqual(["", "a", "b", "c", ""], tokens_ex(".a.b.c.", $.)),
  ?assertEqual(["ab", "cd"], tokens_ex("ab.cd", $.)).

is_valid_name_test() ->
  ValidNames = ["_", "a", "Aa1", "a_A"],
  InvalidNames = ["", "1", " a", "a ", " a ", ".", "a.b.c"],
  [?assert(is_valid_name(Name)) || Name <- ValidNames],
  [?assertNot(is_valid_name(Name)) || Name <- InvalidNames].

is_valid_dotted_name_test_() ->
  ValidNames = ["_", "a", "A._1", <<"a1.b2.c3">>],
  InvalidNames = ["", "1", " a.b.c", "a.b.c ", " a.b.c ", "a..b", ".a.b",
    "a.1.b", "!", "-", "a. b.c"],
  [?_assert(is_valid_dotted_name(Name)) || Name <- ValidNames] ++
  [?_assertNot(is_valid_dotted_name(Name)) || Name <- InvalidNames].

verify_type_test() ->
  ?assertEqual(ok, verify_type(get_test_type("tname", "name.space"))),
  ?assertError({invalid_name, _}, verify_type(get_test_type("", ""))),
  ?assertError({invalid_name, _}, verify_type(get_test_type("", "name.space"))),
  ?assertEqual(ok, verify_type(get_test_type("tname", ""))).

canonizalize_aliases_test() ->
  %% Namespaces for aliases are taken from the original type namespace
  ?assertEqual([<<"name.space.Foo">>, <<"name.space.Bar">>],
    canonicalize_aliases(["Foo", "Bar"], "name.space")),
  ?assertEqual([<<"Foo">>, <<"Bar">>],
    canonicalize_aliases(["Foo", "Bar"], "")).

get_opt_test() ->
  ?assertEqual(value, avro_util:get_opt(key, [{key, value}])),
  ?assertException(error, {not_found, "key"},
                   avro_util:get_opt("key", [{key, value}])).

ensure_lkup_fun_test() ->
  _ = avro_util:ensure_lkup_fun(<<"{\"type\": \"int\"}">>),
  Type = avro:name2type("int"),
  ?assert(is_function(avro_util:ensure_lkup_fun(Type), 1)),
  ok.

get_test_type(Name, Namespace) ->
  avro_fixed:type(Name, 16, [{namespace, Namespace}]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
