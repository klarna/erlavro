%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, Klarna AB
%%% @doc
%%%
%%% @end
%%% Created : 07. Dec 2016 11:04 AM
%%%-------------------------------------------------------------------
-module(avro_util_tests).
-author("tihon").

-include_lib("eunit/include/eunit.hrl").

tokens_ex_test() ->
  ?assertEqual([""], avro_util:tokens_ex("", $.)),
  ?assertEqual(["ab"], avro_util:tokens_ex("ab", $.)),
  ?assertEqual(["", ""], avro_util:tokens_ex(".", $.)),
  ?assertEqual(["", "a", "b", "c", ""], avro_util:tokens_ex(".a.b.c.", $.)),
  ?assertEqual(["ab", "cd"], avro_util:tokens_ex("ab.cd", $.)).

is_correct_name_test() ->
  CorrectNames = ["_", "a", "Aa1", "a_A"],
  IncorrectNames = ["", "1", " a", "a ", " a ", ".", "a.b.c"],
  [?assert(avro_util:is_correct_name(Name)) || Name <- CorrectNames],
  [?assertNot(avro_util:is_correct_name(Name)) || Name <- IncorrectNames].

is_correct_dotted_name_test_() ->
  CorrectNames = ["_", "a", "A._1", "a1.b2.c3"],
  IncorrectNames = ["", "1", " a.b.c", "a.b.c ", " a.b.c ", "a..b", ".a.b",
    "a.1.b", "!", "-", "a. b.c"],
  [?_assert(avro_util:is_correct_dotted_name(Name)) || Name <- CorrectNames] ++
  [?_assertNot(avro_util:is_correct_dotted_name(Name)) || Name <- IncorrectNames].

verify_type_test() ->
  ?assertEqual(ok, avro_util:verify_type(get_test_type("tname", "name.space"))),
  ?assertError({invalid_name, _}, avro_util:verify_type(get_test_type("", ""))),
  ?assertError({invalid_name, _}, avro_util:verify_type(get_test_type("", "name.space"))),
  ?assertEqual(ok, avro_util:verify_type(get_test_type("tname", ""))).

canonizalize_aliases_test() ->
  %% Namespaces for aliases are taken from the original type namespace
  ?assertEqual(["name.space.Foo", "name.space.Bar"],
    avro_util:canonicalize_aliases(["Foo", "Bar"],
      "Bee",
      "name.space",
      "enc.losing")),
  %% Aliases have their own namespaces
  ?assertEqual(["other.ns.Foo", "another.ns2.Bar"],
    avro_util:canonicalize_aliases(["other.ns.Foo", "another.ns2.Bar"],
      "Bee",
      "name.space",
      "enc.losing")),
  %% Namespaces for aliases are taken from enclosing namespace
  ?assertEqual(["enc.losing.Foo", "enc.losing.Bar"],
    avro_util:canonicalize_aliases(["Foo", "Bar"],
      "Bee",
      "",
      "enc.losing")),
  %% Namespaces for aliases are taken from the full type name
  ?assertEqual(["name.space.Foo", "name.space.Bar"],
    avro_util:canonicalize_aliases(["Foo", "Bar"],
      "name.space.Bee",
      "bla.bla",
      "enc.losing")).


%% @private
get_test_type(Name, Namespace) ->
  avro_fixed:type(Name, 16, [{namespace, Namespace}]).