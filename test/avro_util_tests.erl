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

avro_schema_compatible_primitive_test() ->
  ?assert(is_compatible(mkschema(primitive_type(int)),
                        mkschema(primitive_type(int)))),
  ?assertMatch(
     {not_compatible, [<<"int">>], [<<"string">>]},
     is_compatible(mkschema(primitive_type(int)),
                   mkschema(primitive_type(string)))).

avro_schema_promotable_primitive_test() ->
  ?assert(is_compatible(mkschema(primitive_type(long)),
                        mkschema(primitive_type(int)))),
  ?assert(is_compatible(mkschema(primitive_type(float)),
                        mkschema(primitive_type(int)))),
  ?assert(is_compatible(mkschema(primitive_type(double)),
                        mkschema(primitive_type(int)))),
  ?assert(is_compatible(mkschema(primitive_type(float)),
                        mkschema(primitive_type(long)))),
  ?assert(is_compatible(mkschema(primitive_type(double)),
                        mkschema(primitive_type(long)))),
  ?assert(is_compatible(mkschema(primitive_type(double)),
                        mkschema(primitive_type(float)))),
  ?assert(is_compatible(mkschema(primitive_type(bytes)),
                        mkschema(primitive_type(string)))),
  ?assert(is_compatible(mkschema(primitive_type(string)),
                        mkschema(primitive_type(bytes)))),
  ?assertMatch(
     {not_compatible, [<<"string">>], [<<"int">>]},
     is_compatible(mkschema(primitive_type(string)),
                   mkschema(primitive_type(int)))).

avro_schema_compatible_array_test() ->
  Reader = mkschema(array_type(string)),
  Writer = mkschema(array_type(string)),
  Writer2 = mkschema(array_type(int)),
  ?assert(is_compatible(Reader, Writer)),
  ?assertMatch(
     {not_compatible, [<<"array">>, <<"string">>], [<<"array">>, <<"int">>]},
     is_compatible(Reader, Writer2)),
  ?assertMatch(
     {not_compatible, [<<"array">>, <<"int">>], [<<"array">>, <<"map">>]},
     is_compatible(
       mkschema(array_type(int)),
       mkschema(array_type(map_type(int)))
      )),
  ok.

avro_schema_compatible_maps_test() ->
  Reader = mkschema(map_type(string)),
  Writer = mkschema(map_type(string)),
  Suits = ["SPADES", "HEARTS", "CLUBS", "DIAMONDS"],
  AlmostSuits = ["SPADES", "HEARTS", "CLUBS"],
  ?assert(is_compatible(Reader, Writer)),
  ?assertMatch(
     {not_compatible, [<<"map">>, <<"string">>], [<<"map">>, <<"int">>]},
     is_compatible(
       mkschema(map_type(string)),
       mkschema(map_type(int))
      )),
  ?assertMatch(
     {not_compatible, [<<"map">>, <<"Suit">>], [<<"map">>, <<"Suit">>]},
     is_compatible(
        mkschema(map_type(enum_type("Suit", AlmostSuits))),
        mkschema(map_type(enum_type("Suit", Suits))))).

avro_schema_compatible_enums_test() ->
  Suits = ["SPADES", "HEARTS", "CLUBS", "DIAMONDS"],
  AlmostSuits = ["SPADES", "HEARTS", "CLUBS"],
  ?assert(
     is_compatible(
       mkschema(enum_type("Suit", Suits)),
       mkschema(enum_type("Suit", Suits))
      )),
  ?assertMatch(
     {not_compatible, [<<"Suit">>], [<<"Cars">>]},
     is_compatible(
       mkschema(enum_type("Suit", Suits)),
       mkschema(enum_type("Cars", Suits))
      )),
  ?assert(
     is_compatible(
      mkschema(enum_type("Suit", Suits)),
      mkschema(enum_type("Suit", ["SPADES"]))
     )),
  ?assertMatch(
     {not_compatible, [<<"Suit">>], [<<"Suit">>]},
     is_compatible(
       mkschema(enum_type("Suit", ["SPADES"])),
       mkschema(enum_type("Suit", Suits))
      )),
  ?assertMatch(
     {not_compatible, [<<"array">>, <<"Suit">>], [<<"array">>, <<"Suit">>]},
     is_compatible(
        mkschema(array_type(enum_type("Suit", AlmostSuits))),
        mkschema(array_type(enum_type("Suit", Suits))))).


avro_schema_compatible_fixed_test() ->
  ?assert(
    is_compatible(
      mkschema(fixed_type("name", 16)),
      mkschema(fixed_type("name", 16))
     )),
  ?assertMatch(
     {not_compatible, [<<"name">>], [<<"name1">>]},
     is_compatible(
       mkschema(fixed_type("name", 16)),
       mkschema(fixed_type("name1", 16))
      )),
  ?assertMatch(
     {not_compatible, [<<"name">>], [<<"name">>]},
     is_compatible(
       mkschema(fixed_type("name", 16)),
       mkschema(fixed_type("name", 14))
      )).

avro_schema_compatible_union_test() ->
  ?assert(
     is_compatible(
       mkschema([<<"null">>]),
       mkschema([<<"null">>])
      )),
  ?assert(
     is_compatible(
       mkschema([<<"int">>, <<"string">>]),
       mkschema([<<"int">>])
      )),
  ?assertMatch(
     { not_compatible, [<<"union">>, {member_id, 0}, <<"int">>]
     , [<<"union">>, {member_id, 0}, <<"string">>]},
     is_compatible(
       mkschema([<<"int">>, <<"string">>]),
       mkschema([<<"string">>, <<"int">>])
      )),
  ?assert(
     is_compatible(
       mkschema([<<"int">>, <<"string">>]),
       mkschema([<<"int">>, <<"string">>])
      )),
  ?assert(
     is_compatible(
       mkschema([<<"int">>, <<"string">>]),
       mkschema(<<"string">>)
      )),
  ?assert(
     is_compatible(
       mkschema([<<"string">>, <<"int">>]),
       mkschema([<<"string">>])
      )),
  ?assertMatch(
     { not_compatible, [<<"union">>, {member_id, 0}, <<"string">>]
     , [<<"union">>, {member_id, 0}, <<"int">>]},
     is_compatible(
       mkschema([<<"string">>, <<"int">>]),
       mkschema([<<"int">>])
      )),

  ?assert(
     is_compatible(
       mkschema(<<"string">>),
       mkschema([<<"string">>])
      )),
  ?assertMatch(
     { not_compatible, [<<"union">>, {member_id, 1}, <<"int">>]
     , [<<"union">>, {member_id, 1}, <<"bytes">>]},
     is_compatible(
       mkschema([<<"string">>, <<"int">>]),
       mkschema([<<"string">>, <<"bytes">>]))),
  ?assertMatch(
     {not_compatible, [<<"string">>], [<<"union">>, {member_id, 1}, <<"int">>]},
     is_compatible(
       mkschema(<<"string">>),
       mkschema([<<"string">>, <<"int">>])
      )),
  ?assert(
     is_compatible(
       mkschema([<<"int">>, <<"long">>]),
       mkschema(<<"long">>)
      )),
  FixedType1 = jsone:encode(fixed_type("name", 16)),
  FixedType2 = jsone:encode(fixed_type("name1", 14)),
  ?assert(
     is_compatible(
       avro:decode_schema(<<"[", FixedType1/binary, ",",
                            FixedType2/binary, "]">>),
       avro:decode_schema(<<"[", FixedType1/binary, "]">>)
      )),
  ?assertMatch(
     {not_compatible, [<<"union">>], [<<"string">>]},
     is_compatible(
       mkschema([<<"int">>, <<"double">>]),
       mkschema(<<"string">>))),
  ?assertMatch(
     {reader_missing_default_value,[ <<"union">>, {member_id, 0}
                                   , <<"record0">>, {field, "field0"}
                                   ]},
     is_compatible(
       mkschema([ record_type("record0", [field("field0", <<"string">>)])
                , <<"string">>
                ]),
       mkschema([ record_type("record0", [field("field1", <<"string">>)])
                , <<"string">>])
      )),
  ?assertMatch(
     {not_compatible, [<<"union">>], [<<"record0">>]},
     is_compatible(
       mkschema([ record_type("record0", [field("field0", <<"string">>)])
                , <<"string">>
                ]),
       mkschema(record_type("record0", [field("field1", <<"string">>)]))
      )).

avro_schema_compatible_record_test() ->
  ?assert(
     is_compatible(
       mkschema(record_type("record0",
                            [ field("field0", <<"string">>) ])),
       mkschema(record_type("record0",
                            [ field("field0", <<"string">>) ]))
      )),
  ?assertMatch(
     {reader_missing_default_value, [<<"record0">>, {field,"field0"}]},
     is_compatible(
       mkschema(record_type("record0",
                            [ field("field0", <<"string">>) ])),
       mkschema(record_type("record0",
                            [ field("field1", <<"string">>) ]))
      )),
  ?assertMatch(
     {not_compatible, [<<"record0">>, {field,"field0"}, <<"string">>],
                      [<<"record0">>, {field,"field0"}, <<"int">>]},
     is_compatible(
       mkschema(record_type("record0",
                            [ field("field0", <<"string">>) ])),
       mkschema(record_type("record0",
                            [ field("field0", <<"int">>) ]))
      )),
  ?assert(
     is_compatible(
       mkschema(record_type("record0",
                            [ field("defaultfield",
                                    <<"string">>, <<"foo">>) ])),
       mkschema(record_type("record0", []))
      )),
  ?assertMatch(
     {not_compatible, [ <<"record0">>, {field,"field1"}
                      , <<"inner_record">>, {field,"inner_field"}
                      , <<"int">>],
                      [ <<"record0">>, {field,"field1"}
                      , <<"inner_record">>, {field,"inner_field"}
                      , <<"string">>]},
     is_compatible(
       mkschema(record_type("record0",
                            [ field("field0", <<"string">>)
                            , field("field1",
                                    record_type("inner_record",
                                                [ field("inner_field",
                                                        <<"int">>)
                                                ]))
                            ])),
       mkschema(record_type("record0",
                            [ field("field0", <<"string">>)
                            , field("field1",
                                    record_type("inner_record",
                                                [ field("inner_field",
                                                        <<"string">>)
                                                ]))
                            ])))).

primitive_type(Type) ->
  #{ type => Type }.

array_type(Type) ->
  #{ type => array
   , items => Type
   }.

map_type(Type) ->
  #{ type => map
   , values => Type
   }.

enum_type(Name, Symbols) ->
  #{ type => enum
   , name => Name
   , symbols => Symbols
   }.

fixed_type(Name, Size) ->
  #{ type => fixed
   , name => Name
   , size => Size
   }.

record_type(Name, Fields) ->
  #{ type => record
   , name => Name
   , fields => Fields
   }.

field(Name, Type) ->
  #{ type => Type
   , name => Name
   }.

field(Name, Type, Default) ->
  #{ type    => Type
   , name    => Name
   , default => Default
   }.

mkschema(Map) ->
  avro:decode_schema(jsone:encode(Map)).

is_compatible(Reader, Writer) -> avro:is_compatible(Reader, Writer).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
