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
-module(avro_record_tests).

-import(avro_record, [ new/2
                     , type/2
                     , type/3
                     , define_field/2
                     , define_field/3
                     , get_field_def/2
                     , to_list/1
                     , set_value/3
                     , set_values/2
                     , update/3
                     , get_field_type/2
                     , get_value/2
                     , cast_value/2
                     , cast/2
                     ]).

-include_lib("eunit/include/eunit.hrl").
-include("avro_internal.hrl").

type_test() ->
  Field = define_field("invno", long),
  Schema = type("Test", [Field], [{namespace, "name.space"}]),
  ?assertEqual(<<"name.space.Test">>, avro:get_type_fullname(Schema)),
  ?assertEqual({ok, Field}, get_field_def("invno", Schema)).

get_field_def_test() ->
  Field1 = define_field(f1, long),
  Field2 = define_field("f2", long, [{aliases, [a, b]}]),
  Field3 = define_field("f3", long),
  Record = type("Test", [Field1, Field2, Field3]),
  ?assertEqual(false, get_field_def("f4", Record)),
  ?assertEqual({ok, Field2}, get_field_def("f2", Record)),
  ?assertEqual({ok, Field3}, get_field_def("f3", Record)),
  ?assertEqual({ok, Field2}, get_field_def(b, Record)).

get_field_type_test() ->
  Field = define_field("invno", long),
  Schema = type("Test", [Field], [{namespace, "name.space"}]),
  ?assertEqual(avro_primitive:long_type(),
               get_field_type("invno", Schema)).

default_fields_test() ->
  Field = define_field("invno", long,
                       [{default, avro_primitive:long(10)}]),
  Schema = type("Test", [Field], [{namespace, "name.space"}]),
  Rec = new(Schema, []),
  ?assertEqual(avro_primitive:long(10), get_value("invno", Rec)),
  ?assertException(error, {unknown_field, <<"no_such_field">>},
                   get_value("no_such_field", Rec)).

get_set_test() ->
  Schema = type("Test", [define_field("invno", long),
                         define_field("uname", string)
                        ],
                [{namespace, "name.space"}]),
  Rec0 = new(Schema, [{"invno", 0}, {"uname", "some-name"}]),
  Rec1 = set_value("invno", avro_primitive:long(1), Rec0),
  ?assertEqual(avro_primitive:long(1), get_value("invno", Rec1)),
  Rec2 = set_values([{"invno", 2}, {"uname", "new-name"}], Rec1),
  ?assertEqual(avro_primitive:long(2), get_value("invno", Rec2)),
  ?assertEqual(avro_primitive:string("new-name"), get_value("uname", Rec2)),
  ?assertException(error, {<<"invno">>, _}, set_value("invno", "string", Rec2)),
  ?assertException(error, {unknown_field, <<"x">>}, set_value("x", "y", Rec2)).

update_test() ->
  Schema = type("Test", [define_field("invno", long)],
                [{namespace, "name.space"}]),
  Rec0 = new(Schema, [{"invno", 10}]),
  Rec1 = update("invno",
                fun(X) ->
                    avro_primitive:long(avro_primitive:get_value(X)*2)
                end,
                Rec0),
  ?assertEqual(avro_primitive:long(20), get_value("invno", Rec1)).

to_list_test() ->
  Schema = type("Test", [ define_field("invno", long)
                        , define_field("name", string)
                        ],
                        [{namespace, "name.space"}]),
  Rec = new(Schema, [ {"invno", avro_primitive:long(1)}
                    , {"name", avro_primitive:string("some name")}
                    ]),
  L = to_list(Rec),
  ?assertEqual(2, length(L)),
  ?assertEqual({<<"invno">>, avro_primitive:long(1)},
               lists:keyfind(<<"invno">>, 1, L)),
  ?assertEqual({<<"name">>, avro_primitive:string("some name")},
               lists:keyfind(<<"name">>, 1, L)).

to_term_test() ->
  Schema = type("Test",
                [ define_field(invno, long)
                , define_field("name", string)
                ],
                [{namespace, "name.space"}]),
  Rec = new(Schema, [ {"invno", avro_primitive:long(1)}
                    , {"name", avro_primitive:string("some name")}
                    ]),
  Fields = avro:to_term(Rec),
  ?assertEqual(2, length(Fields)),
  ?assertEqual({<<"invno">>, 1}, lists:keyfind(<<"invno">>, 1, Fields)),
  ?assertEqual({<<"name">>, <<"some name">>},
               lists:keyfind(<<"name">>, 1, Fields)).

cast_test() ->
  RecordType = type("Record",
                    [ define_field("a", string)
                    , define_field("b", int)
                    ],
                    [ {namespace, "name.space"} ]),
  {ok, Record} = cast(RecordType, [{"b", 1}, {"a", "foo"}]),
  ?assertEqual(avro_primitive:string("foo"), get_value("a", Record)),
  ?assertEqual(avro_primitive:int(1), get_value("b", Record)).

cast_error_test() ->
  RecordType = type("Record",
                    [define_field("a", long)],
                    [ {namespace, "name.space"} ]),
  ?assertMatch({error, {<<"a">>, _}},
               cast(RecordType, [{"a", "foo"}])).

cast_by_aliases_test() ->
  RecordType = type("Record",
                    [ define_field("a", string, [{aliases, ["al1", "al2"]}])
                    , define_field("b", int, [{aliases, ["al3", "al4"]}])
                    ],
                    [ {namespace, "name.space"}
                    ]),
  {ok, Record} = cast(RecordType, [{"al4", 1}, {"al1", "foo"}]),
  ?assertEqual(avro_primitive:string("foo"), get_value("a", Record)),
  ?assertEqual(avro_primitive:int(1), get_value("b", Record)).

encode_test() ->
  EncodeFun = fun(FieldName, _FieldType, Input) ->
                  {FieldName, {encoded, Input}}
              end,
  Type = type("Test",
              [ define_field("field1", long)
              , define_field("field2", string)],
              [ {namespace, "name.space"} ]),
  ?assertError(?ENC_ERR(required_field_missed,
                        [{record, <<"name.space.Test">>},
                         {field, <<"field2">>}]),
               avro_record:encode(Type, [{<<"field1">>, 1}], EncodeFun)),
  ?assertEqual([{<<"field1">>, {encoded, 1}},
                {<<"field2">>, {encoded, foo}}],
               avro_record:encode(Type,
                                  [{"field2", foo}, {"field1", 1}],
                                  EncodeFun)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
