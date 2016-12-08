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

-include_lib("eunit/include/eunit.hrl").

type_test() ->
  Field = avro_record:define_field("invno", avro_primitive:long_type()),
  Schema = avro_record:type("Test", [Field],
    [ {namespace, "name.space"}
    ]),
  ?assertEqual("name.space.Test", avro:get_type_fullname(Schema)),
  ?assertEqual({ok, Field}, avro_record:get_field_def("invno", Schema)).

get_field_def_test() ->
  Field1 = avro_record:define_field("f1", avro_primitive:long_type()),
  Field2 = avro_record:define_field("f2", avro_primitive:long_type(),
    [{aliases, ["a1", "a2"]}]),
  Field3 = avro_record:define_field("f3", avro_primitive:long_type()),
  Record = avro_record:type("Test", [Field1, Field2, Field3]),
  ?assertEqual(false, avro_record:get_field_def("f4", Record)),
  ?assertEqual({ok, Field2}, avro_record:get_field_def("f2", Record)),
  ?assertEqual({ok, Field3}, avro_record:get_field_def("f3", Record)),
  ?assertEqual({ok, Field2}, avro_record:get_field_def("a2", Record)).

get_field_type_test() ->
  Field = avro_record:define_field("invno", avro_primitive:long_type()),
  Schema = avro_record:type("Test", [Field],
    [ {namespace, "name.space"}
    ]),
  ?assertEqual(avro_primitive:long_type(),
               avro_record:get_field_type("invno", Schema)).

default_fields_test() ->
  Field = avro_record:define_field("invno",
    avro_primitive:long_type(),
    [ {default, avro_primitive:long(10)}
    ]),
  Schema = avro_record:type("Test", [Field],
    [ {namespace, "name.space"}
    ]),
  Rec = avro_record:new(Schema, []),
  ?assertEqual(avro_primitive:long(10), avro_record:get_value("invno", Rec)).

get_set_test() ->
  Schema = avro_record:type("Test",
    [avro_record:define_field("invno", avro_primitive:long_type())],
    [ {namespace, "name.space"}
    ]),
  Rec0 = avro_record:new(Schema, [{"invno", 0}]),
  Rec1 = avro_record:set_value("invno", avro_primitive:long(1), Rec0),
  ?assertEqual(avro_primitive:long(1), avro_record:get_value("invno", Rec1)).

update_test() ->
  Schema = avro_record:type("Test",
    [avro_record:define_field("invno", avro_primitive:long_type())],
    [ {namespace, "name.space"}
    ]),
  Rec0 = avro_record:new(Schema, [{"invno", 10}]),
  Rec1 = avro_record:update("invno",
    fun(X) ->
      avro_primitive:long(avro_primitive:get_value(X)*2)
    end,
    Rec0),
  ?assertEqual(avro_primitive:long(20), avro_record:get_value("invno", Rec1)).

to_list_test() ->
  Schema = avro_record:type("Test",
    [ avro_record:define_field("invno", avro_primitive:long_type())
      , avro_record:define_field("name", avro_primitive:string_type())
    ],
    [ {namespace, "name.space"}
    ]),
  Rec = avro_record:new(Schema, [ {"invno", avro_primitive:long(1)}
    , {"name", avro_primitive:string("some name")}
  ]),
  L = avro_record:to_list(Rec),
  ?assertEqual(2, length(L)),
  ?assertEqual({"invno", avro_primitive:long(1)},
    lists:keyfind("invno", 1, L)),
  ?assertEqual({"name", avro_primitive:string("some name")},
    lists:keyfind("name", 1, L)).

to_term_test() ->
  Schema = avro_record:type("Test",
    [ avro_record:define_field("invno", avro_primitive:long_type())
      , avro_record:define_field("name", avro_primitive:string_type())
    ],
    [ {namespace, "name.space"}
    ]),
  Rec = avro_record:new(Schema, [ {"invno", avro_primitive:long(1)}
    , {"name", avro_primitive:string("some name")}
  ]),
  {Name, Fields} = avro:to_term(Rec),
  ?assertEqual(Name, "name.space.Test"),
  ?assertEqual(2, length(Fields)),
  ?assertEqual({"invno", 1},
    lists:keyfind("invno", 1, Fields)),
  ?assertEqual({"name", "some name"},
    lists:keyfind("name", 1, Fields)).

cast_test() ->
  RecordType = avro_record:type("Record",
    [ avro_record:define_field("a", avro_primitive:string_type())
      , avro_record:define_field("b", avro_primitive:int_type())
    ],
    [ {namespace, "name.space"}
    ]),
  {ok, Record} = avro_record:cast(RecordType, [{"b", 1},
    {"a", "foo"}]),
  ?assertEqual(avro_primitive:string("foo"),
               avro_record:get_value("a", Record)),
  ?assertEqual(avro_primitive:int(1), avro_record:get_value("b", Record)).

cast_by_aliases_test() ->
  RecordType = avro_record:type("Record",
    [ avro_record:define_field("a", avro_primitive:string_type(),
      [{aliases, ["al1", "al2"]}])
      , avro_record:define_field("b", avro_primitive:int_type(),
      [{aliases, ["al3", "al4"]}])
    ],
    [ {namespace, "name.space"}
    ]),
  {ok, Record} = avro_record:cast(RecordType, [{"al4", 1},
    {"al1", "foo"}]),
  ?assertEqual(avro_primitive:string("foo"),
               avro_record:get_value("a", Record)),
  ?assertEqual(avro_primitive:int(1), avro_record:get_value("b", Record)).

new_encoded_test() ->
  Type = avro_record:type("Test",
    [ avro_record:define_field("field1", avro_primitive:long_type())
      , avro_record:define_field("field2", avro_primitive:string_type())
    ],
    [ {namespace, "name.space"}
    ]),
  Fields = [ {"field1", avro_primitive:long(1)}
    , {"field2", avro_primitive:string("f")}
  ],
  Rec = avro_record:new_encoded(Type, Fields, json_binary),
  ?assertException(throw, {value_already_encoded, _},
    avro_record:get_value("any", Rec)),
  ?assertException(throw, {value_already_encoded, _},
    avro_record:set_value("any", "whatever", Rec)),
  ?assertException(throw, {value_already_encoded, _},
    avro_record:update("any", fun()-> "care not" end, Rec)),
  ?assertException(throw, {value_already_encoded, _}, avro_record:to_list(Rec)).

cast_uncast_simple_test() ->
  RecordType = avro_record:type("Record",
    [ avro_record:define_field("a", avro_primitive:string_type())
    , avro_record:define_field("b", avro_primitive:int_type())
    ],
    [ {namespace, "name.space"}
    ]),
  {ok, AvroValue} = avro:cast(RecordType, [{"b", 1}, {"a", "foo"}]),
  {ok, Uncasted} = avro:uncast(AvroValue),
  ?assertEqual([{"b", 1}, {"a", "foo"}], Uncasted).

cast_uncast_complex_test() ->
  InnerType2 = avro_record:type("Inner", [ avro_record:define_field("one", avro_primitive:int_type())
                                        , avro_record:define_field("two", avro_primitive:int_type())
                                        ]),
  InnerType = avro_record:type("Inner", [ avro_record:define_field("1", avro_primitive:string_type())
                                        , avro_record:define_field("2", InnerType2)
                                        ]),
  RecordType = avro_record:type("Record",
    [ avro_record:define_field("a", InnerType)
    , avro_record:define_field("b", avro_primitive:int_type())
    ],
    [ {namespace, "name.space"}
    ]),
  {ok, AvroValue} = avro:cast(RecordType, [{"b", 1}, {"a", [{"1", "hello"}, {"2", [{"one", 1}, {"two", 2}]}]}]),
  {ok, Uncasted} = avro:uncast(AvroValue),
  ?assertEqual([{"b", 1}, {"a", [{"1", "hello"}, {"2", [{"one", 1}, {"two", 2}]}]}], Uncasted).