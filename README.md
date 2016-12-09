Avro support for Erlang (http://avro.apache.org/).

Current version implements Apache Avro 1.7.5 specification.

License: Apache License 2.0

# Compilation

   make

Dependencies: jsonx and mochijson3 (see rebar.config).

[![Build Status](https://travis-ci.org/klarna/erlavro.svg?branch=master)](https://travis-ci.org/klarna/erlavro)

# Examples

## Load Avro Schema file(s) (demonstrating in Erlang shell)

See `priv/interop.avsc` for avro schema definition.

```erlang
1> Store = avro_schema_store:new([], ["priv/interop.avsc"]).
16400
2> Term = hd(element(3, avro_ocf:decode_file("priv/interop.ocf"))).
[{"intField",12},
 {"longField",15234324},
 {"stringField","hey"},
 {"boolField",true},
 {"floatField",1234.0},
 {"doubleField",-1234.0},
 {"bytesField",<<"12312adf">>},
 {"nullField",null},
 {"arrayField",[5.0,0.0,12.0]},
 {"mapField",
  [{"a",[{"label","a"}]},{"bee",[{"label","cee"}]}]},
 {"unionField",12.0},
 {"enumField","C"},
 {"fixedField",<<"1019181716151413">>},
 {"recordField",
  [{"label","blah"},
   {"children",[[{"label","inner"},{"children",[]}]]}]}]
3> Encoded = iolist_to_binary(avro_binary_encoder:encode(Store, "org.apache.avro.Interop", Term)).
<<24,168,212,195,14,6,104,101,121,1,0,64,154,68,0,0,0,0,0,
  72,147,192,16,49,50,51,49,50,97,...>>
4> Term =:= avro_binary_decoder:decode(Encoded, "org.apache.avro.Interop", Store).
true
```

## Define avro schema using erlavro APIs

### Avro Binary Encode/Decode

```erlang
MyRecordType = avro_record:type("MyRecord",
                                [avro_record:define_field("f1", avro_primitive:int_type()),
                                 avro_record:define_field("f2", avro_primitive:string_type())],
                                [{namespace, "my.com"}]),
Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
Term = [{"f1", 1},{"f2","my string"}],
Encoded = avro_binary_encoder:encode(Store, "my.com.MyRecord", Term),
Term =:= avro_binary_decoder:decode(Encoded, "my.com.MyRecord", Store).
```

### Avro JSON Encode/Decode

```erlang
MyRecordType = avro_record:type("MyRecord",
                                [avro_record:define_field("f1", avro_primitive:int_type()),
                                 avro_record:define_field("f2", avro_primitive:string_type())],
                                [{namespace, "my.com"}]),
Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
Term = [{"f1", 1},{"f2","my string"}],
JSON = iolist_to_binary(avro_json_encoder:encode(Store, "my.com.MyRecord", Term)),
Term =:= avro_json_decoder:decode_value(JSON, "my.com.MyRecord", Store, [{is_wrapped, false}]),
io:put_chars(user, JSON).
```

JSON to expect:

```json
{"f1":1,"f2":"my string"}
```

## Object container file encoding/decoding

See avro_ocf.erl for details

# TODOs

This version of library supports only subset of all functionality.
What things should be done:

1. Full support for avro 1.8

