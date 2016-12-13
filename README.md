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
4> Term =:= avro:decode(Encoded, "org.apache.avro.Interop", Store, avro_binary).
true
```

## Define avro schema using erlavro APIs

### Avro Binary Encode/Decode

```erlang
  MyRecordType =
    avro_record:type(
      "MyRecord",
      [avro_record:define_field("f1", avro_primitive:int_type()),
       avro_record:define_field("f2", avro_primitive:string_type())],
      [{namespace, "my.com"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:get_encoder(Store, []),
  Decoder = avro:get_decoder(Store, []),
  Term = [{"f1", 1},{"f2","my string"}],
  Bin = Encoder("my.com.MyRecord", Term),
  Term = Decoder(Bin, "my.com.MyRecord"),
  ok.
```

### Avro JSON Encode/Decode

```erlang
  MyRecordType =
    avro_record:type(
      "MyRecord",
      [avro_record:define_field("f1", avro_primitive:int_type()),
       avro_record:define_field("f2", avro_primitive:string_type())],
      [{namespace, "my.com"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:get_encoder(Store, [{encoding, avro_json}]),
  Decoder = avro:get_decoder(Store, [{encoding, avro_json}]),
  Term = [{"f1", 1},{"f2", "my string"}],
  JSON = Encoder("my.com.MyRecord", Term),
  Term = Decoder(JSON, "my.com.MyRecord"),
  io:put_chars(user, JSON),
  ok.
```

JSON to expect:

```json
{"f1":1,"f2":"my string"}
```

### Encoded Value as a Part of Parent Object

```erlang
  CodecOptions = [], %% [{encoding, avro_json}] for JSON encode/decode
  NullableInt = avro_union:type([avro_primitive:null_type(),
                                 avro_primitive:int_type()]),
  MyRecordType1 =
    avro_record:type(
      "MyRecord1",
      [avro_record:define_field("f1", NullableInt),
       avro_record:define_field("f2", avro_primitive:string_type())],
      [{namespace, "my.com"}]),
  MyRecordType2 =
    avro_record:type(
      "MyRecord2",
      [avro_record:define_field("f1", avro_primitive:string_type()),
       avro_record:define_field("f2", NullableInt)],
      [{namespace, "my.com"}]),
  MyUnion = avro_union:type([MyRecordType1, MyRecordType2]),
  MyArray = avro_array:type(MyUnion),
  Lkup = fun(_) -> erlang:error("not expecting type lookup because "
                                "all types are fully constructed. "
                                "i.e. no name references") end,
  %% Encode Records with type info wrapped
  %% so they can be used as a drop-in part of wrapper object
  WrappedEncoder = avro:get_encoder(Lkup, [wrapped | CodecOptions]),
  T1 = [{"f1", null}, {"f2", "str1"}],
  T2 = [{"f1", "str2"}, {"f2", 2}],
  %% Encode the records with type info wrapped
  R1 = WrappedEncoder(MyRecordType1, T1),
  R2 = WrappedEncoder(MyRecordType2, T2),
  %% Tag the union values for better encoding performance
  U1 = {"my.com.MyRecord1", R1},
  U2 = {"my.com.MyRecord2", R2},
  %% This encoder returns iodata result without type info wrapped
  BinaryEncoder = avro:get_encoder(Lkup, CodecOptions),
  %% Construct the array from encoded elements
  Bin = iolist_to_binary(BinaryEncoder(MyArray, [U1, U2])),
  %% Tag the decoded values
  Hook = avro_decoder_hooks:tag_unions_fun(),
  Decoder = avro:get_decoder(Lkup, [{hook, Hook} | CodecOptions]),
  [ {"my.com.MyRecord1", [{"f1", null}, {"f2", "str1"}]}
  , {"my.com.MyRecord2", [{"f1", "str2"}, {"f2", {"int", 2}}]}
  ] = Decoder(Bin, MyArray),
  ok.
```

## Decoder Hooks

Decoder hook is an anonymous function to be evaluated by the JSON or binary decoder to amend schmea and|or data before and|or after decoding.

* A hook can be used to fast-skip undesired data fields of records or undesired data of big maps etc.
* A hook can be used for debug. e.g. `avro_decoer_hooks:make_binary_decoder_debug_hook/2' gives you a hook which can print decode history and stack upon failure
* A hook can also be used as a monkey patch to fix some corrupted data.

Find the examples in `avro_decoder_hooks.erl'

## NOTEs About Unions

### Union Values Are Better to be Encoded With Tags

In case a union value is not tagged with a type name, the encoder will have to 
try to loop over all union members to encode until succeed. This is not quite 
efficent when the union is relatively big

### Union Values Are Decoded Without Tags by Default

However, you may use the anonymous functions returned from 
`avro_decoer_hooks:tag_unions_fun/0' as a decode hook to get 
the decoded values tagged.

## Object container file encoding/decoding

See avro_ocf.erl for details

# TODOs

This version of library supports only subset of all functionality.
What things should be done:

1. Full support for avro 1.8

