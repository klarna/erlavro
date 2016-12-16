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
3> Encoder = avro:make_encoder(Store, []).
4> Decoder = avro:make_decoder(Store, []).
5> Encoded = iolist_to_binary(Encoder("org.apache.avro.Interop", Term)).
6> Term =:= Decoder("org.apache.avro.Interop", Encoded).
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
      [{namespace, "com.example"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:make_encoder(Store, []),
  Decoder = avro:make_decoder(Store, []),
  Term = [{"f1", 1},{"f2","my string"}],
  Bin = Encoder("com.example.MyRecord", Term),
  Term = Decoder("com.example.MyRecord", Bin),
  ok.
```

### Avro JSON Encode/Decode

```erlang
  MyRecordType =
    avro_record:type(
      "MyRecord",
      [avro_record:define_field("f1", avro_primitive:int_type()),
       avro_record:define_field("f2", avro_primitive:string_type())],
      [{namespace, "com.example"}]),
  Store = avro_schema_store:add_type(MyRecordType, avro_schema_store:new([])),
  Encoder = avro:make_encoder(Store, [{encoding, avro_json}]),
  Decoder = avro:make_decoder(Store, [{encoding, avro_json}]),
  Term = [{"f1", 1},{"f2", "my string"}],
  JSON = Encoder("com.example.MyRecord", Term),
  Term = Decoder("com.example.MyRecord", JSON),
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
      [{namespace, "com.example"}]),
  MyRecordType2 =
    avro_record:type(
      "MyRecord2",
      [avro_record:define_field("f1", avro_primitive:string_type()),
       avro_record:define_field("f2", NullableInt)],
      [{namespace, "com.example"}]),
  MyUnion = avro_union:type([MyRecordType1, MyRecordType2]),
  MyArray = avro_array:type(MyUnion),
  Lkup = fun(_) -> erlang:error("not expecting type lookup because "
                                "all types are fully constructed. "
                                "i.e. no name references") end,
  %% Encode Records with type info wrapped
  %% so they can be used as a drop-in part of wrapper object
  WrappedEncoder = avro:make_encoder(Lkup, [wrapped | CodecOptions]),
  T1 = [{"f1", null}, {"f2", "str1"}],
  T2 = [{"f1", "str2"}, {"f2", 2}],
  %% Encode the records with type info wrapped
  R1 = WrappedEncoder(MyRecordType1, T1),
  R2 = WrappedEncoder(MyRecordType2, T2),
  %% Tag the union values for better encoding performance
  U1 = {"com.example.MyRecord1", R1},
  U2 = {"com.example.MyRecord2", R2},
  %% This encoder returns iodata result without type info wrapped
  BinaryEncoder = avro:make_encoder(Lkup, CodecOptions),
  %% Construct the array from encoded elements
  Bin = iolist_to_binary(BinaryEncoder(MyArray, [U1, U2])),
  %% Tag the decoded values
  Hook = avro_decoder_hooks:tag_unions(),
  Decoder = avro:make_decoder(Lkup, [{hook, Hook} | CodecOptions]),
  [ {"com.example.MyRecord1", [{"f1", null}, {"f2", "str1"}]}
  , {"com.example.MyRecord2", [{"f1", "str2"}, {"f2", 2}]}
  ] = Decoder(MyArray, Bin),
  ok.
```

# Avro Type and Erlang Spec mapping

* null: `null`
  `undefined` is not accepted by encoder, and `null` is not converted to `undefined` by decoder
* boolean: `boolean() | 0 | 1`
* int: `-2147483648..2147483647`
* long: `-9223372036854775808..9223372036854775807`
* float: `integer() | float()`
* double: `integer() | float()`
* bytes: `binary()`
* string: `string()`
  `binary()` is not supported so far
* enum: `string()`
  `atom()` or `binary()` is not supported so far
* array: `list()`
* map: `[string(), term()]`
  `map()` is not supported so far
* fixed: `binary()`
* record: `[{FieldName :: string(), FieldValue :: term()}]`
  `map()` or `atom()` as `FiledName` is not supported so far
* union: `term() | {Tag :: string(), term()}`
  where Tag is the type name

# Decoder Hooks

Decoder hook is an anonymous function to be evaluated by the JSON or binary decoder to amend data before and/or after decoding. 
Hooks can be used to:

* Fast-skip undesired data fields of records or undesired data of big maps etc.
* Debug. e.g. `avro_decoer_hooks:print_debug_trace/2` gives you a hook which can print decode history and stack upon failure
* Monkey patch corrupted data.

The default decoder hook does nothing by passing along the decode calls:

```
fun(__Type__, __SubNameOrId__, Data, DecodeFun) ->
    DecodeFun(Data)
end
```

This is a typical way to implement a hook which actually does something

```
fun(Type, SubNameOrIndex, Data0, DecodeFun) ->
    Data = amend_data(Data0),
    Result = DecodeFun(Data),
    amend_result(Result)
end
```

You may find more details and a few examples in `avro_decoder_hooks.erl`

# Important Notes About Unions

### Union Values Should be Tagged with Type Name for Better Encoding Performance

For a big union like below

```
[
  "com.exmpale.MyRecord1",
  "com.example.MyRecord2",
  ... and many more ...
]
```

There are two ways to encode such unions

* Untagged: `Encoder(UnionType, MyRecord)` where `MyRecord` is of spec `[{field_name(), field_value()}]`
* Tagged: `Encoder(UnionType, MyRecord)` where `MyRecord` is of spec `{"com.example.MyRecordX", [{field_name(), field_value()}]}`

For `Untagged`, the encoder will have to TRY to encode using the union member types one after another until success. 
This is completely fine for small unions (e.g. a union of `null` and `long`), however quite expansive (and sometimes can be problematic) for records. 
Therefore we are recommending the `Tagged` way, because it'll help the encoder to find the member quickly.

### Union Values Are Decoded Without Tags by Default

A bit contradicting to the recommended union encoding, the union members are NOT tagged by decoder BY DEFAULT. 
Because we believe the use case of tagged unions in decoder output is not as common 
You may use the decoder hook `avro_decoer_hooks:tag_unions/0` to have the decoded values tagged. 
NOTE: only named complex types are tagged by this hook, you can of course write your own hook for a different tagging behaviour.

# Object container file encoding/decoding

See `avro_ocf.erl` for details


# TODOs

This version of library supports only subset of all functionality.
What things should be done:

1. Full support for avro 1.8
2. Support `atom() | binary()` as type names

