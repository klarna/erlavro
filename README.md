Avro support for Erlang (http://avro.apache.org/).

Current version implements Apache Avro 1.7.5 specification.

License: Apache License 2.0

[![Build Status](https://travis-ci.org/klarna/erlavro.svg?branch=master)](https://travis-ci.org/klarna/erlavro)

# Avro Type and Erlang Spec Mapping

| Avro | Encoder Input | Decoder Output | Notes |
| --- | --- | --- | --- |
| null | `null` | `null` | `undefined` is not accepted by encoder, and `null` is not converted to `undefined` by decoder |
| boolean | `boolean() | 0 | 1` | `boolean()` | |
| int | `integer()` | `integer()` | `-2147483648..2147483647` |
| long | `integer()` | `integer()` | `-9223372036854775808..9223372036854775807` |
| float | `integer() | float()` | `float()` | |
| double | `integer() | float()` | `float()` | |
| bytes | `binary()` | `binary()` | |
| string | `[byte()] | binary()` | `[byte()]` | NOT `iolist()` for encoder. Will change decoder output to `binary()` in 2.0 |
| enum | `string()` | `string()` | `atom()` or `binary()` is not supported so far |
| fixed | `binary()` | `binary()` | |
| array | `list()` | `list()` | |
| map | `[{Key::string(), Value::in()}]` | `[{Key::string(), Value::out()}]` | Will support `atom() | binary()` as Key in 2.0 |
| record | `[{FieldName::string(), FieldValue::in()}]` | `[{FieldName::string(), FiledValue::out()}]` | Will support `atom()` as `FiledName` for encoder in 2.0; User may implement decoder hook to get `FieldName` decoded as `atom()` |
| union | `in() | {Tag::string(), in()}`  | `out() | {Tag::string(), out()}` | Tag is the type name, See notes about unions below |

Where `in()` and `out()` refer to the input and output type specs recursively

## Important Notes about Unicode Strings

The binary encoder/decoder will respect whatever is given in the input (bytes). 
i.e. The encoder will NOT try to be smart and encode the input `string()` to utf8 (or whatsoever), 
and the decoder will not try to validate or decode the input `binary()` as unicode character list. 

The encoder caller should make sure the input is of spec `[byte()] | binary()`, 
NOT a unicode character list which may possibly contain some code points greater than 255.

For historical reason, the JSON encoder will try to encode the string in utf8. 
And the JSON decoder will try to validate the input strings as utf8 -- as it's how mochijson3 implemented

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
  Term = [{"f1", 1}, {"f2", "my string"}],
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
  Term = [{"f1", 1}, {"f2", "my string"}],
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

# Decoder Hooks

Decoder hook is an anonymous function to be evaluated by the JSON or binary decoder to amend data before and/or after decoding. 
Some hook use cases for example:

* Tag union value with type name. e.g. `avro_decoder_hooks:tag_unions/0`.
* Apply `string_to_atom/1` on record field names or map keys.
* Debugging. e.g. `avro_decoer_hooks:print_debug_trace/2` gives you a hook which can print decode history and stack upon failure.
* For JSON decoder, fast-skip undesired data fields in records or keys in maps.
* Monkey patching corrupted data.

The default decoder hook does nothing but just passing through the decode call:

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
You can of course also splice-up two hooks by one wrapping around the other:

```
Hook1 = fun(T, S, D, F) -> ... end,
fun(Type, SubNameOrIndex, Data0, DecodeFun) ->
  Data = amend_data(Data0),
  Result = Hook1(Type, SubNameOrIndex, Data, DecodeFun),
  amend_result(Result)
end
```

Please find more details and a few examples in `avro_decoder_hooks.erl`

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

A bit contradicting to the recommended union encoding, the decoded values are NOT tagged by DEFAULT. 
Because we believe the use case of tagged unions in decoder output is not as common. 
You may use the decoder hook `avro_decoer_hooks:tag_unions/0` to have the decoded values tagged. 
NOTE: only named complex types are tagged by this hook, you can of course write your own hook for a different tagging behaviour.

# Object container file encoding/decoding

See `avro_ocf.erl` for details

# TODOs

1. Full support for avro 1.8
2. Support `atom() | binary()` as type names
3. Decoded string should be binary() not [byte()] -- for 2.0 as it brakes backward compatibility

