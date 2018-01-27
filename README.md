Avro support for Erlang/Elixir (http://avro.apache.org/).

Current version implements Apache Avro 1.8.1 specification.

License: Apache License 2.0

[![Build Status](https://travis-ci.org/klarna/erlavro.svg?branch=master)](https://travis-ci.org/klarna/erlavro)

# Avro Type and Erlang Spec Mapping

```
name_raw() :: atom() | string() | binary().
name() :: binary().
key_raw() :: atom() | sting() | binary().
key() :: binary().
tag() :: binary().
```

| Avro    | Encoder Input           | Decoder Output            | Notes                                       |
| ------- | ----------------------- | ------------------------- | ------------------------------------------- |
| null    | `null`                  | `null`                    | No implicit `undefined` transformation      |
| boolean | `boolean()`             | `boolean()`               |                                             |
| int     | `integer()`             | `integer()`               | `-2147483648..2147483647`                   |
| long    | `integer()`             | `integer()`               | `-9223372036854775808..9223372036854775807` |
| float   | `integer() \| float()`  | `float()`                 |                                             |
| double  | `integer() \| float()`  | `float()`                 |                                             |
| bytes   | `binary()`              | `binary()`                |                                             |
| string  | `iolist()`              | `binary()`                |                                             |
| enum    | `name_raw()`            | `name()`                  |                                             |
| fixed   | `binary()`              | `binary()`                |                                             |
| array   | `[in()]`                | `[out()]`                 |                                             |
| map     | `[{key_raw(), in()}]`   | `[{key(), out()}]`        |                                             |
| record  | `[{name_raw(), in()}]`  | `[{name(), out()}]`       |                                             |
| union   | `in() \| {tag(), in()}` | `out() \| {tag(), out()}` | See notes about unions below                |

Where `in()` and `out()` refer to the input and output type specs recursively.

## Important Notes about Unicode Strings

The binary encoder/decoder will respect whatever is given in the input (bytes).
i.e. The encoder will NOT try to be smart and encode the input `string()` to utf8 (or whatsoever),
and the decoder will not try to validate or decode the input `binary()` as unicode character list.

The encode caller should make sure the input is of spec `[byte()] | binary()`,
NOT a unicode character list which may possibly contain some code points greater than 255.

# Examples

## Load Avro Schema file(s) (demonstrating in Erlang shell)

See `priv/interop.avsc` for avro schema definition.

```erlang
1> {ok, SchemaJSON} = file:read_file("priv/interop.avsc").
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
3> Encoder = avro:make_simple_encoder(SchemaJSON, []).
4> Decoder = avro:make_simple_decoder(SchemaJSON, []).
5> Encoded = iolist_to_binary(Encoder(Term)).
6> Term =:= Decoder(Encoded).
true
```

## Define avro schema using erlavro APIs

### Avro Binary Encode/Decode

```erlang
  MyRecordType =
    avro_record:type(
      <<"MyRecord">>,
      [avro_record:define_field(f1, int),
       avro_record:define_field(f2, string)],
      [{namespace, 'com.example'}]),
  Encoder = avro:make_simple_encoder(MyRecordType, []),
  Decoder = avro:make_simple_decoder(MyRecordType, []),
  Term = [{<<"f1">>, 1}, {<<"f2">>, <<"my string">>}],
  Bin = Encoder(Term),
  [{<<"f1">>, 1}, {<<"f2">>, <<"my string">>}] = Decoder(Bin),
  ok.
```

### Avro JSON Encode/Decode

```erlang
  MyRecordType =
    avro_record:type(
      "MyRecord",
      [avro_record:define_field("f1", int),
       avro_record:define_field("f2", string)],
      [{namespace, "com.example"}]),
  Encoder = avro:make_simple_encoder(MyRecordType, [{encoding, avro_json}]),
  Decoder = avro:make_simple_decoder(MyRecordType, [{encoding, avro_json}]),
  Term = [{<<"f1">>, 1}, {<<"f2">>, <<"my string">>}],
  JSON = Encoder(Term),
  Term = Decoder(JSON),
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
  NullableInt = avro_union:type([null, int]),
  MyRecordType1 =
    avro_record:type(
      "MyRecord1",
      [avro_record:define_field("f1", NullableInt),
       avro_record:define_field("f2", string)],
      [{namespace, "com.example"}]),
  MyRecordType2 =
    avro_record:type(
      "MyRecord2",
      [avro_record:define_field("f1", string),
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
  T1 = [{"f1", null}, {"f2", <<"str1">>}],
  T2 = [{"f1", <<"str2">>}, {"f2", 2}],
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
  [ {<<"com.example.MyRecord1">>, [{<<"f1">>, null}, {<<"f2">>, <<"str1">>}]}
  , {<<"com.example.MyRecord2">>, [{<<"f1">>, <<"str2">>}, {<<"f2">>, 2}]}
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
This is completely fine for small unions (e.g. a union of `null` and `long`), however quite expensive (and sometimes can be problematic) for records.
Therefore we are recommending the `Tagged` way, because it'll help the encoder to find the member quickly.

### Caution when unioning string type and int/long arrays.

As `[integer()]` list is `string()` in Erlang, this will confuse the encoder.
Please make sure to use `binary()` as avro string encoding input or tag it,
and always tag int/long array value like `{array, [1, 2, 3]}`.

### Union Values Are Decoded Without Tags by Default

A bit contradicting to the recommended union encoding, the decoded values are NOT tagged by DEFAULT.
Because we believe the use case of tagged unions in decoder output is not as common.
You may use the decoder hook `avro_decoer_hooks:tag_unions/0` to have the decoded values tagged.
NOTE: only named complex types are tagged by this hook, you can of course write your own hook for a different tagging behaviour.

# Object container file encoding/decoding

See `avro_ocf.erl` for details

# Logical types and custom type properties.

NOTE: There is no logical type or custom type properties based on avro 'union' type.

`erlavro` encodes/decodes logical types as well as custom type properties,
but (so far) does not validate or transform the encoder/encoder input/output.

e.g. The underlying data type of 'Date' logical type is 'int', in a perfect world,
the encoder should accept `{Y, M, D}` as input and the decoder should transform the integer
back to `{Y, M, D}` --- but this is not supported so far.

Call `avro:get_custom_props/2` to access logical type info (as well as any extra customized type properties)
for extra data validation/transformation at application level.

