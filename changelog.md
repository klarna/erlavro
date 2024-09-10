* 2.10.2
   - Fix bytes and fixed JSON value decode
* 2.10.1
   - Fix dialyzer error.
* 2.10.0
   - Add map as avro store, and use it as default.
   - Changed to store type aliases as type's full name index, so the type store map (or dict) is less bloated.
* 2.9.10
   - Optimize avro:is_same_type/2
   - Upgrade jsone to 1.8.1
* 2.9.9
   - Enable builds with gcc-13
   - Improve `@aliases` support
* 2.9.8
   - Removed support for Rebar 2
* 2.9.7
   - Export OCF make_header and make_block functions
* 2.9.6
   - Add snappy compression for OCF
* 2.9.5
   - Removed workarounds for OTP < 21
   - Add map() to the avro:out() union type
* 2.9.4
   - Dropped `?MODULE` from required error in erlavro.hrl to allow elixir to extract record details
* 2.9.3
   - Add OCF decoder hook
   - Dialyzer fixes
* 2.9.2
   - Move test files from priv to test/data
* 2.9.1
   - Add `true` and `false` to the atom exceptions when converting to strings
* 2.9.0
   - Encode atom values for string types when not `nil` or `null`
* 2.8.3
   - Allow arbitrary atoms in record_opt_name()
* 2.8.2
   - Encode atom `nil` as "null" for Elixir consumers
* 2.8.1
   - Support 'object' as custom type properties
* 2.8.0
   - Improve varint encoding performance
   - Support erlang map for avro record and map type in encoder
   - Allow decoder to decode avro record and map type as erlang map
     based on `record_type` and `map_type` options.
* 2.7.1
   - Issue #81 allow missing codec in ocf
* 2.7.0
   - Add schema compatibility check `avro:is_compatible/2`
* 2.6.5
   - Refine macros for `get_stacktrace` deprecation
* 2.6.4
   - Do not conditionally export functions
* 2.6.3
   - Upgrade jsone from 1.4.3 to 1.4.6 (for OTP-21)
* 2.6.2
   - Support Erlang 21.0 stacktraces
* 2.6.1
   - Allow `"null"` string as default value for 'null' type in union type record fields (contributer bka9)
* 2.6.0
   - Support Parsing Canonical Form for Schemas (contributer congini/@reachfh)
* 2.5.0
   - Add an Erlang implementation of the Avro CRC 64 fingerprint algorithm, avro:crc64_fingerprint/1.
   - Add `avro:make_simple_decoder/2` and `avro:make_simple_encoder/2` to simplify most common use cases.
     `avro:make_decoder/2` and `avro:make_encoder/2` are not deprecated and can still be used when
     there is a need to use the same decoer/encoder for multiple schema.
   - Remove fullname-derived-namespace = namespace-in-type-attributes assertition (#60)
   - Add avro_ocf:make_ocf/2 to allow building ocf content in memory
* 2.4.0
   - Support dict schema store, avro:make_decoder and avro:make_encoder accepts JSON directly
   - Fix default value encode/decode and validation
     Encoder now makes use of record field default value in case the field is mssing in input
   - Type reference validation options for avro:decode_schema/2
   - Changed default build from rebar to rebar3
