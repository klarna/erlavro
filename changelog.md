* 2.4.0
   - Support dict schema store, avro:make_decoder and avro:make_encoder accepts JSON directly
   - Fix default value encode/decode and validation
     Encoder now makes use of record field default value in case the field is mssing in input
   - Type reference validation options for avro:decode_schema/2
   - Changed default build from rebar to rebar3

* 2.5.0
   - Add an Erlang implementation of the Avro CRC 64 fingerprint algorithm, avro:crc64_fingerprint/1.
   - Add `avro:make_simple_decoder/2` and `avro:make_simple_encoder/2` to simplify most common use cases.
     `avro:make_decoder/2` and `avro:make_encoder/2` are not deprecated and can still be used when
     there is a need to use the same decoer/encoder for multiple schema.
   - Remove fullname-derived-namespace = namespace-in-type-attributes assertition (#60)
   - Add avro_ocf:make_ocf/2 to allow building ocf content in memory
* 2.6.0
   - Support Parsing Canonical Form for Schemas (contributer @congini @reachfh)

