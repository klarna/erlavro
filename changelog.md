* 2.4.0
   - Support dict schema store, avro:make_decoder and avro:make_encoder accepts JSON directly
   - Fix default value encode/decode and validation
     Encoder now makes use of record field default value in case the field is mssing in input
   - Type reference validation options for avro:decode_schema/2
   - Changed default build from rebar to rebar3

* 2.5.0
   - Add an Erlang implementation of the Avro CRC 64 fingerprint algorithm, avro:crc64_fingerprint/1.
