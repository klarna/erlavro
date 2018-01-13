* 2.4.0
   - Support dict schema store, avro:make_decoder and avro:make_encoder accepts JSON directly
   - Fix default value encode/decode and validation
     Encoder now makes use of record field default value in case the field is mssing in input
   - Type reference validation options for avro:decode_schema/2

