Avro support for Erlang (http://avro.apache.org/).

Current version implements Apache Avro 1.7.5 specification.

License: Apache License 2.0

Compilation
-----------

   make

Dependencies: jsonx and mochijson3 (see rebar.config).

Technical information
---------------------

Every value is stored together with its type in #avro_value{} record where data
has different meaning depending on the type. It is recommended to use ?AVRO_VALUE*
macros defined in erlavro.hrl to work with this record.

Set of macros are used to check types of values or types themselves:
?AVRO_IS_*_TYPE(T) checks that T is a specified type.
?AVRO_IS_*_VALUE(V) checks that the value has specified type.
Such macros can be used in external code to speed up matching.

Erlang version doesn't make a difference between float and double values.

To do
-----

This version of library supports only subset of all functionality.
What things should be done:

1. Parsing Canonical Form.
2. Object (avro_schema) to manage all types included in the schema,
   with consistency checks, etc. (DONE without consistency checks).
3. Better coverage of possible operations by modules, so ?AVRO_VALUE*
   macros are not exposed to external code.
