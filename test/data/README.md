Files in `test/data` are used in tests

Files downloaded from https://cwiki.apache.org/confluence/display/AVRO/Interoperability+Testing

- interop.avsc
- gen_interop_data.py

Generated files

- interop.ocf: ocf file generated from: `./gen_interop_data.py interop.avsc interop.ocf`
- interop_deflate.ocf: ocf file generated from: `./gen_interop_data.py --codec=deflate interop.avsc interop_deflate.ocf`
- interop_snappy.ocf: ocf file generated from: `./gen_interop_data.py --codec=snappy interop.avsc interop_snappy.ocf`