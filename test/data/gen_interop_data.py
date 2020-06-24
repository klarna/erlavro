#!/usr/bin/env python2

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import getopt
import sys
from avro import schema
from avro import io
from avro import datafile

DATUM = {
  'intField': 12,
  'longField': 15234324L,
  'stringField': unicode('hey'),
  'boolField': True,
  'floatField': 1234.0,
  'doubleField': -1234.0,
  'bytesField': '12312adf',
  'nullField': None,
  'arrayField': [5.0, 0.0, 12.0],
  'mapField': {'a': {'label': 'a'}, 'bee': {'label': 'cee'}},
  'unionField': 12.0,
  'enumField': 'C',
  'fixedField': '1019181716151413',
  'recordField': {'label': 'blah', 'children': [{'label': 'inner', 'children': []}]},
}

def write(interop_schema, writer, codec):
  datum_writer = io.DatumWriter()
  dfw = datafile.DataFileWriter(writer, datum_writer, interop_schema, codec=codec)
  dfw.append(DATUM)
  dfw.close()

if __name__ == "__main__":
  codec = 'null'
  opts, args = getopt.getopt(sys.argv[1:], 'c:', ['codec='])
  for o, a in opts:
    if o == '--codec' or o == '-c':
      codec = a
      
  interop_schema = schema.parse(open(args[0], 'r').read())
  writer = open(args[1], 'wb')
  write(interop_schema, writer, codec)
