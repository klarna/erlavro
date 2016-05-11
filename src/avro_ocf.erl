%% coding: latin-1
%%% ============================================================================
%%% Copyright (c) 2016 Klarna AB
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%
%%% @doc
%%% Encode/decode avro object container files
%%% @end
%%% ============================================================================

-module(avro_ocf).

-export([ append_file/5
        , decode_file/1
        , make_header/1
        , write_header/2
        , write_file/4
        ]).

-export_type([ header/0
             ]).

-include("erlavro.hrl").

-type avro_object() :: term().
-type schema_store() :: avro_schema_store:store().
-type filename() :: file:filename_all().

-record(header, { magic
                , meta
                , sync
                }).

-opaque header() :: #header{}.

%%%_* APIs =====================================================================

%% @doc Decode ocf into unwrapped values.
-spec decode_file(filename()) -> {header(), avro_type(), [avro_object()]}.
decode_file(Filename) ->
  {ok, Bin} = file:read_file(Filename),
  {{_, [ {"magic", Magic}
       , {"meta", Meta}
       , {"sync", Sync}
       ]}, Tail} = decode_stream(undefined, ocf_schema(), Bin),
  {_, SchemaBytes} = lists:keyfind("avro.schema", 1, Meta),
  {_, Codec} = lists:keyfind("avro.codec", 1, Meta),
  <<"null">> = Codec, %% assert, no support for deflate so far
  Schema = avro_json_decoder:decode_schema(SchemaBytes),
  Store = init_schema_store(Schema),
  Header = #header{ magic = Magic
                  , meta  = Meta
                  , sync  = Sync
                  },
  try
    {Header, Schema, decode_blocks(Store, Schema, Sync, Tail, [])}
  after
    avro_schema_store:close(Store)
  end.

%% @doc Write objects in a single block to the given file name.
-spec write_file(filename(), schema_store(), avro_type(), [term()]) -> ok.
write_file(Filename, SchemaStore, Schema, Objects) ->
  Header = make_header(Schema),
  ok = write_header(Filename, Header),
  {ok, Fd} = file:open(Filename, [write]),
  try
    ok = append_file(Fd, Header, SchemaStore, Schema, Objects)
  after
    file:close(Fd)
  end.

%% @doc Writer header bytes to a ocf file.
-spec write_header(filename(), avro_type() | header()) -> ok.
write_header(Filename, #header{} = Header) ->
  HeaderFields =
    [ {"magic", Header#header.magic}
    , {"meta", Header#header.meta}
    , {"sync", Header#header.sync}
    ],
  HeaderRecord = avro_record:new(ocf_schema(), HeaderFields),
  HeaderBytes = avro_binary_encoder:encode_value(HeaderRecord),
  ok = file:write_file(Filename, HeaderBytes);
write_header(Filename, Type) ->
  write_header(Filename, make_header(Type)).

%% @doc Append a block ocf block to the opened IO device.
-spec append_file(file:io_device(), header(),
                  schema_store(), avro_type(), [term()]) -> ok.
append_file(Fd, Header, SchemaStore, Schema, Objects) ->
  Count = length(Objects),
  LkupFun =
    fun(Name) ->
      {ok, T} = avro_schema_store:lookup_type(Name, SchemaStore),
      T
    end,
  Bytes = iolist_to_binary([avro_binary_encoder:encode(LkupFun, Schema, O)
                            || O <- Objects]),
  IoData = [ avro_binary_encoder:encode_value(avro_primitive:long(Count))
           , avro_binary_encoder:encode_value(avro_primitive:long(size(Bytes)))
           , Bytes
           , Header#header.sync
           ],
  ok = file:write(Fd, IoData).

%% @private Make ocf file header.
-spec make_header(avro_type()) -> header().
make_header(Type) ->
  TypeJson = avro_json_encoder:encode_type(Type),
  #header{ magic = <<"Obj1">>
         , meta  = [ {"avro.schema", iolist_to_binary(TypeJson)}
                   , {"avro.codec", <<"null">>}
                   ]
         , sync  = generate_sync_bytes()
         }.

%%%_* Internal functions =======================================================

-spec generate_sync_bytes() -> binary().
generate_sync_bytes() -> crypto:rand_bytes(16).

-spec decode_stream(schema_store() | undefined,
                    avro_type(), binary()) -> [term()].
decode_stream(SchemaStore, Type, Bin) when is_binary(Bin) ->
  TypeLkupFun =
    fun(X) ->
      case avro_schema_store:lookup_type(X, SchemaStore) of
        {ok, T} -> T;
        false   -> erlang:error({unexpected, X})
      end
    end,
  avro_binary_decoder:decode_stream(Bin, Type, TypeLkupFun).

decode_blocks(_Store, _Type, _Sync, <<>>, Acc) ->
  lists:reverse(Acc);
decode_blocks(Store, Type, Sync, Bin0, Acc) ->
  LongType = avro_primitive:long_type(),
  {Count, Bin1} = decode_stream(Store, LongType, Bin0),
  {Size, Bin} = decode_stream(Store, LongType, Bin1),
  <<Block:Size/binary, Sync:16/binary, Tail/binary>> = Bin,
  NewAcc = decode_block(Store, Type, Block, Count, Acc),
  decode_blocks(Store, Type, Sync, Tail, NewAcc).

decode_block(_Store, _Type, <<>>, 0, Acc) -> Acc;
decode_block(Store, Type, Bin, Count, Acc) ->
  {Obj, Tail} = decode_stream(Store, Type, Bin),
  decode_block(Store, Type, Tail, Count - 1, [Obj | Acc]).

%% @private Hande coded schema.
%% {"type": "record", "name": "org.apache.avro.file.Header",
%% "fields" : [
%%   {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
%%   {"name": "meta", "type": {"type": "map", "values": "bytes"}},
%%   {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
%%  ]
%% }
%% @end
-spec ocf_schema() -> avro_type().
ocf_schema() ->
  MagicType = avro_fixed:type("magic", 4),
  MetaType = avro_map:type(avro_primitive:bytes_type()),
  SyncType = avro_fixed:type("sync", 16),
  Fields = [ avro_record:define_field("magic", MagicType)
           , avro_record:define_field("meta", MetaType)
           , avro_record:define_field("sync", SyncType)
           ],
  avro_record:type("org.apache.avro.file.Header", Fields).

%% @private Create and initialize schema store from schema decoded from.
-spec init_schema_store(avro_type()) -> avro_schema_store:store().
init_schema_store(Schema) ->
  Store = avro_schema_store:new([]),
  avro_schema_store:add_type(Schema, Store).

%%%_* Tests ====================================================================

-include_lib("eunit/include/eunit.hrl").

interop_test() ->
  PrivDir = priv_dir(),
  InteropOcfFile = filename:join([PrivDir, "interop.ocf"]),
  {Header, Schema, Objects} = decode_file(InteropOcfFile),
  SchemaStore = init_schema_store(Schema),
  MyFile = filename:join([PrivDir, "interop.ocf.test"]),
  ok = write_header(MyFile, Header),
  {ok, Fd} = file:open(MyFile, [write, append]),
  try
    ok = append_file(Fd, Header, SchemaStore, Schema, Objects)
  after
    file:close(Fd)
  end,
  {Header1, Schema1, Objects1} = decode_file(MyFile),
  ?assertEqual(Header#header{meta = []}, Header1#header{meta = []}),
  ?assertEqual(lists:keysort(1, Header#header.meta),
               lists:keysort(1, Header1#header.meta)),
  ?assertEqual(Schema, Schema1),
  ?assertEqual(Objects, Objects1).

priv_dir() ->
  case filelib:is_dir(filename:join(["..", priv])) of
    true -> filename:join(["..", priv]);
    _    -> "./priv"
  end.

-ifdef(EUNIT).

-endif.
%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
