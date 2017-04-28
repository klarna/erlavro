%% coding: latin-1
%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2016-2017 Klarna AB
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
%%%-----------------------------------------------------------------------------

-module(avro_ocf).

-export([ append_file/5
        , decode_binary/1
        , decode_blocks/5
        , decode_file/1
        , decode_stream/2
        , make_header/1
        , write_header/2
        , write_file/4
        , ocf_schema/0
        ]).

-export_type([ header/0
             ]).

-include("avro_internal.hrl").

-ifdef(TEST).
-export([init_schema_store/1]).
-endif.

-type filename() :: file:filename_all().

-record(header, { magic
                , meta
                , sync
                }).

-opaque header() :: #header{}.

%%%_* APIs =====================================================================

%% @doc Decode ocf into unwrapped values.
-spec decode_file(filename()) -> {header(), avro_type(), [avro:out()]}.
decode_file(Filename) ->
  {ok, Bin} = file:read_file(Filename),
  decode_binary(Bin).

%% @doc Decode ocf binary into unwrapped values.
-spec decode_binary(binary()) -> {header(), avro_type(), [avro:out()]}.
decode_binary(Bin) ->
  {[ {<<"magic">>, Magic}
   , {<<"meta">>, Meta}
   , {<<"sync">>, Sync}
   ], Tail} = decode_stream(ocf_schema(), Bin),
  {_, SchemaBytes} = lists:keyfind(<<"avro.schema">>, 1, Meta),
  {_, Codec} = lists:keyfind(<<"avro.codec">>, 1, Meta),
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
-spec write_file(filename(), schema_store(),
                 type_or_name(), [avro:in()]) -> ok.
write_file(Filename, SchemaStore, Schema, Objects) ->
  Header = make_header(Schema),
  {ok, Fd} = file:open(Filename, [write]),
  try
    ok = write_header(Fd, Header),
    ok = append_file(Fd, Header, SchemaStore, Schema, Objects)
  after
    file:close(Fd)
  end.

%% @doc Writer header bytes to a ocf file.
-spec write_header(file:io_device(), header()) -> ok.
write_header(Fd, Header) ->
  HeaderFields =
    [ {"magic", Header#header.magic}
    , {"meta", Header#header.meta}
    , {"sync", Header#header.sync}
    ],
  HeaderRecord = avro_record:new(ocf_schema(), HeaderFields),
  HeaderBytes = avro_binary_encoder:encode_value(HeaderRecord),
  ok = file:write(Fd, HeaderBytes).

%% @doc Append a block ocf block to the opened IO device.
-spec append_file(file:io_device(), header(), schema_store(),
                  type_or_name(), [avro:in()]) -> ok.
append_file(Fd, Header, SchemaStore, Schema, Objects) ->
  Count = length(Objects),
  Bytes = iolist_to_binary([avro_binary_encoder:encode(SchemaStore, Schema, O)
                            || O <- Objects]),
  IoData = [ avro_binary_encoder:encode_value(avro_primitive:long(Count))
           , avro_binary_encoder:encode_value(avro_primitive:long(size(Bytes)))
           , Bytes
           , Header#header.sync
           ],
  ok = file:write(Fd, IoData).

%% @doc Make ocf header.
-spec make_header(avro_type()) -> header().
make_header(Type) ->
  TypeJson = avro_json_encoder:encode_type(Type),
  #header{ magic = <<"Obj", 1>>
         , meta  = [ {<<"avro.schema">>, iolist_to_binary(TypeJson)}
                   , {<<"avro.codec">>, <<"null">>}
                   ]
         , sync  = generate_sync_bytes()
         }.

%%%_* Internal functions =======================================================

%% @private
-spec generate_sync_bytes() -> binary().
generate_sync_bytes() -> crypto:strong_rand_bytes(16).

%% @private
-spec decode_stream(avro_type(), binary()) -> {avro:out(), binary()}.
decode_stream(Type, Bin) when is_binary(Bin) ->
  Lkup = fun(_) -> erlang:error(unexpected) end,
  avro_binary_decoder:decode_stream(Bin, Type, Lkup).

%% @private
-spec decode_stream(schema_store(), avro_type(), binary()) ->
        {avro:out(), binary()} | no_return().
decode_stream(SchemaStore, Type, Bin) when is_binary(Bin) ->
  avro_binary_decoder:decode_stream(Bin, Type, SchemaStore).

%% @private
-spec decode_blocks(schema_store(), avro_type(),
                    binary(), binary(), [avro:out()]) -> [avro:out()].
decode_blocks(_Store, _Type, _Sync, <<>>, Acc) ->
  lists:reverse(Acc);
decode_blocks(Store, Type, Sync, Bin0, Acc) ->
  LongType = avro_primitive:long_type(),
  {Count, Bin1} = decode_stream(Store, LongType, Bin0),
  {Size, Bin} = decode_stream(Store, LongType, Bin1),
  <<Block:Size/binary, Sync:16/binary, Tail/binary>> = Bin,
  NewAcc = decode_block(Store, Type, Block, Count, Acc),
  decode_blocks(Store, Type, Sync, Tail, NewAcc).

%% @private
-spec decode_block(schema_store(), avro_type(),
                   binary(), integer(), [avro:out()]) -> [avro:out()].
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
-spec init_schema_store(avro_type()) -> schema_store().
init_schema_store(Schema) ->
  Store = avro_schema_store:new([]),
  avro_schema_store:add_type(erlavro_ocf_root, Schema, Store).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
