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

-export([ append_file/3
        , append_file/5
        , decode_binary/1
        , decode_file/1
        , make_header/1
        , make_header/2
        , write_header/2
        , write_file/4
        , write_file/5
        ]).

-export_type([ header/0
             , extra_meta/0
             ]).

-include("avro_internal.hrl").

-ifdef(TEST).
-export([init_schema_store/1]).
-endif.

-type filename() :: file:filename_all().
-type extra_meta() :: [{string() | binary(), binary()}].

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
  Codec = get_codec(Meta),
  Schema = avro_json_decoder:decode_schema(SchemaBytes),
  Store = init_schema_store(Schema),
  Header = #header{ magic = Magic
                  , meta  = Meta
                  , sync  = Sync
                  },
  try
    {Header, Schema, decode_blocks(Store, Schema, Codec, Sync, Tail, [])}
  after
    avro_schema_store:close(Store)
  end.

%% @doc Write objects in a single block to the given file name.
-spec write_file(filename(), schema_store(),
                 type_or_name(), [avro:in()]) -> ok.
write_file(Filename, SchemaStore, Schema, Objects) ->
  write_file(Filename, SchemaStore, Schema, Objects, []).

%% @doc Write objects in a single block to the given file name.
-spec write_file(filename(), schema_store(),
                 type_or_name(), [avro:in()], extra_meta()) -> ok.
write_file(Filename, SchemaStore, Schema, Objects, ExtraMeta) ->
  Header = make_header(Schema, ExtraMeta),
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

%% @doc Append encoded objects to the file as one data block.
-spec append_file(file:io_device(), header(), [binary()]) -> ok.
append_file(Fd, Header, Objects) ->
  Count = length(Objects),
  Data = encode_block(Header#header.meta, Objects),
  Size = size(Data),
  ToWrite =
    [ avro_binary_encoder:encode_value(avro_primitive:long(Count))
    , avro_binary_encoder:encode_value(avro_primitive:long(Size))
    , Data
    , Header#header.sync
    ],
  ok = file:write(Fd, ToWrite).

%% @doc Encode the given objects and append to the file as one data block.
-spec append_file(file:io_device(), header(), schema_store(),
                  type_or_name(), [avro:in()]) -> ok.
append_file(Fd, Header, SchemaStore, Schema, Objects) ->
  EncodedObjects =
    [ avro:encode(SchemaStore, Schema, O, avro_binary) || O <- Objects ],
  append_file(Fd, Header, EncodedObjects).

%% @doc Make ocf header.
-spec make_header(avro_type()) -> header().
make_header(Type) ->
  make_header(Type, _ExtraMeta = []).

%% @doc Make ocf header, and append the given extra metadata fields.
-spec make_header(avro_type(), extra_meta()) -> header().
make_header(Type, ExtraMeta0) ->
  ExtraMeta = validate_extra_meta(ExtraMeta0),
  Meta = case lists:keyfind(<<"avro.codec">>, 1, ExtraMeta) of
           false -> 
             [{<<"avro.codec">>, <<"null">>} | ExtraMeta];
           _ ->
             ExtraMeta
         end,
  TypeJson = avro_json_encoder:encode_type(Type),
  #header{ magic = <<"Obj", 1>>
         , meta  = [{<<"avro.schema">>, iolist_to_binary(TypeJson)} | Meta]
         , sync  = generate_sync_bytes()
         }.

%%%_* Internal functions =======================================================

%% @private Raise an exception if extra meta has a bad format.
%% Otherwise return the formatted metadata entries
%% @end
-spec validate_extra_meta(extra_meta()) -> extra_meta() | no_return().
validate_extra_meta([]) -> [];
validate_extra_meta([{K0, V} | Rest]) ->
  K = iolist_to_binary(K0),
  is_reserved_meta_key(K) andalso erlang:error({reserved_meta_key, K0}),
  is_invalid_codec_meta(K, V) andalso erlang:error({bad_codec, V}),
  is_binary(V) orelse erlang:error({bad_meta_value, V}),
  [{K, V} | validate_extra_meta(Rest)].

%% @private Meta keys which start with 'avro.' are reserved.
-spec is_reserved_meta_key(binary()) -> boolean().
is_reserved_meta_key(<<"avro.codec">>) -> false;
is_reserved_meta_key(<<"avro.", _/binary>>) -> true;
is_reserved_meta_key(_)                     -> false.

%% @private If avro.codec meta is provided, it must be one of supported values.
-spec is_invalid_codec_meta(binary(), binary()) -> boolean().
is_invalid_codec_meta(<<"avro.codec">>, <<"null">>) -> false;
is_invalid_codec_meta(<<"avro.codec">>, <<"deflate">>) -> false;
is_invalid_codec_meta(<<"avro.codec">>, _) -> true;
is_invalid_codec_meta(_, _) -> false.

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
-spec decode_blocks(schema_store(), avro_type(), avro_codec(),
                    binary(), binary(), [avro:out()]) -> [avro:out()].
decode_blocks(_Store, _Type, _Codec, _Sync, <<>>, Acc) ->
  lists:reverse(Acc);
decode_blocks(Store, Type, Codec, Sync, Bin0, Acc) ->
  LongType = avro_primitive:long_type(),
  {Count, Bin1} = decode_stream(Store, LongType, Bin0),
  {Size, Bin} = decode_stream(Store, LongType, Bin1),
  <<Block:Size/binary, Sync:16/binary, Tail/binary>> = Bin,
  NewAcc = decode_block(Store, Type, Codec, Block, Count, Acc),
  decode_blocks(Store, Type, Codec, Sync, Tail, NewAcc).

%% @private
-spec decode_block(schema_store(), avro_type(), avro_codec(),
                   binary(), integer(), [avro:out()]) -> [avro:out()].
decode_block(_Store, _Type, _Codec, <<>>, 0, Acc) -> Acc;
decode_block(Store, Type, deflate, Bin, Count, Acc) ->
  Decompressed = zlib:unzip(Bin),
  decode_block(Store, Type, null, Decompressed, Count, Acc);
decode_block(Store, Type, null, Bin, Count, Acc) ->
  {Obj, Tail} = decode_stream(Store, Type, Bin),
  decode_block(Store, Type, null, Tail, Count - 1, [Obj | Acc]).

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

%% @private Get codec from meta fields
-spec get_codec([{binary(), binary()}]) -> avro_codec().
get_codec(Meta) ->
  case lists:keyfind(<<"avro.codec">>, 1, Meta) of
    {_, <<"null">>} ->
      null;
    {_, <<"deflate">>} ->
      deflate
  end.

%% @private Encode block according to selected codec
-spec encode_block([{binary(), binary()}], iolist()) -> binary().
encode_block(Meta, Data) ->
  case get_codec(Meta) of
    null ->
      iolist_to_binary(Data);
    deflate ->
      zlib:zip(Data)
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
