%% coding: latin-1
%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2016-2018 Klarna AB
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
        , decode_binary/2
        , decode_file/1
        , decode_file/2
        , make_header/1
        , make_header/2
        , make_ocf/2
        , write_header/2
        , write_file/4
        , write_file/5
        ]).

-export_type([ header/0
             , meta/0
             ]).

-include("avro_internal.hrl").

-type filename() :: file:filename_all().
-type meta() :: [{string() | binary(), binary()}].
-type lkup() :: schema_store() | lkup_fun().

-record(header, { magic
                , meta
                , sync
                }).

-opaque header() :: #header{}.

%% Tested in OTP-21, dialyzer had trouble understanding the second arg
%% for the call to write_header/2.
-dialyzer({nowarn_function, [write_file/4, write_file/5]}).

%%%_* APIs =====================================================================

%% @doc decode_file/2 equivalent with default decoder options.
-spec decode_file(filename()) -> {header(), avro_type(), [avro:out()]}.
decode_file(Filename) ->
  decode_file(Filename, avro:make_decoder_options([])).

%% @doc Decode ocf into unwrapped values.
-spec decode_file(filename(), decoder_options()) ->
        {header(), avro_type(), [avro:out()]}.
decode_file(Filename, Options) ->
  {ok, Bin} = file:read_file(Filename),
  decode_binary(Bin, Options).

%% @doc decode_binary/2 equivalent with default decoder options.
-spec decode_binary(binary()) -> {header(), avro_type(), [avro:out()]}.
decode_binary(Bin) ->
  decode_binary(Bin, avro:make_decoder_options([])).

%% @doc Decode ocf binary into unwrapped values.
-spec decode_binary(binary(), decoder_options()) ->
        {header(), avro_type(), [avro:out()]}.
decode_binary(Bin, Options) ->
  {[ {<<"magic">>, Magic}
   , {<<"meta">>, Meta}
   , {<<"sync">>, Sync}
   ], Tail} = decode_stream(ocf_schema(), Bin),
  {_, SchemaBytes} = lists:keyfind(<<"avro.schema">>, 1, Meta),
  Codec = get_codec(Meta),
  %% Ignore bad defaults because ocf schema should never need defaults
  Schema = avro:decode_schema(SchemaBytes, [ignore_bad_default_values]),
  Lkup = avro:make_lkup_fun("_erlavro_ocf_root", Schema),
  Header = #header{ magic = Magic
                  , meta  = Meta
                  , sync  = Sync
                  },
  {Header, Schema, decode_blocks(Lkup, Schema, Codec, Sync, Tail, [], Options)}.

%% @doc Write objects in a single block to the given file name.
-spec write_file(filename(), lkup(), type_or_name(), [avro:in()]) -> ok.
write_file(Filename, Lkup, Schema, Objects) ->
  write_file(Filename, Lkup, Schema, Objects, []).

%% @doc Write objects in a single block to the given file name with custom
%% metadata. @see make_header/2 for details about use of meta data.
%% @end
-spec write_file(filename(), lkup(), type_or_name(), [avro:in()], meta()) -> ok.
write_file(Filename, Lkup, Schema, Objects, Meta) ->
  Header = make_header(Schema, Meta),
  {ok, Fd} = file:open(Filename, [write]),
  try
    ok = write_header(Fd, Header),
    ok = append_file(Fd, Header, Lkup, Schema, Objects)
  after
    file:close(Fd)
  end.

%% @doc Writer header bytes to a ocf file.
-spec write_header(file:io_device(), header()) -> ok.
write_header(Fd, Header) ->
  HeaderBytes = encode_header(Header),
  ok = file:write(Fd, HeaderBytes).

%% @doc Append encoded objects to the file as one data block.
-spec append_file(file:io_device(), header(), [binary()]) -> ok.
append_file(Fd, Header, Objects) ->
  IoData = make_block(Header, Objects),
  ok = file:write(Fd, IoData).

%% @doc Encode the given objects and append to the file as one data block.
-spec append_file(file:io_device(), header(), lkup(),
                  type_or_name(), [avro:in()]) -> ok.
append_file(Fd, Header, Lkup, Schema, Objects) ->
  EncodedObjects =
    [ avro:encode(Lkup, Schema, O, avro_binary) || O <- Objects ],
  append_file(Fd, Header, EncodedObjects).

%% @doc Make ocf header.
-spec make_header(avro_type()) -> header().
make_header(Type) ->
  make_header(Type, _ExtraMeta = []).

%% @doc Make ocf header, and append the given metadata fields.
%% You can use `&gt;&gt;"avro.codec"&lt;&lt;' metadata field to choose what data
%% block coding should be used. Supported values are `&gt;&gt;"null"&lt;&lt;'
%% (default), `&gt;&gt;"deflate"&lt;&lt;' (compressed) and 
%% `&gt;&gt;"snappy"&lt;&lt;' (compressed). Other values in `avro' namespace
%% are reserved for internal use and can't be set. Other than that you are 
%% free to provide any custom metadata.
%% @end
-spec make_header(avro_type(), meta()) -> header().
make_header(Type, Meta0) ->
  ValidatedMeta = validate_meta(Meta0),
  Meta = case lists:keyfind(<<"avro.codec">>, 1, ValidatedMeta) of
           false ->
             [{<<"avro.codec">>, <<"null">>} | ValidatedMeta];
           _ ->
             ValidatedMeta
         end,
  TypeJson = avro_json_encoder:encode_type(Type),
  #header{ magic = <<"Obj", 1>>
         , meta  = [{<<"avro.schema">>, iolist_to_binary(TypeJson)} | Meta]
         , sync  = generate_sync_bytes()
         }.

-spec make_ocf(header(), [binary()]) -> iodata().
make_ocf(Header, Objects) ->
  HeaderBytes = encode_header(Header),
  DataBytes = make_block(Header, Objects),
  [HeaderBytes, DataBytes].

%%%_* Internal functions =======================================================

-spec encode_header(header()) -> iodata().
encode_header(Header) ->
  HeaderFields =
    [ {"magic", Header#header.magic}
    , {"meta", Header#header.meta}
    , {"sync", Header#header.sync}
    ],
  HeaderRecord = avro_record:new(ocf_schema(), HeaderFields),
  avro_binary_encoder:encode_value(HeaderRecord).

-spec make_block(header(), [binary()]) -> iodata().
make_block(Header, Objects) ->
  Count = length(Objects),
  Data = encode_block(Header#header.meta, Objects),
  Size = size(Data),
  [ avro_binary_encoder:encode_value(avro_primitive:long(Count))
  , avro_binary_encoder:encode_value(avro_primitive:long(Size))
  , Data
  , Header#header.sync
  ].

%% Raise an exception if meta has a bad format.
%% Otherwise return the formatted metadata entries
-spec validate_meta(meta()) -> meta() | no_return().
validate_meta([]) -> [];
validate_meta([{K0, V} | Rest]) ->
  K = iolist_to_binary(K0),
  is_reserved_meta_key(K) andalso erlang:error({reserved_meta_key, K0}),
  is_invalid_codec_meta(K, V) andalso erlang:error({bad_codec, V}),
  is_binary(V) orelse erlang:error({bad_meta_value, V}),
  [{K, V} | validate_meta(Rest)].

%% Meta keys which start with 'avro.' are reserved.
-spec is_reserved_meta_key(binary()) -> boolean().
is_reserved_meta_key(<<"avro.codec">>) -> false;
is_reserved_meta_key(<<"avro.", _/binary>>) -> true;
is_reserved_meta_key(_)                     -> false.

%% If avro.codec meta is provided, it must be one of supported values.
-spec is_invalid_codec_meta(binary(), binary()) -> boolean().
is_invalid_codec_meta(<<"avro.codec">>, <<"null">>) -> false;
is_invalid_codec_meta(<<"avro.codec">>, <<"deflate">>) -> false;
is_invalid_codec_meta(<<"avro.codec">>, <<"snappy">>) -> false;
is_invalid_codec_meta(<<"avro.codec">>, _) -> true;
is_invalid_codec_meta(_, _) -> false.

-spec generate_sync_bytes() -> binary().
generate_sync_bytes() -> crypto:strong_rand_bytes(16).

-spec decode_stream(avro_type(), binary()) -> {avro:out(), binary()}.
decode_stream(Type, Bin) when is_binary(Bin) ->
  Lkup = fun(_) -> erlang:error(unexpected) end,
  avro_binary_decoder:decode_stream(Bin, Type, Lkup).

-spec decode_stream(lkup(), avro_type(), binary()) ->
        {avro:out(), binary()} | no_return().
decode_stream(Lkup, Type, Bin) when is_binary(Bin) ->
  avro_binary_decoder:decode_stream(Bin, Type, Lkup).

-spec decode_stream(lkup(), avro_type(), binary(), decoder_options()) ->
        {avro:out(), binary()} | no_return().
decode_stream(Lkup, Type, Bin, Options) when is_binary(Bin) ->
  avro_binary_decoder:decode_stream(Bin, Type, Lkup, Options).

-spec decode_blocks(lkup(), avro_type(), avro_codec(),
                    binary(), binary(), [avro:out()], decoder_options()) ->
        [avro:out()].
decode_blocks(_Lkup, _Type, _Codec, _Sync, <<>>, Acc, _Options) ->
  lists:reverse(Acc);
decode_blocks(Lkup, Type, Codec, Sync, Bin0, Acc, Options) ->
  LongType = avro_primitive:long_type(),
  {Count, Bin1} = decode_stream(Lkup, LongType, Bin0),
  {Size, Bin} = decode_stream(Lkup, LongType, Bin1),
  <<Block:Size/binary, Sync:16/binary, Tail/binary>> = Bin,
  NewAcc = decode_block(Lkup, Type, Codec, Block, Count, Acc, Options),
  decode_blocks(Lkup, Type, Codec, Sync, Tail, NewAcc, Options).

-spec decode_block(lkup(), avro_type(), avro_codec(),
                   binary(), integer(), [avro:out()],
                   decoder_options()) -> [avro:out()].
decode_block(_Lkup, _Type, _Codec, <<>>, 0, Acc, _Options) -> Acc;
decode_block(Lkup, Type, deflate, Bin, Count, Acc, Options) ->
  Decompressed = zlib:unzip(Bin),
  decode_block(Lkup, Type, null, Decompressed, Count, Acc, Options);
decode_block(Lkup, Type, snappy, Bin, Count, Acc, Options) ->
  Size = byte_size(Bin),
  <<Compressed:(Size-4)/binary, Checksum:4/binary>> = Bin,
  {ok, Decompressed} = snappyer:decompress(Compressed),
  case is_valid_checksum(Decompressed, Checksum) of
    true -> Decompressed;
    false -> erlang:error({invalid_checksum, Decompressed})
  end,
  decode_block(Lkup, Type, null, Decompressed, Count, Acc, Options);
decode_block(Lkup, Type, null, Bin, Count, Acc, Options) ->
  {Obj, Tail} = decode_stream(Lkup, Type, Bin, Options),
  decode_block(Lkup, Type, null, Tail, Count - 1, [Obj | Acc], Options).

- spec is_valid_checksum(binary(), binary()) -> boolean().
is_valid_checksum(Decompressed, Checksum) ->
  binary:encode_unsigned(erlang:crc32(Decompressed)) =:= Checksum.

%% Hand coded schema.
%% {"type": "record", "name": "org.apache.avro.file.Header",
%% "fields" : [
%%   {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
%%   {"name": "meta", "type": {"type": "map", "values": "bytes"}},
%%   {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
%%  ]
%% }
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

%% Get codec from meta fields
-spec get_codec([{binary(), binary()}]) -> avro_codec().
get_codec(Meta) ->
  case lists:keyfind(<<"avro.codec">>, 1, Meta) of
    false ->
      null;
    {_, <<"null">>} ->
      null;
    {_, <<"deflate">>} ->
      deflate;
    {_, <<"snappy">>} ->
      snappy
  end.

%% Encode block according to selected codec
-spec encode_block([{binary(), binary()}], iolist()) -> binary().
encode_block(Meta, Data) ->
  case get_codec(Meta) of
    null ->
      iolist_to_binary(Data);
    deflate ->
      zlib:zip(Data);
    snappy ->
      Checksum = erlang:crc32(Data),
      {ok, Bin} = snappyer:compress(Data),
      iolist_to_binary([Bin, <<Checksum:32>>])
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
