%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2025 Klarna AB
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
%%% @doc Compatibility layer for JSON encoding/decoding.
%%% Uses Erlang's native json module on OTP 27+
%%% falls back to jsone on older versions.
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro_json_compat).

%% Suppress Dialyzer warnings about json module functions on OTP < 27
-if(?OTP_RELEASE < 27).
-dialyzer({nowarn_function, [encode/2, decode/2, encoder/2]}).
-endif.

-export([set_provider/1, get_provider/0]).
-export([encode/1, encode/2, decode/1, decode/2]).
-export([inline/1]).
-export_type([json_value/0]).

%% Type definitions compatible with both jsone and native json
-type json_obj() :: #{binary() => json_value()} |
                    [{binary(), json_value()}] |
                    {[{binary(), json_value()}]}.

%% NB: atom() only for encoder inputs.
%% No atom created when decode.
-type string_value() :: atom() | binary().

-type json_value() :: null
                    | boolean()
                    | integer()
                    | float()
                    | string_value()
                    | [json_value()]
                    | json_obj().

-type encode_option() :: native_utf8.
-type decode_option() :: {object_format, tuple | map | proplist}.

%% Suppress warnings about json module functions on OTP < 27
-if(?OTP_RELEASE < 27).
-compile(nowarn_undefined_function).
-endif.

-define(PROVIDER_PTP_KEY, erlavro_json_provider).

%%%_* APIs =====================================================================

%% @doc Set `json' or `jsone' as the JSON encode/decode provider.
-spec set_provider(json | jsone) -> ok.
set_provider(Module) ->
  persistent_term:put(?PROVIDER_PTP_KEY, Module).

%% @doc Get current JSON encode/decode provider.
-spec get_provider() -> json | jsone.
get_provider() ->
  case persistent_term:get(?PROVIDER_PTP_KEY, undefined) of
    undefined ->
      Module = get_available_provider([json, jsone]),
      persistent_term:put(?PROVIDER_PTP_KEY, Module),
      Module;
    Module ->
      Module
  end.

%% @doc Make a inline (already encoded JSON) JSON value.
%% This is a compatible format for jsone.
inline(JSON) -> {{json, JSON}}.

%% @doc Encode Erlang term to JSON.
%% Equivalent to jsone:encode/1
%% Returns iodata() (binary() or iolist())
-spec encode(json_value()) -> iodata().
encode(Value) -> encode(Value, []).

%% @doc Encode Erlang term to JSON with options.
-spec encode(json_value(), [encode_option()]) -> iodata().
encode(Value, Options) ->
  case get_provider() of
    json ->
      iolist_to_binary(json:encode(Value, fun encoder/2));
    Module ->
      apply(Module, encode, [Value, Options])
  end.

%% Custom encoder callback for json:encode/2
%% Handles conversion from jsone format (lists of tuples) to
%% native json format inline
encoder(Value, Encode) when is_list(Value) ->
  %% Check if it's a list of key-value pairs (object) or a plain list (array)
  case is_key_value_list(Value) of
    true ->
      %% It's an object: [{Key, Value}] -> encode as key-value list
      %% Convert atom keys to binary keys for native json
      ConvertedList = [{convert_key(K), V} || {K, V} <- Value],
      json:encode_key_value_list(ConvertedList, Encode);
    false ->
      %% It's an array: [Value] -> encode as array
      json:encode_value(Value, Encode)
  end;
encoder({{json, JSON}}, _Encode) ->
  iolist_to_binary(JSON);
encoder({[]}, _Encode) ->
  <<"{}">>;
encoder({[{_, _} | _] = KvList}, Encode) ->
  encoder(KvList, Encode);
encoder(Value, Encode) ->
  %% Primitive value or map - use default encoding
  json:encode_value(Value, Encode).

%% @doc Decode JSON binary to Erlang term.
%% Default object format is tuple (compatible with jsone default).
-spec decode(binary()) -> json_value().
decode(JSON) ->
  decode(JSON, [{object_format, tuple}]).

%% @doc Decode JSON binary to Erlang term with options.
%% Options:
%%   {object_format, tuple} - Return objects as {[{Key, Value}]} tuples
%%   (default, jsone format)
%%   {object_format, map}   - Return objects as #{Key => Value} maps
%%   {object_format, proplist} - Return objects as [{Key, Value}] proplist
-spec decode(binary(), [decode_option()]) -> json_value().
decode(JSON, Options) ->
  case get_provider() of
    json ->
      JsonBinary = iolist_to_binary(JSON),
      ObjectFormat = proplists:get_value(object_format, Options, tuple),
      Decoded = json:decode(JsonBinary),
      convert_object_format(Decoded, ObjectFormat);
    Module ->
      apply(Module, decode, [JSON, Options])
  end.

%%%_* Internal functions =======================================================

%% @private Check if a list represents a key-value object (proplist)
%% Accepts both binary keys and atom keys
-spec is_key_value_list([term()]) -> boolean().
is_key_value_list([{K, _V} | _]) when is_binary(K) orelse is_atom(K) ->
  true;
is_key_value_list(_) ->
  false.

%% @private Convert key to binary format (native json requires binary keys)
-spec convert_key(atom() | binary()) -> binary().
convert_key(Key) when is_atom(Key) ->
  atom_to_binary(Key, utf8);
convert_key(Key) when is_binary(Key) ->
  Key.

%% @private Convert decoded JSON value's object format
%% jsone with {object_format, tuple} returns objects as {Fields}
%% where Fields is [{Key, Value}]
%% native json returns objects as #{Key => Value} maps
-spec convert_object_format(json_value(), tuple | map | proplist) ->
    json_value().
convert_object_format(Value, Format) when is_map(Value) ->
  case Format of
    tuple ->
      %% Convert map to jsone format: {[{Key, Value}]}
      %% Recursively convert nested maps in values
      %% Note: maps don't preserve JSON key order, so the resulting tuple order
      %% may differ from the original JSON order
      Fields = [{Key, convert_object_format(V, Format)}
                || {Key, V} <- maps:to_list(Value)],
      {Fields};
    map ->
      Value;
    proplist ->
      %% Convert to proplist, recursively convert nested maps
      [{Key, convert_object_format(V, Format)}
       || {Key, V} <- maps:to_list(Value)]
  end;
convert_object_format({Fields}, Format) when is_list(Fields) ->
  %% Already in jsone tuple format, but need to recurse into
  %% nested objects/arrays
  {[{Key, convert_object_format(Value, Format)}
    || {Key, Value} <- Fields]};
convert_object_format(List, Format) when is_list(List) ->
  %% Check if it's a list of key-value pairs (proplist) or a JSON array
  case is_key_value_list(List) of
    true ->
      %% It's a proplist, recurse into values
      [{Key, convert_object_format(Value, Format)} || {Key, Value} <- List];
    false ->
      %% It's a JSON array, recurse into elements
      [convert_object_format(Item, Format) || Item <- List]
  end;
convert_object_format(Value, _Format) ->
  %% Primitive value (null, boolean, number, binary)
  Value.

get_available_provider([]) ->
  error(erlavro_no_json_provider);
get_available_provider([Module | Rest]) ->
  try
    apply(Module, module_info, [module])
  catch
    _:_ ->
      get_available_provider(Rest)
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
