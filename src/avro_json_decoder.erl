%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2013-2017 Klarna AB
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
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc Avro Json decoder
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro_json_decoder).

%% API
-export([ decode_schema/1
        , decode_schema/2
        , decode_value/3
        , decode_value/4
        , decode_value/5
        ]).

-include("avro_internal.hrl").

-ifdef(TEST).
-export([ parse_value/3
        , parse_schema/2
        , parse/5
        ]).
-endif.

-type option_name() :: is_wrapped.

-type options() :: [{option_name(), term()}].
-type hook() :: decoder_hook_fun().
-type json_value() :: jsone:json_value().

-define(JSON_OBJ(FIELDS), {FIELDS}).

%%%_* APIs =====================================================================

%% @doc Decode JSON schema.
-spec decode_schema(iodata()) -> avro_type().
decode_schema(Json) ->
  decode_schema(Json, fun(_) -> erlang:error(no_function) end).

%% @doc Decode Avro schema specified as Json string.
%% Schema store or lookup function is needed to parse default values.
%% @end
-spec decode_schema(iodata(), schema_store() | lkup_fun()) -> avro_type().
decode_schema(JsonSchema, Store) when not is_function(Store) ->
  Lkup = ?AVRO_SCHEMA_LOOKUP_FUN(Store),
  decode_schema(JsonSchema, Lkup);
decode_schema(JsonSchema, Lkup) ->
  Decoded = decode_json(JsonSchema),
  parse_schema(Decoded, Lkup).

%% @doc Decode JSON encoded payload to wrapped (boxed) #avro_value{} record,
%% or unwrapped (unboxed) Erlang term.
%% Options:
%%  is_wrapped (optional, default = true)
%%     By default, this function returns #avro_value{} i.e. all values are
%%     wrapped together with the type info.
%%     If {is_wrapped, false} is given in Options, it returns unwrapped values
%%     which is equivalent as calling avro_xxx:to_term/1 recursively
%%     Unwrapped values for each avro type as in erlang spec:
%%       null:   'null'.
%%       int:    integer().
%%       long:   integer().
%%       float:  float().
%%       double: float().
%%       bytes:  binary().
%%       string: binary().
%%       enum:   binary().
%%       fixed:  binary().
%%       union:  avro:out().
%%       array:  [avro:out()].
%%       map:    [{Key :: string(), Value :: avro:out()}].
%%       record: [{FieldName() :: binary(), FieldValue :: avro:out()}]}
%% @end
-spec decode_value(binary(), type_or_name(),
                   schema_store() | lkup_fun(),
                   options(), hook()) -> avro_value() | avro:out().
decode_value(JsonValue, Schema, Store, Options, Hook)
 when not is_function(Store) ->
  Lkup = ?AVRO_SCHEMA_LOOKUP_FUN(Store),
  decode_value(JsonValue, Schema, Lkup, Options, Hook);
decode_value(JsonValue, Schema, Lkup, Options, Hook) ->
  DecodedJson = decode_json(JsonValue),
  IsWrapped = case lists:keyfind(is_wrapped, 1, Options) of
                {is_wrapped, V} -> V;
                false           -> true %% parse to wrapped value by default
              end,
  parse(DecodedJson, Schema, Lkup, IsWrapped, Hook).

%% @doc Decode value with default options and default hook.
-spec decode_value(binary(), type_or_name(),
                   schema_store() | lkup_fun()) -> avro_value().
decode_value(JsonValue, Schema, StoreOrLkupFun) ->
  decode_value(JsonValue, Schema, StoreOrLkupFun, []).

%% @doc Decode value with default hook.
-spec decode_value(binary(), type_or_name(),
                   schema_store() | lkup_fun(),
                   options()) -> avro_value() | avro:out().
decode_value(JsonValue, Schema, StoreOrLkupFun, Options) ->
  decode_value(JsonValue, Schema, StoreOrLkupFun,
               Options, ?DEFAULT_DECODER_HOOK).

%%%_* Internal functions =======================================================

%% @private
-spec parse_schema(json_value(), lkup_fun()) -> avro_type() | no_return().
parse_schema(?JSON_OBJ(Attrs), Lkup) ->
  %% Json object: this is a complex type definition (except for unions)
  parse_type(Attrs, Lkup);
parse_schema(Array, Lkup) when is_list(Array) ->
  %% Json array: this is an union definition
  parse_union_type(Array, Lkup);
parse_schema(Name, _Lkup) when ?IS_NAME(Name) ->
  %% Json string: this is a type name.
  %% Return #avro_primitive_type{} for primitive types
  %% otherwise make full name.
  try
    primitive_type(Name, [])
  catch error : {unknown_type, _} ->
    ok = avro_util:verify_dotted_name(Name),
    Name
  end.

%% @private Parse JSON object to avro type definition.
-spec parse_type([{binary(), json_value()}], lkup_fun()) ->
        avro_type() | no_return().
parse_type(Attrs, Lkup) ->
  case avro_util:get_opt(<<"type">>, Attrs) of
    ?AVRO_RECORD ->
      parse_record_type(Attrs, Lkup);
    ?AVRO_ENUM ->
      parse_enum_type(Attrs);
    ?AVRO_ARRAY ->
      parse_array_type(Attrs, Lkup);
    ?AVRO_MAP ->
      parse_map_type(Attrs, Lkup);
    ?AVRO_FIXED ->
      parse_fixed_type(Attrs);
    Name ->
      CustomProps = filter_custom_props(Attrs, []),
      primitive_type(Name, CustomProps)
  end.

%% @private
-spec primitive_type(name(), [custom_prop()]) ->
        primitive_type() | no_return().
primitive_type(Name, CustomProps) when ?IS_AVRO_PRIMITIVE_NAME(Name) ->
  avro_primitive:type(Name, CustomProps);
primitive_type(Name, _CustomProps) ->
  erlang:error({unknown_type, Name}).

%% @private
-spec parse_record_type([{binary(), json_value()}],
                        lkup_fun()) -> record_type() | no_return().
parse_record_type(Attrs, Lkup) ->
  Name    = avro_util:get_opt(<<"name">>,      Attrs),
  Ns      = avro_util:get_opt(<<"namespace">>, Attrs, <<"">>),
  Doc     = avro_util:get_opt(<<"doc">>,       Attrs, <<"">>),
  Aliases = avro_util:get_opt(<<"aliases">>,   Attrs, []),
  Fields0 = avro_util:get_opt(<<"fields">>,    Attrs),
  Fields  = parse_record_fields(Fields0, Lkup),
  Custom  = filter_custom_props(Attrs, [<<"fields">>]),
  avro_record:type(Name, Fields,
                   [ {namespace,    Ns}
                   , {doc,          Doc}
                   , {aliases,      Aliases}
                   | Custom
                   ]).

%% @private
-spec parse_record_fields([{name(), json_value()}], lkup_fun()) ->
        [record_field()] | no_return().
parse_record_fields(Fields, Lkup) ->
  lists:map(fun(?JSON_OBJ(FieldAttrs)) ->
                parse_record_field(FieldAttrs, Lkup)
            end,
            Fields).

%% @private
-spec parse_record_field([{binary(), json_value()}], lkup_fun()) ->
        record_field() | no_return().
parse_record_field(Attrs, Lkup) ->
  Name      = avro_util:get_opt(<<"name">>,    Attrs),
  Doc       = avro_util:get_opt(<<"doc">>,     Attrs, <<"">>),
  Type      = avro_util:get_opt(<<"type">>,    Attrs),
  Default   = avro_util:get_opt(<<"default">>, Attrs, undefined),
  Order     = avro_util:get_opt(<<"order">>,   Attrs, <<"ascending">>),
  Aliases   = avro_util:get_opt(<<"aliases">>, Attrs, []),
  FieldType = parse_schema(Type, Lkup),
  #avro_record_field
  { name    = Name
  , doc     = Doc
  , type    = FieldType
  , default = parse_default_value(Default, FieldType, Lkup)
  , order   = parse_order(Order)
  , aliases = parse_aliases(Aliases)
  }.

%% @private
-spec parse_default_value(null | undefined | json_value(), avro_type(), lkup_fun()) ->
        null | undefined | avro_value().
parse_default_value(undefined, _FieldType, _Lkup) ->
  undefined;
parse_default_value(null, _FieldType, _Lkup) ->
  null;
parse_default_value(Value, FieldType, Lkup) when ?IS_UNION_TYPE(FieldType) ->
  %% Strange agreement about unions: default value for an union field
  %% corresponds to the first type in this union.
  %% Why not to use normal union values format?
  [FirstType|_] = avro_union:get_types(FieldType),
  avro_union:new(FieldType, parse_value(Value, FirstType, Lkup));
parse_default_value(Value, FieldType, Lkup) ->
  parse_value(Value, FieldType, Lkup).

%% @private
-spec parse_order(binary()) -> ascending | descending | ignore.
parse_order(<<"ascending">>)  -> ascending;
parse_order(<<"descending">>) -> descending;
parse_order(<<"ignore">>)     -> ignore.

%% @private
-spec parse_enum_type([{binary(), json_value()}]) -> enum_type().
parse_enum_type(Attrs) ->
  NameBin = avro_util:get_opt(<<"name">>,      Attrs),
  NsBin   = avro_util:get_opt(<<"namespace">>, Attrs, <<"">>),
  Doc     = avro_util:get_opt(<<"doc">>,       Attrs, <<"">>),
  Aliases = avro_util:get_opt(<<"aliases">>,   Attrs, []),
  Symbols = avro_util:get_opt(<<"symbols">>,   Attrs),
  Custom  = filter_custom_props(Attrs, [<<"symbols">>]),
  avro_enum:type(NameBin,
                 parse_enum_symbols(Symbols),
                 [ {namespace,    NsBin}
                 , {doc,          Doc}
                 , {aliases,      parse_aliases(Aliases)}
                 | Custom
                 ]).

%% @private
-spec parse_enum_symbols([binary()]) -> [enum_symbol()].
parse_enum_symbols([_|_] = SymbolsArray) ->
  SymbolsArray.

%% @private
-spec parse_array_type([{binary(), json_value()}], lkup_fun()) -> array_type().
parse_array_type(Attrs, Lkup) ->
  Items  = avro_util:get_opt(<<"items">>, Attrs),
  Custom = filter_custom_props(Attrs, [<<"items">>]),
  avro_array:type(parse_schema(Items, Lkup), Custom).

%% @private
-spec parse_map_type([{binary(), json_value()}], lkup_fun()) -> map_type().
parse_map_type(Attrs, Lkup) ->
  Values = avro_util:get_opt(<<"values">>, Attrs),
  Custom = filter_custom_props(Attrs, [<<"values">>]),
  avro_map:type(parse_schema(Values, Lkup), Custom).

%% @private
-spec parse_fixed_type([{binary(), json_value()}]) -> fixed_type().
parse_fixed_type(Attrs) ->
  NameBin = avro_util:get_opt(<<"name">>,      Attrs),
  NsBin   = avro_util:get_opt(<<"namespace">>, Attrs, <<"">>),
  Aliases = avro_util:get_opt(<<"aliases">>,   Attrs, []),
  Doc     = avro_util:get_opt(<<"doc">>,       Attrs, ?NO_DOC),
  Size    = avro_util:get_opt(<<"size">>,      Attrs),
  Custom  = filter_custom_props(Attrs, [<<"size">>]),
  avro_fixed:type(NameBin,
                  parse_fixed_size(Size),
                  [ {namespace,    NsBin}
                  , {aliases,      parse_aliases(Aliases)}
                  , {doc,          Doc}
                  | Custom
                  ]).

%% @private
-spec parse_fixed_size(integer()) -> pos_integer().
parse_fixed_size(N) when is_integer(N) andalso N > 0 -> N.

%% @private
-spec parse_union_type(json_value(), lkup_fun()) -> union_type().
parse_union_type(Attrs, Lkup) ->
  Types = lists:map(
            fun(Schema) ->
                parse_schema(Schema, Lkup)
            end,
            Attrs),
  avro_union:type(Types).

%% @private
-spec parse_aliases([name()]) -> [name()] | no_return().
parse_aliases(AliasesArray) when is_list(AliasesArray) ->
  lists:map(
    fun(AliasBin) when is_binary(AliasBin) ->
        ok = avro_util:verify_dotted_name(AliasBin),
        AliasBin
    end,
    AliasesArray).

%% @private
-spec parse_value(json_value(), avro_type(), lkup_fun()) ->
        avro_value() | no_return().
parse_value(Value, Type, Lkup) ->
  parse(Value, Type, Lkup, _IsWrapped = true, ?DEFAULT_DECODER_HOOK).

%% @private
-spec parse(json_value(), type_or_name(), lkup_fun(), boolean(), hook()) ->
        avro_value() | avro:out() | no_return().
parse(Value, TypeName, Lkup, IsWrapped, Hook) when ?IS_NAME_RAW(TypeName) ->
  %% Type is defined by its name
  Type = Lkup(?NAME(TypeName)),
  parse(Value, Type, Lkup, IsWrapped, Hook);
parse(Value, Type, _Lkup, IsWrapped, Hook) when ?IS_PRIMITIVE_TYPE(Type) ->
 Hook(Type, <<>>, Value,
      fun(JsonV) ->
        WrappedValue = parse_prim(JsonV, Type),
        case IsWrapped of
          true  -> WrappedValue;
          false -> avro_primitive:get_value(WrappedValue)
        end
      end);
parse(V, Type, _Lkup, IsWrapped, Hook) when ?IS_ENUM_TYPE(Type),
                                            is_binary(V) ->
  Hook(Type, <<>>, V,
       fun(JsonV) ->
         case IsWrapped of
           true  -> avro_enum:new(Type, JsonV);
           false -> JsonV
         end
       end);
parse(V, Type, _Lkup, IsWrapped, Hook) when ?IS_FIXED_TYPE(Type) ->
  Hook(Type, <<>>, V,
       fun(JsonV) ->
         case IsWrapped of
           true  -> avro_fixed:new(Type, parse_bytes(JsonV));
           false -> parse_bytes(JsonV)
         end
       end);
parse(V, Type, Lkup, IsWrapped, Hook) when ?IS_RECORD_TYPE(Type) ->
  parse_record(V, Type, Lkup, IsWrapped, Hook);
parse(V, Type, Lkup, IsWrapped, Hook) when ?IS_ARRAY_TYPE(Type) ->
  parse_array(V, Type, Lkup, IsWrapped, Hook);
parse(V, Type, Lkup, IsWrapped, Hook) when ?IS_MAP_TYPE(Type) ->
  parse_map(V, Type, Lkup, IsWrapped, Hook);
parse(V, Type, Lkup, IsWrapped, Hook) when ?IS_UNION_TYPE(Type) ->
  parse_union(V, Type, Lkup, IsWrapped, Hook).

%% @private Parse primitive values, return wrapped (boxed) value.
-spec parse_prim(json_value(), avro_type()) -> avro_value().
parse_prim(null, Type) when ?IS_NULL_TYPE(Type) ->
  avro_primitive:null();
parse_prim(V, Type) when ?IS_BOOLEAN_TYPE(Type) andalso is_boolean(V) ->
  avro_primitive:boolean(V);
parse_prim(V, Type) when ?IS_INT_TYPE(Type) andalso
                         is_integer(V)      andalso
                         V >= ?INT4_MIN     andalso
                         V =< ?INT4_MAX ->
  avro_primitive:int(V);
parse_prim(V, Type) when ?IS_LONG_TYPE(Type) andalso
                          is_integer(V)      andalso
                          V >= ?INT8_MIN     andalso
                          V =< ?INT8_MAX ->
  avro_primitive:long(V);
parse_prim(V, Type) when ?IS_FLOAT_TYPE(Type) andalso
                          (is_float(V) orelse is_integer(V)) ->
  avro_primitive:float(V);
parse_prim(V, Type) when ?IS_DOUBLE_TYPE(Type) andalso
                         (is_float(V) orelse is_integer(V)) ->
  avro_primitive:double(V);
parse_prim(V, Type) when ?IS_BYTES_TYPE(Type) andalso
                         is_binary(V) ->
  Bin = parse_bytes(V),
  avro_primitive:bytes(Bin);
parse_prim(V, Type) when ?IS_STRING_TYPE(Type) andalso
                         is_binary(V) ->
  avro_primitive:string(V).

%% @private
-spec parse_bytes(binary()) -> binary().
parse_bytes(BytesStr) ->
  list_to_binary(parse_bytes(BytesStr, [])).

%% @private
-spec parse_bytes(binary(), [byte()]) -> [byte()].
parse_bytes(<<>>, Acc) ->
  lists:reverse(Acc);
parse_bytes(<<"\\u00", B1, B0, Rest/binary>>, Acc) ->
  Byte = erlang:list_to_integer([B1, B0], 16),
  parse_bytes(Rest, [Byte | Acc]).

%% @private
-spec parse_record(json_value(), record_type(),
                   lkup_fun(), boolean(), hook()) ->
        avro_value() | avro:out().
parse_record(?JSON_OBJ(Attrs), Type, Lkup, IsWrapped, Hook) ->
  Hook(Type, none, Attrs,
       fun(JsonValues) ->
         Fields = convert_attrs_to_record_fields(JsonValues, Type, Lkup,
                                                 IsWrapped, Hook),
         case IsWrapped of
           true  -> avro_record:new(Type, Fields);
           false -> Fields
         end
       end).

%% @private
-spec convert_attrs_to_record_fields(json_value(), record_type(), lkup_fun(),
                                     boolean(), hook()) ->
        [{name(), avro_value() | avro:out()}] | no_return().
convert_attrs_to_record_fields(Attrs, Type, Lkup, IsWrapped, Hook) ->
  lists:map(
    fun({FieldName, Value}) ->
        FieldType = avro_record:get_field_type(FieldName, Type),
        FieldValue =
          Hook(Type, FieldName, Value,
               fun(JsonV) ->
                 parse(JsonV, FieldType, Lkup, IsWrapped, Hook)
               end),
        {FieldName, FieldValue}
    end,
    Attrs).

%% @private
-spec parse_array([json_value()], array_type(),
                  lkup_fun(), boolean(), hook()) ->
        [avro_value() | avro:out()].
parse_array(V, Type, Lkup, IsWrapped, Hook) when is_list(V) ->
  ItemsType = avro_array:get_items_type(Type),
  {_Index, ParsedArray} =
    lists:foldl(
      fun(Item, {Index, Acc}) ->
        ParsedItem = Hook(Type, Index, Item,
                          fun(JsonV) ->
                            parse(JsonV, ItemsType, Lkup, IsWrapped, Hook)
                          end),
        {Index+1, [ParsedItem | Acc]}
      end,
      {_ZeroBasedIndexInitialValue = 0, []}, V),
  Items = lists:reverse(ParsedArray),
  case IsWrapped of
    true ->
      %% Here we can use direct version of new because we casted all items
      %% to the array type before
      avro_array:new_direct(Type, Items);
    false ->
      Items
  end.

%% @private
-spec parse_map(json_value(), map_type(), lkup_fun(), boolean(), hook()) ->
        avro_value() | avro:out().
parse_map(?JSON_OBJ(Attrs), Type, Lkup, IsWrapped, Hook) ->
  ItemsType = avro_map:get_items_type(Type),
  L = lists:map(
        fun({KeyBin, Value}) ->
            V = Hook(Type, KeyBin, Value,
                     fun(JsonV) ->
                       parse(JsonV, ItemsType, Lkup, IsWrapped, Hook)
                     end),
            {KeyBin, V}
        end, Attrs),
  case IsWrapped of
    true  -> avro_map:new(Type, L);
    false -> L
  end.

%% @private
-spec parse_union(json_value(), union_type(),
                  lkup_fun(), boolean(), hook()) ->
        avro_value() | avro:out() | no_return().
parse_union(null = Value, Type, Lkup, IsWrapped, Hook) ->
  %% Union values specified as null
  parse_union_ex(?AVRO_NULL, Value, Type, Lkup, IsWrapped, Hook);
parse_union(?JSON_OBJ([{ValueTypeName, Value}]),
            Type, Lkup, IsWrapped, Hook) ->
  %% Union value specified as {"type": <value>}
  parse_union_ex(ValueTypeName, Value, Type, Lkup, IsWrapped, Hook).

%% @private
-spec parse_union_ex(name(), json_value(), union_type(),
                     lkup_fun(), boolean(), hook()) ->
        avro_value() | avro:out() | no_return().
parse_union_ex(ValueTypeName, Value, UnionType, Lkup, IsWrapped, Hook) ->
  Hook(UnionType, ValueTypeName, Value,
       fun(In) ->
          do_parse_union_ex(ValueTypeName, In, UnionType,
                            Lkup, IsWrapped, Hook)
       end).

%% @private
-spec do_parse_union_ex(name(), json_value(), union_type(),
                        lkup_fun(), boolean(), hook()) ->
        avro_value() | avro:out() | no_return().
do_parse_union_ex(ValueTypeName, Value, UnionType,
                  Lkup, IsWrapped, Hook) ->
  case avro_union:lookup_type(ValueTypeName, UnionType) of
    {ok, ValueType} ->
      ParsedValue = parse(Value, ValueType, Lkup, IsWrapped, Hook),
      case IsWrapped of
        true ->
          %% Here we can create the value directly because we know that
          %% the type of value belongs to the union type and we can skip
          %% additional looping over union types in avro_union:cast
          avro_union:new_direct(UnionType, ParsedValue);
        false ->
          ParsedValue
      end;
    false ->
      erlang:error({unknown_union_member, ValueTypeName})
  end.

%% @private Always use tuple as object foramt.
-spec decode_json(binary()) -> json_value().
decode_json(JSON) -> jsone:decode(JSON, [{object_format, tuple}]).

%% @private Filter out non-custom properties.
-spec filter_custom_props([{binary(), json_value()}], [name()]) ->
        [custom_prop()].
filter_custom_props(Attrs, Keys0) ->
  Keys = [<<"type">>, <<"name">>, <<"namespace">>,
          <<"doc">>, <<"aliases">> | Keys0],
  avro_util:delete_opts(Attrs, Keys).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
