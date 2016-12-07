%%%-------------------------------------------------------------------
%%% Copyright (c) 2013-2016 Klarna AB
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
%%%-------------------------------------------------------------------
-module(avro_json_decoder).

%% API
-export([ decode_schema/1
        , decode_schema/2
        , decode_value/3
        , decode_value/4
        , decode_value/5
        ]).

-include("erlavro.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-type option_name() :: json_decoder
                     | is_wrapped.

-type options() :: [{option_name(), term()}].
-type lkup_fun() :: fun((string()) -> avro_type()).
-type schema_store() :: avro_schema_store:store().
-type hook() :: decoder_hook_fun().

%%%===================================================================
%%% API
%%%===================================================================

-spec decode_schema(iodata()) -> avro_type().
decode_schema(Json) ->
  decode_schema(Json, fun(_) -> erlang:error(no_function) end).

%% Decode Avro schema specified as Json string.
%% ExtractTypeFun should be a function returning Avro type by its full name,
%% it is needed to parse default values.
-spec decode_schema(iodata(), schema_store() | lkup_fun()) -> avro_type().
decode_schema(JsonSchema, Store) when not is_function(Store) ->
  ExtractTypeFun = ?AVRO_SCHEMA_LOOKUP_FUN(Store),
  decode_schema(JsonSchema, ExtractTypeFun);
decode_schema(JsonSchema, ExtractTypeFun) ->
  parse_schema(mochijson3:decode(JsonSchema), "", ExtractTypeFun).

%% @doc Decode value specified as Json string according to Avro schema
%% in Schema. ExtractTypeFun should be provided to retrieve types
%% specified by their names inside Schema.
%%
%% Options:
%%   json_decoder (optional, default = jsonx)
%%     jsonx | mochijson3
%%     jsonx is about 12 times faster than mochijson3 and almost compatible
%%     with it. The one discovered incompatibility is that jsonx performs more
%%     strict checks on incoming json strings and can fail on some cases
%%     (for example, on non-ascii symbols) while mochijson3 can parse such
%%     strings without problems.
%%     Current strategy is to use jsonx as the main parser and fall back to
%%     mochijson3 in case of parsing issues.
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
%%       string: string().
%%       enum:   string().
%%       fixed:  binary().
%%       union:  unwrapped().
%%       array:  [unwrapped()].
%%       map:    [{Key :: string(), Value :: unwrapped()}].
%%       record: [{FieldName() :: string(), unwrapped()}]}
%% @end
-spec decode_value(binary(), avro_type_or_name(),
                   schema_store() | lkup_fun(),
                   options(), hook()) -> avro_value() | term().
decode_value(JsonValue, Schema, Store, Options, Hook)
 when not is_function(Store) ->
  ExtractTypeFun = ?AVRO_SCHEMA_LOOKUP_FUN(Store),
  decode_value(JsonValue, Schema, ExtractTypeFun, Options, Hook);
decode_value(JsonValue, Schema, ExtractTypeFun, Options, Hook) ->
  DecodedJson =
    case lists:keyfind(json_decoder, 1, Options) of
      {_, mochijson3} ->
        mochijson3:decode(JsonValue);
      _ ->
        case jsonx:decode(JsonValue, [{format, struct}]) of
          {error, _Err, _Pos} ->
            mochijson3:decode(JsonValue);
          Decoded ->
            Decoded
        end
    end,
  IsWrapped = case lists:keyfind(is_wrapped, 1, Options) of
                {is_wrapped, V} -> V;
                false           -> true %% parse to wrapped value by default
              end,
  parse(DecodedJson, Schema, ExtractTypeFun, IsWrapped, Hook).

%% @doc This function is kept for backward compatibility.
%% it calls decode_value/4 forcing to use mochijson3 as json decoder.
%% @end
-spec decode_value(binary(),
                   avro_type_or_name(),
                   schema_store() | lkup_fun()) -> avro_value().
decode_value(JsonValue, Schema, StoreOrLkupFun) ->
  decode_value(JsonValue, Schema, StoreOrLkupFun,
               [{json_decoder, mochijson3}]).

decode_value(JsonValue, Schema, StoreOrLkupFun, Options) ->
  decode_value(JsonValue, Schema, StoreOrLkupFun,
               Options, ?DEFAULT_DECODER_HOOK).

%%%===================================================================
%%% Schema parsing
%%%===================================================================

parse_schema({struct, Attrs}, EnclosingNs, ExtractTypeFun) ->
  %% Json object: this is a type definition (except for unions)
  parse_type(Attrs, EnclosingNs, ExtractTypeFun);
parse_schema(Array, EnclosingNs, ExtractTypeFun) when is_list(Array) ->
  %% Json array: this is an union definition
  parse_union_type(Array, EnclosingNs, ExtractTypeFun);
parse_schema(NameBin, EnclosingNs, _ExtractTypeFun) when is_binary(NameBin) ->
  %% Json string: this is a type name. If the name corresponds to one
  %% of primitive types then return it, otherwise make full name.
  case type_from_name(NameBin) of
    undefined ->
      Name = binary_to_list(NameBin),
      avro_util:verify_dotted_name(Name),
      avro:build_type_fullname(Name, EnclosingNs, EnclosingNs);
    Type ->
      Type
  end;
parse_schema(_, _EnclosingNs, _ExtractTypeFun) ->
  %% Other Json value
  erlang:error(unexpected_element_in_schema).

parse_type(Attrs, EnclosingNs, ExtractTypeFun) ->
  TypeAttr = avro_util:get_opt(<<"type">>, Attrs),
  case TypeAttr of
    <<?AVRO_RECORD>> -> parse_record_type(Attrs, EnclosingNs, ExtractTypeFun);
    <<?AVRO_ENUM>>   -> parse_enum_type(Attrs, EnclosingNs);
    <<?AVRO_ARRAY>>  -> parse_array_type(Attrs, EnclosingNs, ExtractTypeFun);
    <<?AVRO_MAP>>    -> parse_map_type(Attrs, EnclosingNs, ExtractTypeFun);
    <<?AVRO_FIXED>>  -> parse_fixed_type(Attrs, EnclosingNs);
    _                -> case type_from_name(TypeAttr) of
                          undefined -> erlang:error(unknown_type);
                          Type      -> Type
                        end
  end.

parse_record_type(Attrs, EnclosingNs, ExtractTypeFun) ->
  NameBin = avro_util:get_opt(<<"name">>,      Attrs),
  NsBin   = avro_util:get_opt(<<"namespace">>, Attrs, <<"">>),
  Doc     = avro_util:get_opt(<<"doc">>,       Attrs, <<"">>),
  Aliases = avro_util:get_opt(<<"aliases">>,   Attrs, []),
  Fields  = avro_util:get_opt(<<"fields">>,    Attrs),
  Name    = binary_to_list(NameBin),
  Ns      = binary_to_list(NsBin),
  %% Based on the record's own namespace and the enclosing namespace
  %% new enclosing namespace for all types inside the record is
  %% calculated.
  {_, RecordNs} = avro:split_type_name(Name, Ns, EnclosingNs),
  avro_record:type(Name,
                   parse_record_fields(Fields, RecordNs, ExtractTypeFun),
                   [ {namespace, Ns}
                   , {doc, binary_to_list(Doc)}
                   , {aliases, Aliases}
                   , {enclosing_ns, EnclosingNs}
                   ]).

parse_record_fields(Fields, EnclosingNs, ExtractTypeFun) ->
  lists:map(fun({struct, FieldAttrs}) ->
                parse_record_field(FieldAttrs, EnclosingNs, ExtractTypeFun);
               (_) ->
                erlang:error(wrong_record_field_specification)
            end,
            Fields).

parse_record_field(Attrs, EnclosingNs, ExtractTypeFun) ->
  Name      = avro_util:get_opt(<<"name">>,    Attrs),
  Doc       = avro_util:get_opt(<<"doc">>,     Attrs, <<"">>),
  Type      = avro_util:get_opt(<<"type">>,    Attrs),
  Default   = avro_util:get_opt(<<"default">>, Attrs, undefined),
  Order     = avro_util:get_opt(<<"order">>,   Attrs, <<"ascending">>),
  Aliases   = avro_util:get_opt(<<"aliases">>, Attrs, []),
  FieldType = parse_schema(Type, EnclosingNs, ExtractTypeFun),
  #avro_record_field
  { name    = binary_to_list(Name)
  , doc     = binary_to_list(Doc)
  , type    = FieldType
  , default = parse_default_value(Default, FieldType, ExtractTypeFun)
  , order   = parse_order(Order)
  , aliases = parse_aliases(Aliases)
  }.

parse_default_value(undefined, _FieldType, _ExtractTypeFun) ->
  undefined;
parse_default_value(Value, FieldType, ExtractTypeFun)
  when ?AVRO_IS_UNION_TYPE(FieldType) ->
  %% Strange agreement about unions: default value for an union field
  %% corresponds to the first type in this union.
  %% Why not to use normal union values format?
  [FirstType|_] = avro_union:get_types(FieldType),
  avro_union:new(FieldType, parse_value(Value, FirstType, ExtractTypeFun));
parse_default_value(Value, FieldType, ExtractTypeFun) ->
  parse_value(Value, FieldType, ExtractTypeFun).

parse_order(<<"ascending">>)  -> ascending;
parse_order(<<"descending">>) -> ascending;
parse_order(<<"ignore">>)     -> ignore;
parse_order(Order)            -> erlang:error({unknown_sort_order, Order}).

parse_enum_type(Attrs, EnclosingNs) ->
  NameBin = avro_util:get_opt(<<"name">>,      Attrs),
  NsBin   = avro_util:get_opt(<<"namespace">>, Attrs, <<"">>),
  Doc     = avro_util:get_opt(<<"doc">>,       Attrs, <<"">>),
  Aliases = avro_util:get_opt(<<"aliases">>,   Attrs, []),
  Symbols = avro_util:get_opt(<<"symbols">>,   Attrs),
  avro_enum:type(binary_to_list(NameBin),
                 parse_enum_symbols(Symbols),
                 [ {namespace,    binary_to_list(NsBin)}
                 , {doc,          binary_to_list(Doc)}
                 , {aliases,      parse_aliases(Aliases)}
                 , {enclosing_ns, EnclosingNs}
                 ]).

parse_enum_symbols(SymbolsArray) when is_list(SymbolsArray) ->
  lists:map(
    fun(SymBin) when is_binary(SymBin) ->
        erlang:binary_to_list(SymBin);
       (_) ->
        erlang:error(wrong_enum_symbols_specification)
    end,
    SymbolsArray);
parse_enum_symbols(_) ->
  erlang:error(wrong_enum_symbols_specification).

parse_array_type(Attrs, EnclosingNs, ExtractTypeFun) ->
  Items = avro_util:get_opt(<<"items">>, Attrs),
  avro_array:type(parse_schema(Items, EnclosingNs, ExtractTypeFun)).

parse_map_type(Attrs, EnclosingNs, ExtractTypeFun) ->
  Values = avro_util:get_opt(<<"values">>, Attrs),
  avro_map:type(parse_schema(Values, EnclosingNs, ExtractTypeFun)).

parse_fixed_type(Attrs, EnclosingNs) ->
  NameBin = avro_util:get_opt(<<"name">>,      Attrs),
  NsBin   = avro_util:get_opt(<<"namespace">>, Attrs, <<"">>),
  Aliases = avro_util:get_opt(<<"aliases">>,   Attrs, []),
  Size    = avro_util:get_opt(<<"size">>, Attrs),
  avro_fixed:type(binary_to_list(NameBin),
                  parse_fixed_size(Size),
                  [ {namespace,    binary_to_list(NsBin)}
                  , {aliases,      parse_aliases(Aliases)}
                  , {enclosing_ns, EnclosingNs}
                  ]).

parse_fixed_size(N) when is_integer(N) andalso N > 0 ->
  N;
parse_fixed_size(_) ->
  erlang:error(wrong_fixed_size_specification).

parse_union_type(Attrs, EnclosingNs, ExtractTypeFun) ->
  Types = lists:map(
            fun(Schema) ->
                parse_schema(Schema, EnclosingNs, ExtractTypeFun)
            end,
            Attrs),
  avro_union:type(Types).

parse_aliases(AliasesArray) when is_list(AliasesArray) ->
  lists:map(
    fun(AliasBin) when is_binary(AliasBin) ->
        Alias = binary_to_list(AliasBin),
        avro_util:verify_dotted_name(Alias),
        Alias;
       (_) ->
        erlang:error(wrong_aliases_specification)
    end,
    AliasesArray);
parse_aliases(_) ->
  erlang:error(wrong_aliases_specification).

%% Primitive types can be specified as their names
type_from_name(<<?AVRO_NULL>>)    -> avro_primitive:null_type();
type_from_name(<<?AVRO_BOOLEAN>>) -> avro_primitive:boolean_type();
type_from_name(<<?AVRO_INT>>)     -> avro_primitive:int_type();
type_from_name(<<?AVRO_LONG>>)    -> avro_primitive:long_type();
type_from_name(<<?AVRO_FLOAT>>)   -> avro_primitive:float_type();
type_from_name(<<?AVRO_DOUBLE>>)  -> avro_primitive:double_type();
type_from_name(<<?AVRO_BYTES>>)   -> avro_primitive:bytes_type();
type_from_name(<<?AVRO_STRING>>)  -> avro_primitive:string_type();
type_from_name(_)                 -> undefined.

%%%===================================================================
%%% Values parsing
%%%===================================================================

parse_value(Value, Type, ExtractFun) ->
  parse(Value, Type, ExtractFun, _IsWrapped = true, ?DEFAULT_DECODER_HOOK).

parse(Value, TypeName, ExtractFun, IsWrapped, Hook) when is_list(TypeName) ->
  %% Type is defined by its name
  Type = ExtractFun(TypeName),
  parse(Value, Type, ExtractFun, IsWrapped, Hook);
parse(Value, Type, _ExtractFun, IsWrapped, Hook)
 when ?AVRO_IS_PRIMITIVE_TYPE(Type) ->
 Hook(Type, "", Value,
      fun(JsonV) ->
        WrappedValue = parse_prim(JsonV, Type),
        case IsWrapped of
          true  -> WrappedValue;
          false -> avro_primitive:get_value(WrappedValue)
        end
      end);
parse(V, Type, _ExtractFun, IsWrapped, Hook) when ?AVRO_IS_ENUM_TYPE(Type),
                                                  is_binary(V) ->
  Hook(Type, "", V,
       fun(JsonV) ->
         case IsWrapped of
           true  -> avro_enum:new(Type, binary_to_list(JsonV));
           false -> binary_to_list(JsonV)
         end
       end);
parse(V, Type, _ExtractFun, IsWrapped, Hook) when ?AVRO_IS_FIXED_TYPE(Type) ->
  Hook(Type, "", V,
       fun(JsonV) ->
         case IsWrapped of
           true  -> avro_fixed:new(Type, parse_bytes(JsonV));
           false -> parse_bytes(JsonV)
         end
       end);
parse(V, Type, ExtractFun, IsWrapped, Hook) when ?AVRO_IS_RECORD_TYPE(Type) ->
  parse_record(V, Type, ExtractFun, IsWrapped, Hook);
parse(V, Type, ExtractFun, IsWrapped, Hook) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  parse_array(V, Type, ExtractFun, IsWrapped, Hook);
parse(V, Type, ExtractFun, IsWrapped, Hook) when ?AVRO_IS_MAP_TYPE(Type) ->
  parse_map(V, Type, ExtractFun, IsWrapped, Hook);
parse(V, Type, ExtractFun, IsWrapped, Hook) when ?AVRO_IS_UNION_TYPE(Type) ->
  parse_union(V, Type, ExtractFun, IsWrapped, Hook);
parse(Value, Type, _ExtractFun, _IsWrapped, _Hook) ->
  erlang:error({value_does_not_correspond_to_schema, Type, Value}).

%% Parse primitive values, return wrapped value.
parse_prim(null, Type) when ?AVRO_IS_NULL_TYPE(Type) ->
  avro_primitive:null();
parse_prim(V, Type) when ?AVRO_IS_BOOLEAN_TYPE(Type) andalso is_boolean(V) ->
  avro_primitive:boolean(V);
parse_prim(V, Type) when ?AVRO_IS_INT_TYPE(Type) andalso
                         is_integer(V)           andalso
                         V >= ?INT4_MIN          andalso
                         V =< ?INT4_MAX ->
  avro_primitive:int(V);
parse_prim(V, Type) when ?AVRO_IS_LONG_TYPE(Type) andalso
                          is_integer(V)            andalso
                          V >= ?INT8_MIN           andalso
                          V =< ?INT8_MAX ->
  avro_primitive:long(V);
parse_prim(V, Type) when ?AVRO_IS_FLOAT_TYPE(Type) andalso
                          (is_float(V) orelse is_integer(V)) ->
  avro_primitive:float(V);
parse_prim(V, Type) when ?AVRO_IS_DOUBLE_TYPE(Type) andalso
                         (is_float(V) orelse is_integer(V)) ->
  avro_primitive:double(V);
parse_prim(V, Type) when ?AVRO_IS_BYTES_TYPE(Type) andalso
                         is_binary(V) ->
  Bin = parse_bytes(V),
  avro_primitive:bytes(Bin);
parse_prim(V, Type) when ?AVRO_IS_STRING_TYPE(Type) andalso
                         is_binary(V) ->
  avro_primitive:string(binary_to_list(V)).


parse_bytes(BytesStr) ->
  list_to_binary(parse_bytes(BytesStr, [])).

parse_bytes(<<>>, Acc) ->
  lists:reverse(Acc);
parse_bytes(<<"\\u00", B1, B0, Rest/binary>>, Acc) ->
  Byte = erlang:list_to_integer([B1, B0], 16),
  parse_bytes(Rest, [Byte | Acc]);
parse_bytes(_, _) ->
  erlang:error(wrong_bytes_string).

parse_record({struct, Attrs}, Type, ExtractFun, IsWrapped, Hook) ->
  Hook(Type, none, Attrs,
       fun(JsonValues) ->
         Fields = convert_attrs_to_record_fields(JsonValues, Type, ExtractFun,
                                                 IsWrapped, Hook),
         case IsWrapped of
           true  -> avro_record:new(Type, Fields);
           false -> Fields
         end
       end);
parse_record(_, _, _, _, _) ->
  erlang:error(wrong_record_value).

convert_attrs_to_record_fields(Attrs, Type, ExtractFun, IsWrapped, Hook) ->
  lists:map(
    fun({FieldNameBin, Value}) ->
        FieldName = binary_to_list(FieldNameBin),
        FieldType = avro_record:get_field_type(FieldName, Type),
        FieldValue =
          Hook(Type, FieldName, Value,
               fun(JsonV) ->
                 parse(JsonV, FieldType, ExtractFun, IsWrapped, Hook)
               end),
        {FieldName, FieldValue}
    end,
    Attrs).

parse_array(V, Type, ExtractFun, IsWrapped, Hook) when is_list(V) ->
  ItemsType = avro_array:get_items_type(Type),
  {_Index, ParsedArray} =
    lists:foldl(
      fun(Item, {Index, Acc}) ->
        ParsedItem = Hook(Type, Index, Item,
                          fun(JsonV) ->
                            parse(JsonV, ItemsType, ExtractFun, IsWrapped, Hook)
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
  end;
parse_array(_, _, _, _, _) ->
  erlang:error(wrong_array_value).

parse_map({struct, Attrs}, Type, ExtractFun, IsWrapped, Hook) ->
  ItemsType = avro_map:get_items_type(Type),
  D = lists:foldl(
        fun({KeyBin, Value}, D) ->
            V = Hook(Type, KeyBin, Value,
                     fun(JsonV) ->
                       parse(JsonV, ItemsType, ExtractFun, IsWrapped, Hook)
                     end),
            dict:store(binary_to_list(KeyBin), V, D)
        end,
        dict:new(),
        Attrs),
  case IsWrapped of
    true  -> avro_map:new(Type, D);
    false -> dict:to_list(D)
  end.

parse_union(null = Value, Type, ExtractFun, IsWrapped, Hook) ->
  %% Union values specified as null
  parse_union_ex(?AVRO_NULL, Value, Type, ExtractFun, IsWrapped, Hook);
parse_union({struct, [{ValueTypeNameBin, Value}]},
            Type, ExtractFun, IsWrapped, Hook) ->
  %% Union value specified as {"type": <value>}
  ValueTypeName = binary_to_list(ValueTypeNameBin),
  parse_union_ex(ValueTypeName, Value, Type, ExtractFun, IsWrapped, Hook);
parse_union(_, _, _, _, _) ->
  erlang:error(wrong_union_value).

parse_union_ex(ValueTypeName, Value, UnionType,
               ExtractFun, IsWrapped, Hook) ->
  case avro_union:lookup_child_type(UnionType, ValueTypeName) of
    {ok, ValueType} ->
      ParsedValue = parse(Value, ValueType, ExtractFun, IsWrapped, Hook),
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
      erlang:error(unknown_type_of_union_value)
  end.


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
