%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc Avro Json decoder
%%% @end
%%%-------------------------------------------------------------------
-module(avro_json_decoder).

%% API
-export([decode_schema/2]).
-export([decode_value/3]).

-include("erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% Decode Avro schema specified as Json string.
%% ExtractTypeFun should be a function returning Avro type by its full name,
%% it is needed to parse default values.
-spec decode_schema(string(),
                    fun((string()) -> avro_type()))
                    -> avro_type().

decode_schema(JsonSchema, ExtractTypeFun) ->
  parse_schema(mochijson3:decode(JsonSchema), "", ExtractTypeFun).

%% Decode value specified as Json string according to Avro schema
%% in Schema. ExtractTypeFun should be provided to retrieve types
%% specified by their names inside Schema.
-spec decode_value(string(),
                   avro_type_or_name(),
                   fun((string()) -> avro_type()))
                  -> avro_value().

decode_value(JsonValue, Schema, ExtractTypeFun) ->
  parse_value(mochijson3:decode(JsonValue), Schema, ExtractTypeFun).

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
      avro_check:verify_dotted_name(Name),
      avro:build_type_fullname(Name, EnclosingNs, EnclosingNs);
    Type ->
      Type
  end;
parse_schema(_, _EnclosingNs, _ExtractTypeFun) ->
  %% Other Json value
  erlang:error(unexpected_element_in_schema).

parse_type(Attrs, EnclosingNs, ExtractTypeFun) ->
  TypeAttr = get_attr_value(<<"type">>, Attrs),
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
  NameBin = get_attr_value(<<"name">>,      Attrs),
  NsBin   = get_attr_value(<<"namespace">>, Attrs, <<"">>),
  Doc     = get_attr_value(<<"doc">>,       Attrs, <<"">>),
  Aliases = get_attr_value(<<"aliases">>,   Attrs, []),
  Fields  = get_attr_value(<<"fields">>,    Attrs),
  Name    = binary_to_list(NameBin),
  Ns      = binary_to_list(NsBin),
  %% Based on the record's own namespace and the enclosing namespace
  %% new enclosing namespace for all types inside the record is
  %% calculated.
  {_, RecordNs} = avro:split_type_name(Name, Ns, EnclosingNs),
  avro_record:type(Name,
                   Ns,
                   binary_to_list(Doc),
                   parse_record_fields(Fields, RecordNs, ExtractTypeFun),
                   Aliases,
                   EnclosingNs).

parse_record_fields(Fields, EnclosingNs, ExtractTypeFun) ->
  lists:map(fun({struct, FieldAttrs}) ->
                parse_record_field(FieldAttrs, EnclosingNs, ExtractTypeFun);
               (_) ->
                erlang:error(wrong_record_field_specification)
            end,
            Fields).

parse_record_field(Attrs, EnclosingNs, ExtractTypeFun) ->
  Name      = get_attr_value(<<"name">>,    Attrs),
  Doc       = get_attr_value(<<"doc">>,     Attrs, <<"">>),
  Type      = get_attr_value(<<"type">>,    Attrs),
  Default   = get_attr_value(<<"default">>, Attrs, undefined),
  Order     = get_attr_value(<<"order">>,   Attrs, <<"ascending">>),
  Aliases   = get_attr_value(<<"aliases">>, Attrs, []),
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
  NameBin = get_attr_value(<<"name">>,      Attrs),
  NsBin   = get_attr_value(<<"namespace">>, Attrs, <<"">>),
  Doc     = get_attr_value(<<"doc">>,       Attrs, <<"">>),
  Aliases = get_attr_value(<<"aliases">>,   Attrs, []),
  Symbols = get_attr_value(<<"symbols">>,   Attrs),
  Name    = binary_to_list(NameBin),
  Ns      = binary_to_list(NsBin),
  Type = #avro_enum_type
         { name      = binary_to_list(NameBin)
         , namespace = binary_to_list(NsBin)
         , doc       = binary_to_list(Doc)
         , aliases   = parse_aliases(Aliases)
         , symbols   = parse_enum_symbols(Symbols)
         , fullname  = avro:build_type_fullname(Name, Ns, EnclosingNs)
         },
  avro_check:verify_type(Type),
  Type.

parse_enum_symbols(SymbolsArray) when is_list(SymbolsArray) ->
  Symbols = lists:map(
              fun(SymBin) when is_binary(SymBin) ->
                  erlang:binary_to_list(SymBin);
                 (_) ->
                  erlang:error(wrong_enum_symbols_specification)
              end,
              SymbolsArray),
  %% TODO: check correctness and uniqueness
  Symbols;
parse_enum_symbols(_) ->
  erlang:error(wrong_enum_symbols_specification).

parse_array_type(Attrs, EnclosingNs, ExtractTypeFun) ->
  Items = get_attr_value(<<"items">>, Attrs),
  avro_array:type(parse_schema(Items, EnclosingNs, ExtractTypeFun)).

parse_map_type(Attrs, EnclosingNs, ExtractTypeFun) ->
  Values = get_attr_value(<<"values">>, Attrs),
  #avro_map_type
  { type = parse_schema(Values, EnclosingNs, ExtractTypeFun)
  }.

parse_fixed_type(Attrs, EnclosingNs) ->
  NameBin = get_attr_value(<<"name">>,      Attrs),
  NsBin   = get_attr_value(<<"namespace">>, Attrs, <<"">>),
  Aliases = get_attr_value(<<"aliases">>,   Attrs, []),
  Size    = get_attr_value(<<"size">>, Attrs),
  Name    = binary_to_list(NameBin),
  Ns      = binary_to_list(NsBin),
  Type = #avro_fixed_type
         { name      = Name
         , namespace = Ns
         , aliases   = parse_aliases(Aliases)
         , size      = parse_fixed_size(Size)
         , fullname  = avro:build_type_fullname(Name, Ns, EnclosingNs)
         },
  avro_check:verify_type(Type),
  Type.

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
        avro_check:verify_dotted_name(Alias),
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

parse_value(null, Type, _ExtractFun) when ?AVRO_IS_NULL_TYPE(Type) ->
  avro_primitive:null();

parse_value(V, Type, _ExtractFun) when ?AVRO_IS_BOOLEAN_TYPE(Type) andalso
                                       is_boolean(V) ->
  avro_primitive:boolean(V);

parse_value(V, Type, _ExtractFun) when ?AVRO_IS_INT_TYPE(Type) andalso
                                       is_integer(V)           andalso
                                       V >= ?INT4_MIN          andalso
                                       V =< ?INT4_MAX ->
  avro_primitive:int(V);

parse_value(V, Type, _ExtractFun) when ?AVRO_IS_LONG_TYPE(Type) andalso
                                       is_integer(V)            andalso
                                       V >= ?INT8_MIN           andalso
                                       V =< ?INT8_MAX ->
  avro_primitive:long(V);

parse_value(V, Type, _ExtractFun) when ?AVRO_IS_FLOAT_TYPE(Type) andalso
                                       (is_float(V) orelse is_integer(V)) ->
  avro_primitive:float(V);

parse_value(V, Type, _ExtractFun) when ?AVRO_IS_DOUBLE_TYPE(Type) andalso
                                       (is_float(V) orelse is_integer(V)) ->
  avro_primitive:double(V);

parse_value(V, Type, _ExtractFun) when ?AVRO_IS_BYTES_TYPE(Type) andalso
                                       is_binary(V) ->
  Bin = parse_bytes(V),
  avro_primitive:bytes(Bin);

parse_value(V, Type, _ExtractFun) when ?AVRO_IS_STRING_TYPE(Type) andalso
                                       is_binary(V) ->
  avro_primitive:string(binary_to_list(V));

parse_value(V, Type, ExtractFun) when ?AVRO_IS_RECORD_TYPE(Type) ->
  parse_record(V, Type, ExtractFun);

parse_value(V, Type, ExtractFun) when ?AVRO_IS_ENUM_TYPE(Type) ->
  parse_enum(V, Type, ExtractFun);

parse_value(V, Type, ExtractFun) when ?AVRO_IS_ARRAY_TYPE(Type) ->
  parse_array(V, Type, ExtractFun);

parse_value(V, Type, ExtractFun) when ?AVRO_IS_MAP_TYPE(Type) ->
  parse_map(V, Type, ExtractFun);

parse_value(V, Type, ExtractFun) when ?AVRO_IS_UNION_TYPE(Type) ->
  parse_union(V, Type, ExtractFun);

parse_value(V, Type, ExtractFun) when ?AVRO_IS_FIXED_TYPE(Type) ->
  parse_fixed(V, Type, ExtractFun);

parse_value(Value, SchemaName, ExtractFun) when is_list(SchemaName) ->
  %% Type is defined by its name
  Schema = ExtractFun(SchemaName),
  parse_value(Value, Schema, ExtractFun);

parse_value(_Value, _Schema, _ExtractFun) ->
  erlang:error(value_does_not_correspond_to_schema).

parse_bytes(BytesStr) ->
  list_to_binary(parse_bytes(BytesStr, [])).

parse_bytes(<<>>, Acc) ->
  lists:reverse(Acc);
parse_bytes(<<"\\u00", B1, B0, Rest/binary>>, Acc) ->
  Byte = erlang:list_to_integer([B1, B0], 16),
  parse_bytes(Rest, [Byte | Acc]);
parse_bytes(_, _) ->
  erlang:error(wrong_bytes_string).

parse_record({struct, Attrs}, Type, ExtractFun) ->
  parse_record(Attrs, Type, ExtractFun);
parse_record(Attrs, Type, ExtractFun) when is_list(Attrs) ->
  Fields = convert_attrs_to_record_fields(Attrs, Type, ExtractFun),
  avro_record:new(Type, Fields);
parse_record(_, _, _) ->
  erlang:error(wrong_record_value).

convert_attrs_to_record_fields(Attrs, Type, ExtractFun) ->
  lists:map(
    fun({FieldNameBin, Value}) ->
        FieldName = binary_to_list(FieldNameBin),
        FieldType = avro_record:get_field_type(FieldName, Type),
        {FieldName, parse_value(Value, FieldType, ExtractFun)}
    end,
    Attrs).

parse_enum(_V, _Type, _ExtractFun) ->
  erlang:error(enums_support_is_coming_soon).

parse_array(V, Type, ExtractFun) when is_list(V) ->
  ItemsType = avro_array:get_items_type(Type),
  Items = lists:map(
            fun(Item) ->
                parse_value(Item, ItemsType, ExtractFun)
            end,
            V),
  avro_array:new(Type, Items);
parse_array(_, _, _) ->
  erlang:error(wrong_array_value).

parse_map(_V, _Type, _ExtractFun) ->
  erlang:error(maps_support_is_coming_soon).

parse_union(null = Value, Type, ExtractFun) ->
  %% Union values specified as null
  parse_union_ex(?AVRO_NULL, Value, Type, ExtractFun);
parse_union({struct, [{ValueTypeNameBin, Value}]}, Type, ExtractFun) ->
  %% Union value specified as {"type": <value>}
  ValueTypeName = binary_to_list(ValueTypeNameBin),
  parse_union_ex(ValueTypeName, Value, Type, ExtractFun);
parse_union(_, _, _) ->
  erlang:error(wrong_union_value).

parse_union_ex(ValueTypeName, Value, UnionType, ExtractFun) ->
  PossibleTypes = avro_union:get_types(UnionType),
  case lookup_type_by_name(ValueTypeName, PossibleTypes) of
    {ok, ValueType} ->
      avro_union:new(UnionType, parse_value(Value, ValueType, ExtractFun));
    false ->
      erlang:error(unknown_type_of_union_value)
  end.

%% Search for a type through list of types by its full name.
%% Since full names for unnamed types are same as their Avro names,
%% this code effectively performs resolution of all types inside
%% unions.
lookup_type_by_name(_FullName, []) ->
  false;
lookup_type_by_name(FullName, [Type|Rest]) ->
  CandidateTypeName = get_type_fullname_ex(Type),
  if FullName =:= CandidateTypeName -> {ok, Type};
     true                           -> lookup_type_by_name(FullName, Rest)
  end.

%% If type is specified by its name then return this name,
%% otherwise return type's full name.
get_type_fullname_ex(TypeName) when is_list(TypeName) ->
  TypeName;
get_type_fullname_ex(Type) ->
  avro:get_type_fullname(Type).

parse_fixed(_V, _Type, _ExtractFun) ->
  erlang:error(
    fixed_support_is_only_available_for_premium_customers_for_100_euro).

%%%===================================================================
%%% Utilities
%%%===================================================================

get_attr_value(Name, Attrs) ->
  case lists:keyfind(Name, 1, Attrs) of
    {_, Value} -> Value;
    false      -> erlang:error({required_attribute_missing, Name})
  end.

get_attr_value(Name, Attrs, Default) ->
  case lists:keyfind(Name, 1, Attrs) of
    {_, Value} -> Value;
    false      -> Default
  end.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

get_test_record() ->
  Fields = [ avro_record:field("invno", avro_primitive:long_type(), "")
           , avro_record:field("array",
                               avro_array:type(avro_primitive:string_type()),
                               "")
           , avro_record:field("union",
                               avro_union:type([ avro_primitive:null_type()
                                               , avro_primitive:int_type()
                                               , avro_primitive:boolean_type()
                                               ]),
                               "")],
  avro_record:type("Test", "name.space", "", Fields).

parse_primitive_type_name_test() ->
  %% Check that primitive types specified by their names are parsed correctly
  ?assertEqual(avro_primitive:int_type(),
               parse_schema(<<"int">>, "foobar", none)).

parse_primitive_type_object_test() ->
  %% Check that primitive types specified by type objects are parsed correctly
  Schema = {struct, [{<<"type">>, <<"int">>}]},
  ?assertEqual(avro_primitive:int_type(),
               parse_schema(Schema, "foobar", none)).

parse_record_type_test() ->
  Schema = {struct,
            [ {<<"type">>, <<"record">>}
            , {<<"name">>, <<"TestRecord">>}
            , {<<"namespace">>, <<"name.space">>}
            , {<<"fields">>, []}
            ]},
  Record = parse_schema(Schema, "", none),
  ?assertEqual(avro_record:type("TestRecord", "name.space", "", []),
               Record).

parse_record_type_with_default_values_test() ->
  Schema = {struct,
            [ {<<"type">>, <<"record">>}
            , {<<"name">>, <<"TestRecord">>}
            , {<<"namespace">>, <<"name.space">>}
            , {<<"fields">>,
               [ {struct, [ {<<"name">>, <<"string_field">>}
                          , {<<"type">>, <<"string">>}
                          , {<<"default">>, <<"FOOBAR">>}
                          ]}
               , {struct, [ {<<"name">>, <<"union_field">>}
                          , {<<"type">>, [<<"boolean">>, <<"int">>]}
                          , {<<"default">>, true}
                          ]}
               ]}
            ]},
  Record = parse_schema(Schema, "", none),
  ExpectedUnion = avro_union:type([ avro_primitive:boolean_type()
                                  , avro_primitive:int_type()]),
  Expected = avro_record:type(
               "TestRecord",
               "name.space",
               "",
               [ avro_record:field("string_field",
                                   avro_primitive:string_type(),
                                   "",
                                   avro_primitive:string("FOOBAR"))
               , avro_record:field("union_field",
                                   ExpectedUnion,
                                   "",
                                   avro_union:new(ExpectedUnion,
                                                  avro_primitive:boolean(true)))
               ]),
  ?assertEqual(Expected, Record).

parse_record_type_with_enclosing_namespace_test() ->
  Schema= {struct,
           [ {<<"type">>, <<"record">>}
           , {<<"name">>, <<"TestRecord">>}
           , {<<"fields">>, []}
           ]},
  Record = parse_schema(Schema, "name.space", none),
  ?assertEqual("name.space.TestRecord",  avro:get_type_fullname(Record)).

parse_union_type_test() ->
  Schema = [ <<"int">>
           , <<"string">>
           , <<"typename">>
           ],
  Union = parse_schema(Schema, "name.space", none),
  ?assertEqual(avro_union:type([avro_primitive:int_type(),
                                avro_primitive:string_type(),
                                "name.space.typename"]),
               Union).

parse_bytes_value_test() ->
  Json = <<"\\u0010\\u0000\\u00FF">>,
  Value = parse_value(Json, avro_primitive:bytes_type(), none),
  ?assertEqual(avro_primitive:bytes(<<16,0,255>>), Value).

parse_record_value_test() ->
  %% This test also tests parsing other types inside the record
  TestRecord = get_test_record(),
  Json = {struct,
          [ {<<"invno">>, 100}
          , {<<"array">>, [<<"ACTIVE">>, <<"CLOSED">>]}
          , {<<"union">>, {struct, [{<<"boolean">>, true}]}}
          ]},
  Value = parse_value(Json, TestRecord, none),
  ?assertEqual(avro_primitive:long(100), avro_record:get("invno", Value)),
  ?assertEqual(avro_array:new(avro_record:get_field_type("array", TestRecord),
                              [avro_primitive:string("ACTIVE"),
                               avro_primitive:string("CLOSED")]),
               avro_record:get("array", Value)),
  ?assertEqual(avro_primitive:boolean(true),
               avro_union:get_value(avro_record:get("union", Value))).

parse_record_value_missing_field_test() ->
  %% This test also tests parsing other types inside the record
  TestRecord = get_test_record(),
  Json = {struct,
          [ {<<"invno">>, 100}
          , {<<"union">>, {struct, [{<<"boolean">>, true}]}}
          ]},
  %% parse_value(Json, TestRecord, none),
  %% ok.
  ?assertError({required_field_missed, "array"},
               parse_value(Json, TestRecord, none)).

parse_record_value_unknown_field_test() ->
  %% This test also tests parsing other types inside the record
  TestRecord = get_test_record(),
  Json = {struct,
          [ {<<"invno">>, 100}
          , {<<"array">>, [<<"ACTIVE">>, <<"CLOSED">>]}
          , {<<"union">>, {struct, [{<<"boolean">>, true}]}}
          , {<<"unknown_field">>, 1}
          ]},
  ?assertError({unknown_field, "unknown_field"},
               parse_value(Json, TestRecord, none)).

parse_union_value_primitive_test() ->
  Type = avro_union:type([ avro_primitive:null_type()
                         , avro_primitive:string_type()]),
  Json = {struct, [{<<"string">>, <<"str">>}]},
  Value = parse_value(Json, Type, none),
  ?assertEqual(avro_primitive:string("str"), avro_union:get_value(Value)).

parse_union_value_null_test() ->
  Type = avro_union:type([ avro_primitive:null_type()
                         , avro_primitive:string_type()]),
  Json = null,
  Value = parse_value(Json, Type, none),
  ?assertEqual(avro_primitive:null(), avro_union:get_value(Value)).

parse_union_value_fail_test() ->
  Type = avro_union:type([ avro_primitive:null_type()
                         , avro_primitive:string_type()]),
  Json = {struct, [{<<"boolean">>, true}]},
  ?assertError(unknown_type_of_union_value, parse_value(Json, Type, none)).

parse_value_with_extract_type_fun_test() ->
  ExtractTypeFun = fun("name.space.Test") ->
                       get_test_record()
                   end,
  Schema = {struct, [ {<<"type">>, <<"array">>}
                    , {<<"items">>, <<"Test">>}
                    ]},
  ValueJson = [{struct,
                [ {<<"invno">>, 100}
                , {<<"array">>, [<<"ACTIVE">>, <<"CLOSED">>]}
                , {<<"union">>, {struct, [{<<"boolean">>, true}]}}
                ]}],
  Type = parse_schema(Schema, "name.space", ExtractTypeFun),
  ExpectedType = avro_array:type("name.space.Test"),
  ?assertEqual(ExpectedType, Type),
  Value = parse_value(ValueJson, Type, ExtractTypeFun),
  [Rec] = avro_array:get(Value),
  ?assert(?AVRO_IS_RECORD_VALUE(Rec)),
  ?assertEqual("name.space.Test",
               avro:get_type_fullname(?AVRO_VALUE_TYPE(Rec))),
  ?assertEqual(avro_primitive:long(100), avro_record:get("invno", Rec)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
