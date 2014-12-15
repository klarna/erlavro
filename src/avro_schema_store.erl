%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc Stores all types in the schema.
%%%
%%% The module allows to access all types in uniform way by using
%%% their full names. After a type was successfully placed into
%%% the store all its name parts are resolved so that no further
%%% actions to work with names are needed.
%%%
%%% When type is added to the store all its children named types are
%%% extracted and stored as separate types as well. Their placeholders
%%% in the original type are replaced by their full names. The types
%%% itself keep their full names in 'name' field and empty strings
%%% in 'namespace'.
%%%
%%% Error will be thrown when name conflict is detected during type
%%% addition.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_schema_store).

%% API
-export([ new/0
        , new/1
        , close/1
        , add_type/2
        , lookup_type/2
        , lookup_type_json/2
        , fold/3
        ]).

-include("erlavro.hrl").

-opaque store() :: ets:tid().

-define(ETS_TABLE_NAME, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

-spec new() -> store().

new() ->
  new([]).

%% Options:
%%   {access, public|protected|private} - has same meaning as access
%%     mode in ets:new and defines what processes can have access to
%%     the store. Default value is private.
%% IDEA: {storage, ets|plain} - choose where to store the schema,
%%     in an ets table or directly in the store. Currently only ets is
%%     supported.

new(Options) ->
  Access = proplists:get_value(access, Options, private),
  init_ets_store(Access).

close(Store) ->
  destroy_ets_store(Store).

-spec add_type(avro_type(), store()) -> store().
add_type(Type, Store) ->
  case avro:is_named_type(Type) of
    true  ->
      {ConvertedType, ExtractedTypes} =
        extract_children_types(Type),
      lists:foldl(
        fun do_add_type/2,
        do_add_type(ConvertedType, Store),
        ExtractedTypes);
    false ->
      erlang:error(unnamed_type_cant_be_added)
  end.

-spec lookup_type(string(), store()) -> {ok, avro_type()} | false.
lookup_type(FullName, Store) ->
  get_type_from_store(FullName, Store).

-spec lookup_type_json(string(), store()) -> {ok, term()} | false.
lookup_type_json(FullName, Store) ->
  get_type_json_from_store(FullName, Store).

fold(F, Acc0, Store) ->
  ets:foldl(
    fun({FullName, Type}, Acc) ->
        F(FullName, Type, Acc)
    end,
    Acc0,
    Store).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_add_type(Type, Store) ->
  FullName = avro:get_type_fullname(Type),
  Aliases = avro:get_aliases(Type),
  do_add_type_by_names([FullName|Aliases], Type, Store).

do_add_type_by_names([], _Type, Store) ->
  Store;
do_add_type_by_names([Name|Rest], Type, Store) ->
  case get_type_from_store(Name, Store) of
    {ok, _} ->
      erlang:error({type_with_same_name_already_exists_in_store, Name});
    false   ->
      Store1 = put_type_to_store(Name, Type, Store),
      do_add_type_by_names(Rest, Type, Store1)
  end.

%% Extract all children types from the type if possible, replace extracted
%% types with their names. Types specified by names are replaced with their
%% full names. Type names are canonicalized: name is replaced by full name,
%% namespace is cleared.
-spec extract_children_types(avro_type())
                            -> {avro_type(), [avro_type()]}.

extract_children_types(Primitive)
  when ?AVRO_IS_PRIMITIVE_TYPE(Primitive) ->
  %% Primitive types can't have children type definitions
  {Primitive, []};
extract_children_types(Record) when ?AVRO_IS_RECORD_TYPE(Record) ->
  {NewFields, ExtractedTypes} =
    lists:foldr(
      fun(Field, {FieldsAcc, ExtractedAcc}) ->
          {NewType, Extracted} =
            convert_type(Field#avro_record_field.type),
          {[Field#avro_record_field{type = NewType}|FieldsAcc],
           Extracted ++ ExtractedAcc}
      end,
      {[], []},
      Record#avro_record_type.fields),
  {Record#avro_record_type{ fields = NewFields }, ExtractedTypes};
extract_children_types(Enum) when ?AVRO_IS_ENUM_TYPE(Enum) ->
  %% Enums don't have children types
  {Enum, []};
extract_children_types(Array) when ?AVRO_IS_ARRAY_TYPE(Array) ->
  ChildType = avro_array:get_items_type(Array),
  {NewChildType, Extracted} = convert_type(ChildType),
  {avro_array:type(NewChildType), Extracted};
extract_children_types(Map) when ?AVRO_IS_MAP_TYPE(Map) ->
  ChildType = Map#avro_map_type.type,
  {NewChildType, Extracted} = convert_type(ChildType),
  {Map#avro_map_type{type = NewChildType}, Extracted};
extract_children_types(Union) when ?AVRO_IS_UNION_TYPE(Union) ->
  ChildrenTypes = avro_union:get_types(Union),
  {NewChildren, ExtractedTypes} =
    lists:foldr(
      fun(ChildType, {ConvertedAcc, ExtractedAcc}) ->
          {ChildType1, Extracted} = convert_type(ChildType),
          {[ChildType1|ConvertedAcc], Extracted ++ ExtractedAcc}
      end,
      {[], []},
      ChildrenTypes),
  {avro_union:type(NewChildren), ExtractedTypes};
extract_children_types(Fixed) when ?AVRO_IS_FIXED_TYPE(Fixed) ->
  %% Fixed types don't have children types.
  {Fixed, []}.

%% Convert a type using following rules:
%% - if the type is represented by its name then the name is used.
%% - if the type is specified as a schema then the is replaced by
%%   its full name and the type itself is added to resulting list.
%%   Children types of the replaced type is also extracted.
convert_type(TypeName) when is_list(TypeName) ->
  {TypeName, []};
convert_type(Type) ->
  {NewType, Extracted} = extract_children_types(Type),
  case replace_type_with_name(NewType) of
    {ok, Name} -> {Name, [NewType|Extracted]};
    false      -> {NewType, Extracted}
  end.

replace_type_with_name(Type) ->
  case avro:is_named_type(Type) of
    true  ->
      %% Named type should be replaced by its name.
      {ok, avro:get_type_fullname(Type)};
    false ->
      %% Unnamed types are always kept on their places
      false
  end.

%%%===================================================================
%%% Low level store access
%%%===================================================================

init_ets_store(Access) when
    Access =:= private orelse
    Access =:= protected orelse
    Access =:= public ->
  ets:new(?ETS_TABLE_NAME, [Access]).

put_type_to_store(Name, Type, Store) ->
  true = ets:insert(Store, {Name, Type}),
  Json = avro_json_encoder:encode_type(Type),
  true = ets:insert(Store, {{json, Name}, Json}),
  Store.

get_type_from_store(Name, Store) ->
  case ets:lookup(Store, Name) of
    []             -> false;
    [{Name, Type}] -> {ok, Type}
  end.

get_type_json_from_store(Name, Store) ->
  case ets:lookup(Store, {json, Name}) of
    []                     -> false;
    [{{json, Name}, Json}] -> {ok, Json}
  end.

destroy_ets_store(Store) ->
  ets:delete(Store),
  ok.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

sub_record() ->
  avro_record:type(
    "TestSubRecord",
    [ avro_record:define_field("sub_field1", avro_primitive:boolean_type())
    , avro_record:define_field("sub_field2",
                               avro_enum:type("MyEnum", ["A"],
                                              [{namespace, "another.name"}]))
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestSubRecordAlias"]}
    ]).

extracted_sub_record() ->
  avro_record:type(
    "TestSubRecord",
    [ avro_record:define_field(
        "sub_field1", avro_primitive:boolean_type())
    , avro_record:define_field(
        "sub_field2", "another.name.MyEnum")
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestSubRecordAlias"]}
    ]).

test_record() ->
  avro_record:type(
    "TestRecord",
    [ %% simple type
      avro_record:define_field("field1", avro_primitive:int_type())
      %% huge nested type
    , avro_record:define_field(
        "field2",
        avro_array:type(
          avro_union:type(
            [ avro_primitive:string_type()
            , sub_record()
            , avro_fixed:type("MyFixed", 16,
                              [{namespace, "com.klarna.test.bix"}])
            ])))
      %% named type without explicit namespace
    , avro_record:define_field("field3", "com.klarna.test.bix.SomeType")
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestRecordAlias1", "TestRecordAlias2"]}
    ]
   ).

extracted_test_record() ->
  avro_record:type(
    "TestRecord",
    [ %% simple type
      avro_record:define_field(
        "field1", avro_primitive:int_type())
      %% huge nested type
    , avro_record:define_field(
        "field2", avro_array:type(
                    avro_union:type(
                      [ avro_primitive:string_type()
                      , "com.klarna.test.bix.TestSubRecord"
                      , "com.klarna.test.bix.MyFixed"
                      ])))
      %% named type without explicit namespace
    , avro_record:define_field(
        "field3", "com.klarna.test.bix.SomeType")
    ],
    [ {namespace, "com.klarna.test.bix"}
    , {doc, "Some doc"}
    , {aliases, ["TestRecordAlias1", "TestRecordAlias2"]}
    ]).

extract_from_primitive_type_test() ->
  Type = avro_primitive:int_type(),
  ?assertEqual({Type, []}, extract_children_types(Type)).

extract_from_nested_primitive_type_test() ->
  Type = avro_array:type(avro_primitive:int_type()),
  ?assertEqual({Type, []}, extract_children_types(Type)).

extract_from_named_type_test() ->
  Type = avro_array:type("com.klarna.test.bix.SomeType"),
  ?assertEqual({Type, []}, extract_children_types(Type)).

extract_from_extractable_type_test() ->
  Type = avro_array:type(test_record()),
  Expected = { avro_array:type("com.klarna.test.bix.TestRecord")
             , [ extracted_test_record()
               , extracted_sub_record()
               , avro_enum:type("MyEnum", ["A"],
                                [{namespace, "another.name"}])
               , avro_fixed:type("MyFixed", 16,
                                 [{namespace, "com.klarna.test.bix"}])
               ]
             },
  ?assertEqual(Expected, extract_children_types(Type)).

add_type_test() ->
  Store = new(),
  Store1 = add_type(test_record(), Store),
  ?assertEqual({ok, extracted_test_record()},
               lookup_type("com.klarna.test.bix.TestRecord", Store1)),
  ?assertEqual({ok, extracted_test_record()},
               lookup_type("com.klarna.test.bix.TestRecordAlias1", Store1)),
  ?assertEqual({ok, extracted_sub_record()},
               lookup_type("com.klarna.test.bix.TestSubRecord", Store1)),
  ?assertEqual({ok, extracted_sub_record()},
               lookup_type("com.klarna.test.bix.TestSubRecordAlias", Store1)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
