%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2013-2018 Klarna AB
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
%%% @doc Collections of Avro utility functions shared between other modules.
%%% Should not be used externally.
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro_util).

%% API
-export([ canonicalize_aliases/2
        , canonicalize_custom_props/1
        , canonicalize_name/1
        , canonicalize_type_or_name/1
        , delete_opts/2
        , encode_defaults/2
        , ensure_binary/1
        , ensure_lkup_fun/1
        , expand_type/2
        , expand_type/3
        , flatten_type/1
        , get_opt/2
        , get_opt/3
        , is_compatible/2
        , make_lkup_fun/2
        , parse_defaults/2
        , resolve_duplicated_refs/1
        , validate/2
        , verify_names/1
        , verify_dotted_name/1
        , verify_aliases/1
        , verify_type/1
        ]).

%% Exported for test
-export([ is_valid_dotted_name/1
        , is_valid_name/1
        , tokens_ex/2
        ]).

-include("avro_internal.hrl").

-type lkup() :: avro:lkup_fun().
-define(SEEN, avro_get_nested_type_seen_full_names).

-type validate_hist() :: [{avro:fullname(), ref | def}].
-type validate_fun() :: fun((type_or_name(), validate_hist()) -> ok).

%%%_* APIs =====================================================================

%% @doc Validate type definitions according to given options.
-spec validate(avro:avro_type(), avro:sc_opts()) -> ok | no_return().
validate(Type, Opts) ->
  AllowBadRefs = proplists:get_bool(allow_bad_references, Opts),
  AllowTypeRedef = proplists:get_bool(allow_type_redefine, Opts),
  validate(Type, AllowBadRefs, AllowTypeRedef).

%% @doc Ensure type lookup fun.
-spec ensure_lkup_fun(avro:schema_all()) -> lkup().
ensure_lkup_fun(JSON) when is_binary(JSON) ->
  ensure_lkup_fun(avro:decode_schema(JSON));
ensure_lkup_fun(Lkup) when is_function(Lkup, 1) ->
  Lkup;
ensure_lkup_fun(Sc) ->
  case avro_schema_store:is_store(Sc) of
    true -> avro_schema_store:to_lookup_fun(Sc);
    false -> make_lkup_fun(?ASSIGNED_NAME, Sc)
  end.

%% @doc Make a schema store (dict based) and wrap it in a lookup fun.
-spec make_lkup_fun(name_raw(), avro_type()) -> lkup().
make_lkup_fun(AssignedName, Type) ->
  Store0 = avro_schema_store:new([dict]),
  Store = avro_schema_store:add_type(AssignedName, Type, Store0),
  avro_schema_store:to_lookup_fun(Store).

%% @doc Go recursively into the type tree to encode default values.
%% into JSON format, the encoded default values are later used (inline)
%% to construct JSON schema.
%% Type lookup function `Lkup' is to encode default value for named types
%% @end
-spec encode_defaults(type_or_name(), lkup()) -> avro_type().
encode_defaults(N, _) when ?IS_NAME(N) -> N;
encode_defaults(T, _) when ?IS_PRIMITIVE_TYPE(T) -> T;
encode_defaults(T, _) when ?IS_FIXED_TYPE(T) -> T;
encode_defaults(T, _) when ?IS_ENUM_TYPE(T) -> T;
encode_defaults(T, ParseFun) when ?IS_MAP_TYPE(T) ->
  avro_map:update_items_type(T, fun(ST) -> encode_defaults(ST, ParseFun) end);
encode_defaults(T, ParseFun) when ?IS_ARRAY_TYPE(T) ->
  avro_array:update_items_type(T, fun(ST) -> encode_defaults(ST, ParseFun) end);
encode_defaults(T, ParseFun) when ?IS_UNION_TYPE(T) ->
  avro_union:update_member_types(T, fun(M) -> encode_defaults(M, ParseFun) end);
encode_defaults(T, ParseFun) when ?IS_RECORD_TYPE(T) ->
  avro_record:encode_defaults(T, ParseFun).

%% @doc Decode default values into `avro:out()'.
-spec parse_defaults(type_or_name(), avro_json_decoder:default_parse_fun()) ->
        type_or_name().
parse_defaults(N, _) when ?IS_NAME(N) -> N;
parse_defaults(T, _) when ?IS_PRIMITIVE_TYPE(T) -> T;
parse_defaults(T, _) when ?IS_FIXED_TYPE(T) -> T;
parse_defaults(T, _) when ?IS_ENUM_TYPE(T) -> T;
parse_defaults(T, ParseFun) when ?IS_MAP_TYPE(T) ->
  avro_map:update_items_type(T, fun(ST) -> parse_defaults(ST, ParseFun) end);
parse_defaults(T, ParseFun) when ?IS_ARRAY_TYPE(T) ->
  avro_array:update_items_type(T, fun(ST) -> parse_defaults(ST, ParseFun) end);
parse_defaults(T, ParseFun) when ?IS_UNION_TYPE(T) ->
  avro_union:update_member_types(T, fun(M) -> parse_defaults(M, ParseFun) end);
parse_defaults(T, ParseFun) when ?IS_RECORD_TYPE(T) ->
  avro_record:parse_defaults(T, ParseFun).

%% @doc Get prop from prop-list, 'error' exception if prop is not found.
-spec get_opt(Key, [{Key, Value}]) -> Value | no_return()
        when Key :: atom() | binary(), Value :: term().
get_opt(Key, Opts) ->
  case lists:keyfind(Key, 1, Opts) of
    {Key, Value} -> Value;
    false        -> erlang:error({not_found, Key})
  end.

%% @doc Get prop from prop-list, return the given Default if prop is not found.
-spec get_opt(Key, [{Key, Value}], Value) -> Value
        when Key :: atom() | binary(), Value :: term().
get_opt(Key, Opts, Default) ->
  case lists:keyfind(Key, 1, Opts) of
    {Key, Value} -> Value;
    false        -> Default
  end.

%% @doc Delete props from prop-list.
-spec delete_opts([{Key, Value}], [Key]) -> [{Key, Value}]
        when Key :: atom() | binary(), Value :: term().
delete_opts(KvList, Keys) ->
  lists:foldl(fun(K, Acc) -> lists:keydelete(K, 1, Acc) end, KvList, Keys).

%% @doc Ensure all keys and values are binary in custom properties.
-spec canonicalize_custom_props([{KeyIn, ValueIn}]) -> [{KeyOut, ValueOut}]
        when KeyIn :: name_raw(),
             KeyOut :: name(),
             ValueIn :: atom() | number() | string() | binary()
                      | [atom() | string() | binary()],
             ValueOut :: number() | binary() |  [number() | binary()].
canonicalize_custom_props(Props0) ->
  %% Filter out all type_prop_name() keys first
  Props = delete_opts(Props0, [namespace, doc, aliases]),
  CanonicalizeValue = fun(V) when is_number(V)  -> V;
                         (V) when is_boolean(V) -> V;
                         (V)                    -> ensure_binary(V)
                      end,
  F = fun({K, V}) ->
          Key = ensure_binary(K),
          Val = case is_array(V) of
                  true  -> lists:map(CanonicalizeValue, V);
                  false -> CanonicalizeValue(V)
                end,
          {Key, Val}
      end,
  lists:map(F, Props).

%% @doc Assert validity of a list of names.
-spec verify_names([name_raw()]) -> ok | no_return().
verify_names(Names) ->
  lists:foreach(
    fun(Name) ->
      ?ERROR_IF_NOT(is_valid_name(name_string(Name)), {invalid_name, Name})
    end, Names).

%% @doc Assert validity of a fullname.
-spec verify_dotted_name(name_raw()) -> ok | no_return().
verify_dotted_name(Name) ->
  ?ERROR_IF_NOT(is_valid_dotted_name(name_string(Name)),
                {invalid_name, Name}).

%% @doc Assert validity of aliases.
-spec verify_aliases([name_raw()]) -> ok | no_return().
verify_aliases(Aliases) ->
  lists:foreach(
    fun(Alias) ->
        verify_dotted_name(Alias)
    end,
    Aliases).

%% @doc Assert that the given type names are not AVRO reserved names.
-spec verify_type(avro_type()) -> ok | no_return().
verify_type(Type) ->
  true = avro:is_named_type(Type), %% assert
  verify_type_name(Type).

%% @doc Convert aliases to full-name representation using provided names and
%% namespaces from the original type
%% @end
canonicalize_aliases(Aliases, Ns) ->
  lists:map(fun(Alias) -> avro:build_type_fullname(Alias, Ns) end, Aliases).

%% @doc Convert atom() | string() to binary().
%% NOTE: Avro names are ascii only, there is no need for special utf8.
%%       handling when converting string() to binary().
%% @end
-spec canonicalize_name(name_raw()) -> name().
canonicalize_name(Name) -> ensure_binary(Name).

%% @doc Covert atom() | string() binary().
-spec ensure_binary(name_raw()) -> binary().
ensure_binary(A) when is_atom(A)   -> atom_to_binary(A, utf8);
ensure_binary(L) when is_list(L)   -> iolist_to_binary(L);
ensure_binary(B) when is_binary(B) -> B.

%% @doc Canonicalize name if it is not a type.
-spec canonicalize_type_or_name(type_or_name()) -> type_or_name().
canonicalize_type_or_name(Name) when ?IS_NAME_RAW(Name) ->
  avro:name2type(Name);
canonicalize_type_or_name(Type) when ?IS_TYPE_RECORD(Type) ->
  Type.

%% @doc Flatten out all named types, return the extracted format and all
%% (recursively extracted) children types in a list.
%% If the type is named (i.e. record type), the extracted format is its
%% full name, and the type itself is added to the extracted list.
%% @end
-spec flatten_type(type_or_name()) -> {type_or_name(), [avro_type()]}.
flatten_type(TypeName) when ?IS_NAME(TypeName) ->
  %% it's a reference
  {TypeName, []};
flatten_type(Type) when ?IS_TYPE_RECORD(Type) ->
  case avro:is_named_type(Type) of
    true ->
      {NewType, Extracted} = flatten(Type),
      Fullname = avro:get_type_fullname(NewType),
      %% Named types are replaced by their fullnames.
      {Fullname, [NewType | Extracted]};
    false ->
      flatten(Type)
  end.

%% @doc Get type and lookup sub-types recursively.
%% This should allow callers to write a root type to one avsc schema file
%% instead of scattering all named types to their own avsc files.
%% @end
-spec expand_type(type_or_name(), avro:schema_all()) ->
        avro_type() | no_return().
expand_type(Type, Sc) ->
  expand_type(Type, Sc, compact).

%% @doc Expand type in 2 different styles.
%% compact: use type name reference if it is a type seen
%%          before while traversing the type tree.
%% bloated: use type name reference only if a type is seen
%%          before in the current traversing stack. (to avoid stack overflow)
%% @end
-spec expand_type(type_or_name(), avro:schema_all(), compact | bloated) ->
        avro_type() | no_return().
expand_type(Type0, Sc, Style) ->
  Lkup = ensure_lkup_fun(Sc),
  Type = canonicalize_type_or_name(Type0),
  try
    %% Use process dictionary to keep the history of seen type names
    %% to simplify (comparing to a lists:foldr version) the loop functions.
    %%
    %% ?SEEN names are 'previously' already resolved full names of types
    %% when traversing the type tree, so there is no need to resolve
    %% the 'current' type reference again.
    %%
    %% Stack keeps only the depth first traversing path, to be used when
    %% expanding to bloated style
    _ = erlang:put(?SEEN, []),
    case ?IS_NAME(Type) of
      true  -> do_expand_type(Type, Lkup, Style, _Stack = []);
      false -> expand(Type, Lkup, Style, _Stack = [])
    end
  after
    erlang:erase(?SEEN)
  end.

%% @doc Resolve duplicated references to internal context. e.g. when two
%% or more record fields share the same type and all are expanded, we should
%% make reference for the later ones and only expand the first.
%% @end
-spec resolve_duplicated_refs(avro:type_or_name()) -> avro:type_or_name().
resolve_duplicated_refs(Type0) ->
  {Type, _Refs} = resolve_duplicated_refs(Type0, []),
  Type.

%% @doc Check if writer schema is compatible to reader schema.
-spec is_compatible(avro_type(), avro_type()) ->
                       true | {false, {not_compatible, _, _}} |
                       {false, {reader_missing_defalut_value, _}}.
is_compatible(Reader, Writer) ->
  try
    do_is_compatible_next(Reader, Writer, [], [])
  catch
    throw : {not_compatible, RPath, WPath} ->
      {false, {not_compatible, lists:reverse(RPath), lists:reverse(WPath)}};
    throw : {reader_missing_defalut_value, Path} ->
      {false, {reader_missing_default_value, lists:reverse(Path)}}
  end.

do_is_compatible_next(Reader, Writer, RPath, WPath) ->
  NewRPath = [avro:get_type_fullname(Reader) | RPath],
  NewWPath = [avro:get_type_fullname(Writer) | WPath],
  do_is_compatible(Reader, Writer, NewRPath, NewWPath).

do_is_compatible(Reader, Writer, RPath, WPath)
  when ?IS_RECORD_TYPE(Reader) andalso ?IS_RECORD_TYPE(Writer) ->
  SameType = avro:is_same_type(Reader, Writer),
  ReaderTypes = avro_record:get_all_field_data(Reader),
  WriterTypes = avro_record:get_all_field_data(Writer),
  SameType andalso
    lists:all(
      fun({FieldName, FieldType, Default}) ->
          case lists:keysearch(FieldName, 1, WriterTypes) of
            false ->
              Default =/= ?NO_VALUE orelse
                erlang:throw({ reader_missing_defalut_value
                             , [{field, FieldName} |  WPath]
                             });
            {value, {_, WriterType, _}} ->
              FieldDesc = {field, FieldName},
              do_is_compatible_next(FieldType, WriterType,
                               [FieldDesc | RPath],
                               [FieldDesc | WPath])
          end
      end,
      ReaderTypes);
do_is_compatible(Reader, Writer, RPath, WPath)
  when ?IS_ENUM_TYPE(Reader) andalso ?IS_ENUM_TYPE(Writer) ->
  avro:is_same_type(Reader, Writer) andalso
    Writer#avro_enum_type.symbols -- Reader#avro_enum_type.symbols == [] orelse
    erlang:throw({not_compatible, RPath, WPath});
do_is_compatible(Reader, Writer, RPath, WPath)
  when ?IS_FIXED_TYPE(Reader) andalso ?IS_FIXED_TYPE(Writer) ->
  avro:is_same_type(Reader, Writer)
    andalso (Reader#avro_fixed_type.size == Writer#avro_fixed_type.size) orelse
    erlang:throw({not_compatible, RPath, WPath});
do_is_compatible(Reader, Writer, RPath, WPath)
  when ?IS_UNION_TYPE(Reader) andalso ?IS_UNION_TYPE(Writer) ->
  WriterTypes = avro_union:get_types(Writer),
  lists:all(
    fun(WriterType) ->
        do_is_compatible_next(Reader, WriterType, tl(RPath), WPath)
    end, WriterTypes);
do_is_compatible(Reader, Writer, RPath, WPath)
  when ?IS_UNION_TYPE(Reader) ->
  ReaderTypes = avro_union:get_types(Reader),
  lists:any(
    fun(ReaderType) ->
        try
          do_is_compatible_next(ReaderType, Writer, RPath, tl(WPath))
        catch throw:{not_compatible, _, _} -> false;
              throw:{reader_missing_defalut_value, _} -> false
        end
    end, ReaderTypes) orelse
    erlang:throw({not_compatible, RPath, WPath});
do_is_compatible(Reader, Writer, RPath, WPath)
  when ?IS_UNION_TYPE(Writer) ->
  WriterTypes = avro_union:get_types(Writer),
  lists:all(
    fun(WriterType) ->
        do_is_compatible_next(Reader, WriterType, tl(RPath), WPath)
    end,
    WriterTypes);
do_is_compatible(Reader, Writer, RPath, WPath)
  when ?IS_ARRAY_TYPE(Reader) andalso ?IS_ARRAY_TYPE(Writer) ->
  ReaderType = avro_array:get_items_type(Reader),
  WriterType = avro_array:get_items_type(Writer),
  do_is_compatible_next(ReaderType, WriterType, RPath, WPath);
do_is_compatible(Reader, Writer, RPath, WPath)
  when ?IS_MAP_TYPE(Reader) andalso ?IS_MAP_TYPE(Writer) ->
  ReaderType = avro_map:get_items_type(Reader),
  WriterType = avro_map:get_items_type(Writer),
  do_is_compatible_next(ReaderType, WriterType, RPath, WPath);
do_is_compatible(Reader, Writer, RPath, WPath) ->
  promotable(Reader, Writer) orelse
    avro:is_same_type(Reader, Writer) orelse
    erlang:throw({not_compatible, RPath, WPath}).

%%%_* Internal functions =======================================================

%% @private
-spec resolve_duplicated_refs(avro:type_or_name(), [avro:fullname()]) ->
        {avro:type_or_name(), [avro:fullname()]}.
resolve_duplicated_refs(N, SeenRefs) when ?IS_NAME(N) ->
  {N, SeenRefs};
resolve_duplicated_refs(T, SeenRefs) when ?IS_PRIMITIVE_TYPE(T) ->
  {T, SeenRefs};
resolve_duplicated_refs(T, SeenRefs0) when ?IS_FIXED_TYPE(T) orelse
                                           ?IS_ENUM_TYPE(T) ->
  Name = avro:get_type_fullname(T),
  case lists:member(Name, SeenRefs0) of
    true -> {Name, SeenRefs0};
    false -> {T, [Name | SeenRefs0]}
  end;
resolve_duplicated_refs(#avro_array_type{type = T0} = Array, SeenRefs0) ->
  {T, SeenRefs} = resolve_duplicated_refs(T0, SeenRefs0),
  {Array#avro_array_type{type = T}, SeenRefs};
resolve_duplicated_refs(#avro_map_type{type = T0} = Map, SeenRefs0) ->
  {T, SeenRefs} = resolve_duplicated_refs(T0, SeenRefs0),
  {Map#avro_map_type{type = T}, SeenRefs};
resolve_duplicated_refs(Union, SeenRefs0) when ?IS_UNION_TYPE(Union) ->
  Members0 = avro_union:get_types(Union),
  {Members, SeenRefs} =
    lists:mapfoldl(
      fun(T, SeenRefsAcc) ->
          resolve_duplicated_refs(T, SeenRefsAcc)
      end, SeenRefs0, Members0),
  {avro_union:type(Members), SeenRefs};
resolve_duplicated_refs(#avro_record_type{fields = Fields0} = R, SeenRefs0) ->
  Name = avro:get_type_fullname(R),
  case lists:member(Name, SeenRefs0) of
    true ->
      {Name, SeenRefs0};
    false ->
      SeenRefs1 = [Name | SeenRefs0],
      {Fields, SeenRefs} =
        lists:mapfoldl(
          fun(#avro_record_field{type = FT} = Field, SeenRefsAcc) ->
              {NewFT, NewSeenRefsAcc} =
                resolve_duplicated_refs(FT, SeenRefsAcc),
              {Field#avro_record_field{type = NewFT}, NewSeenRefsAcc}
          end, SeenRefs1, Fields0),
      {R#avro_record_type{fields = Fields}, SeenRefs}
  end.

%% @private
-spec flatten(avro_type()) -> {avro_type(), [avro_type()]}.
flatten(#avro_primitive_type{} = Primitive) ->
  {Primitive, []};
flatten(#avro_enum_type{} = Enum) ->
  {Enum, []};
flatten(#avro_fixed_type{} = Fixed) ->
  {Fixed, []};
flatten(#avro_record_type{} = Record) ->
  {NewFields, ExtractedTypes} =
    lists:foldr(
      fun(Field, {FieldsAcc, ExtractedAcc}) ->
          {NewType, Extracted} = flatten_type(Field#avro_record_field.type),
          {[Field#avro_record_field{type = NewType}|FieldsAcc],
           Extracted ++ ExtractedAcc}
      end,
      {[], []},
      Record#avro_record_type.fields),
  {Record#avro_record_type{fields = NewFields}, ExtractedTypes};
flatten(#avro_array_type{} = Array) ->
  ChildType = avro_array:get_items_type(Array),
  {NewChildType, Extracted} = flatten_type(ChildType),
  {avro_array:type(NewChildType), Extracted};
flatten(#avro_map_type{type = ChildType} = Map) ->
  {NewChildType, Extracted} = flatten_type(ChildType),
  {Map#avro_map_type{type = NewChildType}, Extracted};
flatten(#avro_union_type{} = Union) ->
  ChildrenTypes = avro_union:get_types(Union),
  {NewChildren, ExtractedTypes} =
    lists:foldr(
      fun(ChildType, {FlattenAcc, ExtractedAcc}) ->
          {ChildType1, Extracted} = flatten_type(ChildType),
          {[ChildType1 | FlattenAcc], Extracted ++ ExtractedAcc}
      end,
      {[], []},
      ChildrenTypes),
  {avro_union:type(NewChildren), ExtractedTypes}.

%% @private
-spec do_expand_type(fullname(), lkup(), compact | bloated, [fullname()]) ->
        fullname() | avro_type() | no_return().
do_expand_type(Fullname, Lkup, Style, Stack) when ?IS_NAME(Fullname) ->
  Hist = erlang:get(?SEEN),
  IsSeenBefore =
    case Style of
      compact -> lists:member(Fullname, Hist);
      bloated -> lists:member(Fullname, Stack)
    end,
  case IsSeenBefore of
    true ->
      %% This type name has been resolved earlier
      %% do not go deeper for 2 reasons:
      %% 1. To avoid stack overflow in case of recursive reference
      %% 2. Should not duplicate the type definitions for 'compact' style
      Fullname;
    false ->
      T = Lkup(Fullname),
      erlang:put(?SEEN, [Fullname | Hist]),
      expand(T, Lkup, Style, [Fullname | Stack])
  end.

%% @private
-spec expand(avro_type(), lkup(), compact | bloated, [fullname()]) ->
        avro_type() | no_return().
expand(#avro_record_type{fields = Fields} = T, Lkup, Style, Stack) ->
  ResolvedFields =
    lists:map(
      fun(#avro_record_field{type = Type} = F) ->
        ResolvedType = expand(Type, Lkup, Style, Stack),
        F#avro_record_field{type = ResolvedType}
      end, Fields),
  T#avro_record_type{fields = ResolvedFields};
expand(#avro_array_type{type = SubType} = T, Lkup, Style, Stack) ->
  ResolvedSubType = expand(SubType, Lkup, Style, Stack),
  T#avro_array_type{type = ResolvedSubType};
expand(#avro_map_type{type = SubType} = T, Lkup, Style, Stack) ->
  ResolvedSubType = expand(SubType, Lkup, Style, Stack),
  T#avro_map_type{type = ResolvedSubType};
expand(#avro_union_type{} = T, Lkup, Style, Stack) ->
  SubTypes = avro_union:get_types(T),
  ResolvedSubTypes =
    lists:map(
      fun(SubType) ->
          expand(SubType, Lkup, Style, Stack)
      end, SubTypes),
  avro_union:type(ResolvedSubTypes);
expand(Fullname, Lkup, Style, Stack) when ?IS_NAME(Fullname) ->
  do_expand_type(Fullname, Lkup, Style, Stack);
expand(T, _Lkup, _Style, _Stack) when ?IS_TYPE_RECORD(T) ->
  T.

%% @private Ensure string() format name for validation.
-spec name_string(name_raw()) -> string().
name_string(Name) when is_binary(Name) ->
  %% No need of utf8 validation since names are all ascii
  binary_to_list(Name);
name_string(Name) when is_atom(Name) ->
  atom_to_list(Name);
name_string(Name) when is_list(Name) ->
  Name.

%% @private Check validity of the name portion of type names,
%% record field names and enums symbols (everything where dots should not
%% present).
%% @end
-spec is_valid_name(string() | string()) -> boolean().
is_valid_name([]) -> false;
is_valid_name([H | T]) ->
  is_valid_name_head(H) andalso
  lists:all(fun(I) -> is_valid_name_char(I) end, T).

%% @private Check validity of type name or namespace.
%% where name parts can be splitted with dots.
%% @end
-spec is_valid_dotted_name(name_raw()) -> boolean().
is_valid_dotted_name(DottedName) when ?IS_NAME_RAW(DottedName) ->
  NameStr = binary_to_list(?NAME(DottedName)),
  Names = tokens_ex(NameStr, $.),
  Names =/= [] andalso lists:all(fun is_valid_name/1, Names).

%% @private
-spec reserved_type_names() -> [name()].
reserved_type_names() ->
  [?AVRO_NULL, ?AVRO_BOOLEAN, ?AVRO_INT, ?AVRO_LONG, ?AVRO_FLOAT,
   ?AVRO_DOUBLE, ?AVRO_BYTES, ?AVRO_STRING, ?AVRO_RECORD, ?AVRO_ENUM,
   ?AVRO_ARRAY, ?AVRO_MAP, ?AVRO_UNION, ?AVRO_FIXED].

%% @private A valid leading char of a name: [a-z,A-Z,_].
-spec is_valid_name_head(byte()) -> boolean().
is_valid_name_head(S) ->
  (S >= $A andalso S =< $Z) orelse
  (S >= $a andalso S =< $z) orelse
  S =:= $_.

%% @private In addition to what applies to leading char, body char can be 0-9.
is_valid_name_char(S) -> is_valid_name_head(S) orelse
                         (S >= $0 andalso S =< $9).

%% @private
verify_type_name(Type) ->
  Name = avro:get_type_name(Type),
  Ns = avro:get_type_namespace(Type),
  Fullname = avro:get_type_fullname(Type),
  ?ERROR_IF_NOT(is_valid_name(name_string(Name)),
                {invalid_name, Name}),
  %% Verify namespace only if it is non-empty (empty namespaces are allowed)
  Ns =:= ?NS_GLOBAL orelse verify_dotted_name(Ns),
  ok = verify_dotted_name(Fullname),
  %% We are not interested in the namespace here, so we can ignore
  %% EnclosingExtension value.
  {CanonicalName, _} = avro:split_type_name(Name, Ns),
  ReservedNames = reserved_type_names(),
  ?ERROR_IF(lists:member(CanonicalName, ReservedNames),
            {reserved, Name, CanonicalName}).

%% @private Splits string to tokens but doesn't count consecutive delimiters as
%% a single delimiter. So tokens_ex("a...b", $.) produces ["a","","","b"].
%% @end
-spec tokens_ex(string(), byte()) -> [string()].
tokens_ex([], _Delimiter) ->
  [""];
tokens_ex([Delimiter|Rest], Delimiter) ->
  [[]|tokens_ex(Rest, Delimiter)];
tokens_ex([C|Rest], Delimiter) ->
  [Token|Tail] = tokens_ex(Rest, Delimiter),
  [[C|Token]|Tail].

%% @private Best-effort of arrary recognition.
is_array([H | _]) -> not is_integer(H); %% a string
is_array(_)       -> false.

%% @private
validate(T, AllowBadRefs, AllowTypeRedef) ->
  Dummy = fun(_T, _Hist) -> ok end,
  F1 = case AllowBadRefs of
         true -> Dummy;
         false -> fun validate_bad_ref/2
       end,
  F2 = case AllowTypeRedef of
         true -> Dummy;
         false -> fun validate_redef/2
       end,
  F = fun(Type, Hist) ->
          ok = F1(Type, Hist),
          ok = F2(Type, Hist),
          case ?IS_NAME(Type) of
            true  -> [{Type, ref} | Hist];
            false -> [{Name, def} || Name <- names(Type)] ++ Hist
          end
      end,
  ok = do_validate(T, F).

-spec do_validate(avro_type(), validate_fun()) -> ok.
do_validate(Type, ValidateFun) ->
  _ = do_validate(Type, ValidateFun, _Hist = []),
  ok.

%% @private
-spec do_validate(type_or_name(), validate_fun(), validate_hist()) ->
        validate_hist().
do_validate(Type, ValidateFun, Hist) ->
  NewHist = ValidateFun(Type, Hist),
  lists:foldl(
    fun(ST, HistIn) ->
        do_validate(ST, ValidateFun, HistIn)
    end, NewHist, sub_types(Type)).

%% @private
-spec validate_bad_ref(avro:type_or_name(), validate_hist()) -> ok.
validate_bad_ref(N, Hist) when ?IS_NAME(N) ->
  case lists:any(fun({Name, _}) -> N =:= Name end, Hist) of
    true -> ok;
    false -> erlang:throw({ref_to_unknown_type, N})
  end;
validate_bad_ref(_T, _Hist) ->
  ok.

%% @private
-spec validate_redef(avro:type_or_name(), validate_hist()) -> ok.
validate_redef(T, Hist) ->
  Names = names(T),
  lists:foreach(
    fun(Name) ->
        case [N || {N, def} <- Hist, N =:= Name] of
          [] -> ok;
          _ -> erlang:throw({type_redefined, Name})
               end
    end, Names).

%% @private
-spec names(avro_type()) -> [avro:fullname()].
names(#avro_enum_type{fullname = FN, aliases = Aliases}) -> [FN | Aliases];
names(#avro_fixed_type{fullname = FN, aliases = Aliases}) -> [FN | Aliases];
names(#avro_record_type{fullname = FN, aliases = Aliases}) -> [FN | Aliases];
names(_) -> [].

%% @private
-spec sub_types(avro_type()) -> [avro_type()].
sub_types(T) when ?IS_RECORD_TYPE(T) ->
  [ST || {_N, ST} <- avro_record:get_all_field_types(T)];
sub_types(T) when ?IS_UNION_TYPE(T) -> avro_union:get_types(T);
sub_types(T) when ?IS_MAP_TYPE(T) -> [avro_map:get_items_type(T)];
sub_types(T) when ?IS_ARRAY_TYPE(T) -> [avro_array:get_items_type(T)];
sub_types(_) -> [].


promotable(Reader, Writer) when ?IS_INT_TYPE(Writer) ->
   ?IS_LONG_TYPE(Reader) orelse ?IS_FLOAT_TYPE(Reader) orelse
    ?IS_DOUBLE_TYPE(Reader);
promotable(Reader, Writer) when ?IS_LONG_TYPE(Writer) ->
  ?IS_FLOAT_TYPE(Reader) orelse ?IS_DOUBLE_TYPE(Reader);
promotable(Reader, Writer) when ?IS_FLOAT_TYPE(Writer) ->
  ?IS_DOUBLE_TYPE(Reader);
promotable(Reader, Writer) when ?IS_STRING_TYPE(Writer) ->
  ?IS_BYTES_TYPE(Reader);
promotable(Reader, Writer) when ?IS_BYTES_TYPE(Writer) ->
  ?IS_STRING_TYPE(Reader);
promotable(_, _) ->
  false.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
