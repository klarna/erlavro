%%%-----------------------------------------------------------------------------
%%%
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
        , ensure_binary/1
        , expand_type/2
        , flatten_type/1
        , get_opt/2
        , get_opt/3
        , verify_names/1
        , verify_dotted_name/1
        , verify_aliases/1
        , verify_type/1
        ]).

-ifdef(TEST).
-export([ is_valid_dotted_name/1
        , is_valid_name/1
        , tokens_ex/2
        ]).
-endif.

-include("avro_internal.hrl").

-type store() :: avro_schema_store:store().
-define(SEEN, avro_get_nested_type_seen_full_names).

%%%_* APIs =====================================================================

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
-spec expand_type(type_or_name(), store()) -> avro_type() | no_return().
expand_type(Type0, Store) ->
  Type = avro_util:canonicalize_type_or_name(Type0),
  try
    %% Use process dictionary to keep the history of seen type names
    %% to simplify (comparing to a lists:foldr version) the loop functions.
    %%
    %% The ?SEEN names are 'previously' already resolved full names of types
    %% when traversing the type tree, so there is no need to resolve
    %% the 'current' type reference again.
    %%
    %% Otherwise the encoded JSON schema would be bloated with repeated
    %% type definitions.
    _ = erlang:put(?SEEN, []),
    case ?IS_NAME(Type) of
      true  -> do_expand_type(Type, Store);
      false -> expand(Type, Store)
    end
  after
    erlang:erase(?SEEN)
  end.

%%%_* Internal functions =======================================================

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
-spec do_expand_type(fullname(), store()) ->
        fullname() | avro_type() | no_return().
do_expand_type(Fullname, Store) when ?IS_NAME(Fullname) ->
  Hist = erlang:get(?SEEN),
  case lists:member(Fullname, Hist) of
    true ->
      %% This type name has been resolved earlier
      %% do not go deeper for 2 reasons:
      %% 1. There is no need to duplicate the type definitions
      %% 2. To avoid stack overflow in case of recursive reference
      Fullname;
    false ->
      {ok, T} = avro_schema_store:lookup_type(Fullname, Store),
      erlang:put(?SEEN, [Fullname | Hist]),
      expand(T, Store)
  end.

%% @private
-spec expand(avro_type(), store()) -> avro_type() | no_return().
expand(#avro_record_type{fields = Fields} = T, Store) ->
  ResolvedFields =
    lists:map(
      fun(#avro_record_field{type = Type} = F) ->
        ResolvedType = expand(Type, Store),
        F#avro_record_field{type = ResolvedType}
      end, Fields),
  T#avro_record_type{fields = ResolvedFields};
expand(#avro_array_type{type = SubType} = T, Store) ->
  ResolvedSubType = expand(SubType, Store),
  T#avro_array_type{type = ResolvedSubType};
expand(#avro_map_type{type = SubType} = T, Store) ->
  ResolvedSubType = expand(SubType, Store),
  T#avro_map_type{type = ResolvedSubType};
expand(#avro_union_type{} = T, Store) ->
  SubTypes = avro_union:get_types(T),
  ResolvedSubTypes =
    lists:map(fun(SubType) -> expand(SubType, Store) end, SubTypes),
  avro_union:type(ResolvedSubTypes);
expand(Fullname, Store) when ?IS_NAME(Fullname) ->
  do_expand_type(Fullname, Store);
expand(T, _Store) when ?IS_TYPE_RECORD(T) ->
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

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
