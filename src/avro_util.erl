%%%-----------------------------------------------------------------------------
%%%
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
%%% @doc Collections of Avro utility functions shared between other modules.
%%% Should not be used externally.
%%% @end
%%%-----------------------------------------------------------------------------

-module(avro_util).

%% API
-export([ canonicalize_aliases/4
        , canonicalize_name/1
        , canonicalize_type_or_name/1
        , ensure_binary/1
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

%%%_* APIs =====================================================================

-spec get_opt(Key, [{Key, Value}]) -> Value | no_return()
        when Key :: atom() | binary(), Value :: term().
get_opt(Key, Opts) ->
  case lists:keyfind(Key, 1, Opts) of
    {Key, Value} -> Value;
    false        -> erlang:error({not_found, Key})
  end.

-spec get_opt(Key, [{Key, Value}], Value) -> Value
        when Key :: atom() | binary(), Value :: term().
get_opt(Key, Opts, Default) ->
  case lists:keyfind(Key, 1, Opts) of
    {Key, Value} -> Value;
    false        -> Default
  end.


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
canonicalize_aliases(Aliases, Name, Namespace, EnclosingNs) ->
  lists:map(
    fun(Alias) ->
        {_, ProperNs} = avro:split_type_name(Name, Namespace, EnclosingNs),
        avro:build_type_fullname(Alias, ProperNs, EnclosingNs)
    end,
    Aliases).

%% @doc Convert atom() | string() to binary().
%% NOTE: Avro names are ascii only, there is no need for special utf8.
%%       handling when converting string() to binary().
%% @end
-spec canonicalize_name(name_raw()) -> name().
canonicalize_name(Name) -> ensure_binary(Name).

%% @doc Covert atom() | string() binary().
-spec ensure_binary(atom() | iolist()) -> binary().
ensure_binary(A) when is_atom(A)   -> atom_to_binary(A, utf8);
ensure_binary(L) when is_list(L)   -> iolist_to_binary(L);
ensure_binary(B) when is_binary(B) -> B.

%% @doc Canonicalize name if it is not a type.
-spec canonicalize_type_or_name(avro_type_or_name()) -> avro_type_or_name().
canonicalize_type_or_name(Name) when ?IS_NAME_RAW(Name) ->
  canonicalize_name(Name);
canonicalize_type_or_name(Type) when ?IS_AVRO_TYPE(Type) ->
  Type.

%%%_* Internal functions =======================================================

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
  ok = verify_dotted_name(Name),
  %% Verify namespace only if it is non-empty (empty namespaces are allowed)
  Ns =:= ?NS_GLOBAL orelse verify_dotted_name(Ns),
  ok = verify_dotted_name(Fullname),
  %% We are not interested in the namespace here, so we can ignore
  %% EnclosingExtension value.
  {CanonicalName, _} = avro:split_type_name(Name, Ns, ?NS_GLOBAL),
  ReservedNames = reserved_type_names(),
  ?ERROR_IF(lists:member(CanonicalName, ReservedNames),
            reserved_name_is_used_for_type_name).

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

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
