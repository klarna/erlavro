%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc Collections of Avro utility functions shared between other modules.
%%% Should not be used externally.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_util).

%% API
-export([get_opt/2]).
-export([get_opt/3]).
-export([error_if/2]).
-export([error_if_not/2]).
-export([verify_name/1]).
-export([verify_names/1]).
-export([verify_dotted_name/1]).
-export([verify_aliases/1]).
-export([verify_type/1]).
-export([canonicalize_aliases/4]).

-include("erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

get_opt(Key, Opts) ->
  case lists:keyfind(Key, 1, Opts) of
    {Key, Value} -> Value;
    false        -> erlang:error({error, {key_not_found, Key}})
  end.

get_opt(Key, Opts, Default) ->
  case lists:keyfind(Key, 1, Opts) of
    {Key, Value} -> Value;
    false        -> Default
  end.

error_if(true, Error)   -> erlang:error(Error);
error_if(false, _Error) -> ok.

error_if_not(Cond, Error) -> error_if(not Cond, Error).

verify_name(Name) ->
  error_if_not(is_correct_name(Name), {invalid_name, Name}).

verify_names(Names) ->
  lists:foreach(fun verify_name/1, Names).

verify_dotted_name(Name) ->
  error_if_not(is_correct_dotted_name(Name), {invalid_name, Name}).

%% Verify aliases list for correctness
verify_aliases(Aliases) ->
  lists:foreach(
    fun(Alias) ->
        verify_dotted_name(Alias)
    end,
    Aliases).

%% Verify overall type definition for correctness. Error is thrown
%% when issues are found.
-spec verify_type(avro_type()) -> ok.

verify_type(Type) ->
    case avro:is_named_type(Type) of
        true  -> verify_type_name(Type);
        false -> ok
    end.

%% Convert aliases to full-name representation using provided names and
%% namespaces from the original type
canonicalize_aliases(Aliases, Name, Namespace, EnclosingNs) ->
  lists:map(
    fun(Alias) ->
        {_, ProperNs} = avro:split_type_name(Name, Namespace, EnclosingNs),
        avro:build_type_fullname(Alias, ProperNs, EnclosingNs)
    end,
    Aliases).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Check correctness of the name portion of type names, record field names and
%% enums symbols (everything where dots should not present in).
-spec is_correct_name(string()) -> boolean().

is_correct_name([])    -> false;
is_correct_name([H])   -> is_correct_first_symbol(H);
is_correct_name([H|T]) -> is_correct_first_symbol(H) andalso
                          is_correct_name_tail(T).

%% Check correctness of type name or namespace (where name parts can be splitted
%% with dots).
-spec is_correct_dotted_name(string()) -> boolean().

is_correct_dotted_name(DottedName) ->
  Names = tokens_ex(DottedName, $.),
  Names =/= [] andalso lists:all(fun is_correct_name/1, Names).

reserved_type_names() ->
    [?AVRO_NULL, ?AVRO_BOOLEAN, ?AVRO_INT, ?AVRO_LONG, ?AVRO_FLOAT,
     ?AVRO_DOUBLE, ?AVRO_BYTES, ?AVRO_STRING, ?AVRO_RECORD, ?AVRO_ENUM,
     ?AVRO_ARRAY, ?AVRO_MAP, ?AVRO_UNION, ?AVRO_FIXED].

is_correct_first_symbol(S) -> (S >= $A andalso S =< $Z) orelse
                              (S >= $a andalso S =< $z) orelse
                              S =:= $_.

is_correct_symbol(S) -> is_correct_first_symbol(S) orelse
                        (S >= $0 andalso S =< $9).

is_correct_name_tail([])    -> true;
is_correct_name_tail([H|T]) -> is_correct_symbol(H) andalso
                               is_correct_name_tail(T).

verify_type_name(Type) ->
  Name = avro:get_type_name(Type),
  Ns = avro:get_type_namespace(Type),
  Fullname = avro:get_type_fullname(Type),
  verify_dotted_name(Name),
  %% Verify namespace only if it is non-empty (empty namespaces are allowed)
  Ns =:= "" orelse verify_dotted_name(Ns),
  verify_dotted_name(Fullname),
  %% We are not interested in the namespace here, so we can ignore
  %% EnclosingExtension value.
  {CanonicalName, _} = avro:split_type_name(Name, Ns, ""),
  error_if(lists:member(CanonicalName, reserved_type_names()),
           reserved_name_is_used_for_type_name).

%% Splits string to tokens but doesn't count consecutive delimiters as
%% a single delimiter. So tokens_ex("a...b", $.) produces ["a","","","b"].
tokens_ex([], _Delimiter) ->
  [""];
tokens_ex([Delimiter|Rest], Delimiter) ->
  [[]|tokens_ex(Rest, Delimiter)];
tokens_ex([C|Rest], Delimiter) ->
  [Token|Tail] = tokens_ex(Rest, Delimiter),
  [[C|Token]|Tail].

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

get_test_type(Name, Namespace) ->
  #avro_fixed_type{name = Name,
                   namespace = Namespace,
                   size = 16,
                   fullname = avro:build_type_fullname(Name, Namespace, "")}.

tokens_ex_test() ->
  ?assertEqual([""], tokens_ex("", $.)),
  ?assertEqual(["ab"], tokens_ex("ab", $.)),
  ?assertEqual(["", ""], tokens_ex(".", $.)),
  ?assertEqual(["", "a", "b", "c", ""], tokens_ex(".a.b.c.", $.)),
  ?assertEqual(["ab", "cd"], tokens_ex("ab.cd", $.)).

is_correct_name_test() ->
  CorrectNames = ["_", "a", "Aa1", "a_A"],
  IncorrectNames = ["", "1", " a", "a ", " a ", ".", "a.b.c"],
  [?assert(is_correct_name(Name)) || Name <- CorrectNames],
  [?assertNot(is_correct_name(Name)) || Name <- IncorrectNames].

is_correct_dotted_name_test_() ->
  CorrectNames = ["_", "a", "A._1", "a1.b2.c3"],
  IncorrectNames = ["", "1", " a.b.c", "a.b.c ", " a.b.c ", "a..b", ".a.b",
                    "a.1.b", "!", "-", "a. b.c"],
  [?_assert(is_correct_dotted_name(Name)) || Name <- CorrectNames] ++
  [?_assertNot(is_correct_dotted_name(Name)) || Name <- IncorrectNames].

verify_type_test() ->
  ?assertEqual(ok, verify_type(get_test_type("tname", "name.space"))),
  ?assertError({invalid_name, _}, verify_type(get_test_type("", ""))),
  ?assertError({invalid_name, _}, verify_type(get_test_type("", "name.space"))),
  ?assertEqual(ok, verify_type(get_test_type("tname", ""))).

canonizalize_aliases_test() ->
  %% Namespaces for aliases are taken from the original type namespace
  ?assertEqual(["name.space.Foo", "name.space.Bar"],
               canonicalize_aliases(["Foo", "Bar"],
                                    "Bee",
                                    "name.space",
                                    "enc.losing")),
  %% Aliases have their own namespaces
  ?assertEqual(["other.ns.Foo", "another.ns2.Bar"],
               canonicalize_aliases(["other.ns.Foo", "another.ns2.Bar"],
                                    "Bee",
                                    "name.space",
                                    "enc.losing")),
  %% Namespaces for aliases are taken from enclosing namespace
  ?assertEqual(["enc.losing.Foo", "enc.losing.Bar"],
               canonicalize_aliases(["Foo", "Bar"],
                                    "Bee",
                                    "",
                                    "enc.losing")),
  %% Namespaces for aliases are taken from the full type name
  ?assertEqual(["name.space.Foo", "name.space.Bar"],
               canonicalize_aliases(["Foo", "Bar"],
                                    "name.space.Bee",
                                    "bla.bla",
                                    "enc.losing")).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
