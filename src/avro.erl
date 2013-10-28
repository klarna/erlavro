%%%-------------------------------------------------------------------
%%% @doc General Avro handling code.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(avro).

-export([is_named_type/1]).
-export([split_type_name/2]).
-export([get_type_fullname/2]).
-export([set_type_fullname/2]).
-export([verify_type/1]).

-export([]).

-include_lib("erlavro/include/erlavro.hrl").


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Returns true if the type can have its own name defined in schema.
-spec is_named_type(avro_type()) -> boolean().

is_named_type(#avro_record_type{}) -> true;
is_named_type(#avro_enum_type{})   -> true;
is_named_type(#avro_fixed_type{})  -> true;
is_named_type(_)                   -> false.

%% Splits type's name to its canonical short name and namespace.
%% Type should be verified before using this function.
-spec split_type_name(avro_type_or_name(), string()) -> {string(), string()}.

split_type_name(TypeName, EnclosingNamespace) when is_list(TypeName) ->
  canonicalize_name(TypeName, EnclosingNamespace, EnclosingNamespace);
split_type_name(Type, EnclosingNamespace) ->
  Name = get_type_name(Type),
  Ns = get_type_namespace(Type),
  case is_named_type(Type) of
    true  -> canonicalize_name(Name, Ns, EnclosingNamespace);
    false -> {Name, Ns}
  end.

%% Constructs the type's full name.
%% Type should be verified before using this function.
-spec get_type_fullname(avro_type_or_name(), string()) -> string().

get_type_fullname(TypeName, EnclosingNamespace) when is_list(TypeName) ->
  {Name, Ns} = split_type_name(TypeName, EnclosingNamespace),
  make_fullname(Name, Ns);
get_type_fullname(Type, EnclosingNamespace) ->
  {Name, Ns} = split_type_name(Type, EnclosingNamespace),
  case is_named_type(Type) of
    true  -> make_fullname(Name, Ns);
    false -> Name
  end.

%% Replace original type's name with its fullname, namespace is cleared.
%% If type doesn't have name, nothing is changed.
%% Main purpose of this function is to convert types to Parsing Canonical Form.
-spec set_type_fullname(avro_type(), string()) -> avro_type().

set_type_fullname(Type, EnclosingNs) ->
  FullName = get_type_fullname(Type, EnclosingNs),
  case Type of
    Record when ?AVRO_IS_RECORD_TYPE(Record) ->
      Record#avro_record_type{name = FullName, namespace = ""};
    Enum when ?AVRO_IS_ENUM_TYPE(Enum) ->
      Enum#avro_enum_type{name = FullName, namespace = ""};
    Fixed when ?AVRO_IS_FIXED_TYPE(Fixed) ->
      Fixed#avro_fixed_type{name = FullName, namespace = ""};
    Type -> Type
  end.

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

is_correct_dotted_name(Name) ->
  case split_fullname(Name) of
    false       -> is_correct_name(Name);
    {SName, Ns} -> is_correct_name(SName) andalso
                   is_correct_dotted_name(Ns)
  end.

%% Verify overall type definition for correctness. Error is thrown
%% when issues are found.
-spec verify_type(avro_type()) -> ok.

verify_type(Type) ->
    case is_named_type(Type) of
        true  -> verify_type_name(Type);
        false -> ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

reserved_type_names() ->
    [?AVRO_NULL, ?AVRO_BOOLEAN, ?AVRO_INT, ?AVRO_LONG, ?AVRO_FLOAT,
     ?AVRO_DOUBLE, ?AVRO_BYTES, ?AVRO_STRING, ?AVRO_ARRAY, ?AVRO_MAP,
     ?AVRO_UNION].

%% Returns type name field as it is set in the type
-spec get_type_name(avro_type()) -> string().

get_type_name(#avro_primitive_type{name = Name}) -> Name;
get_type_name(#avro_record_type{name = Name})    -> Name;
get_type_name(#avro_enum_type{name = Name})      -> Name;
get_type_name(#avro_array_type{})                -> ?AVRO_ARRAY;
get_type_name(#avro_map_type{})                  -> ?AVRO_MAP;
get_type_name(#avro_union_type{})                -> ?AVRO_UNION;
get_type_name(#avro_fixed_type{name = Name})     -> Name.

%% Returns type namespace field as it is set in the type
-spec get_type_namespace(avro_type()) -> string().

get_type_namespace(#avro_primitive_type{})            -> "";
get_type_namespace(#avro_record_type{namespace = Ns}) -> Ns;
get_type_namespace(#avro_enum_type{namespace = Ns})   -> Ns;
get_type_namespace(#avro_array_type{})                -> "";
get_type_namespace(#avro_map_type{})                  -> "";
get_type_namespace(#avro_union_type{})                -> "";
get_type_namespace(#avro_fixed_type{namespace = Ns})  -> Ns.

is_correct_first_symbol(S) -> (S >= $A andalso S =< $Z) orelse
                              (S >= $a andalso S =< $z) orelse
                              S =:= $_.

is_correct_symbol(S) -> is_correct_first_symbol(S) orelse
                        (S >= $0 andalso S =< $9).

is_correct_name_tail([])    -> true;
is_correct_name_tail([H|T]) -> is_correct_symbol(H) andalso
                               is_correct_name_tail(T).

verify_type_name(Type) ->
    Name = get_type_name(Type),
    Ns = get_type_namespace(Type),
    error_if_false(is_correct_dotted_name(Name),
                   {invalid_name, Name}),
    error_if_false(Ns =:= "" orelse is_correct_dotted_name(Ns),
                   {invalid_name, Ns}),
    %% It is important to call canonicalize_name after basic checks,
    %% because it assumes that all names are correct.
    %% We are not interested in the namespace here, so we can ignore
    %% EnclosingExtension value.
    {CanonicalName, _} = canonicalize_name(Name, Ns, ""),
    error_if_false(not lists:member(CanonicalName, reserved_type_names()),
                   reserved_name_is_used_for_type_name).

%% Convert name parts to their canonical short name and namespace
canonicalize_name(Name, Namespace, EnclosingNamespace) ->
    case split_fullname(Name) of
        {_, _} = N -> N;
        false ->
            ProperNs = if Namespace =:= "" -> EnclosingNamespace;
                          true             -> Namespace
                       end,
            {Name, ProperNs}
    end.

%% Splits FullName to {Name, Namespace} or returns false
%% if FullName is not a full name.
%% The function can fail if it is called on badly formatted names.
-spec split_fullname(string()) -> {string(), string()} | false.
split_fullname(FullName) ->
    case string:rchr(FullName, $.) of
        0 ->
            %% Dot not found
            false;
        DotPos ->
            { string:substr(FullName, DotPos + 1)
            , string:substr(FullName, 1, DotPos-1)
            }
    end.

make_fullname(_Name, "") ->
  %% This case is a guard that namespace is specified correctly
  erlang:error({avro_error, "Can't make full name with empty namespace"});
make_fullname(Name, Namespace) ->
  Namespace ++ "." ++ Name.

error_if_false(true, _Err) -> ok;
error_if_false(false, Err) -> erlang:error(Err).

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

get_test_type(Name, Namespace) ->
  #avro_fixed_type{name = Name, namespace = Namespace, size = 16}.

is_correct_name_test() ->
  CorrectNames = ["_", "a", "Aa1", "a_A"],
  IncorrectNames = ["", "1", " a", "a ", " a ", ".", "a.b.c"],
  [?assert(is_correct_name(Name)) || Name <- CorrectNames],
  [?assertNot(is_correct_name(Name)) || Name <- IncorrectNames].

is_correct_dotted_name_test() ->
  CorrectNames = ["_", "a", "A._1", "a1.b2.c3"],
  IncorrectNames = ["", "1", " a.b.c", "a.b.c ", " a.b.c ", "a..b", ".a.b",
                    "a.1.b", "!", "-", "a. b.c"],
  [?assert(is_correct_dotted_name(Name)) || Name <- CorrectNames],
  [?assertNot(is_correct_dotted_name(Name)) || Name <- IncorrectNames].

verify_type_test() ->
  ?assertEqual(ok, verify_type(get_test_type("tname", "name.space"))),
  ?assertError({invalid_name, _}, verify_type(get_test_type("", ""))),
  ?assertError({invalid_name, _}, verify_type(get_test_type("", "name.space"))),
  ?assertEqual(ok, verify_type(get_test_type("tname", ""))).

split_type_name_test() ->
  ?assertEqual({"int", ""},
               split_type_name(avro_primitive:int_type(),
                               "enc.losing")),
  ?assertEqual({"tname", "name.space"},
               split_type_name(get_test_type("tname", "name.space"),
                               "enc.losing")),
  ?assertEqual({"tname", "name.space"},
               split_type_name(get_test_type("name.space.tname",
                                             "name1.space1"),
                               "enc.losing")),
  ?assertEqual({"tname", "enc.losing"},
               split_type_name(get_test_type("tname", ""),
                               "enc.losing")).

get_type_fullname_test() ->
  ?assertEqual("name.space.tname",
               get_type_fullname(get_test_type("tname", "name.space"),
                                 "enc.losing")),
  ?assertEqual("int",
               get_type_fullname(avro_primitive:int_type(),
                                 "enc.losing")).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
