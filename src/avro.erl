-module(avro).

-export([is_named_type/1]).
-export([get_name/1]).
-export([get_fullname/2]).
-export([verify_type/1]).

-export([]).

-include_lib("erlavro/include/erlavro.hrl").


%%%===================================================================
%%% Internal functions
%%%===================================================================

is_named_type(#avro_record_type{}) -> true;
is_named_type(#avro_enum_type{})   -> true;
is_named_type(#avro_fixed_type{})  -> true;
is_named_type(_)              -> false.

get_name(#avro_primitive_type{name = Name})              -> {Name,        ""};
get_name(#avro_record_type{name = Name, namespace = Ns}) -> {Name,        Ns};
get_name(#avro_enum_type{name = Name, namespace = Ns})   -> {Name,        Ns};
get_name(#avro_array_type{})                             -> {?AVRO_ARRAY, ""};
get_name(#avro_map_type{})                               -> {?AVRO_MAP,   ""};
get_name(#avro_union_type{})                             -> {?AVRO_UNION, ""};
get_name(#avro_fixed_type{name = Name, namespace = Ns})  -> {Name,        Ns}.

get_fullname(Type, EnclosingNamespace) ->
    {Name, Ns} = get_name(Type),
    case is_named_type(Type) of
        true  ->
            {ProperName, ProperNs} =
                canonicalize_name(Name, Ns, EnclosingNamespace),
            make_fullname(ProperName, ProperNs);
        false ->
            %% Primitive and unnamed types don't belong to any namespace
            Name
    end.

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

canonicalize_name(Name, Namespace, EnclosingNamespace) ->
    case split_fullname(Name) of
        {_, _} = N -> N;
        false ->
            ProperNs = if Namespace =:= "" -> EnclosingNamespace;
                          true             -> Namespace
                       end,
            {Name, ProperNs}
    end.

is_correct_name(Name) ->
    Re = "^([A-Za-z_][A-Za-z0-9_]*\.)*[A-Za-z_][A-Za-z0-9_]*$",
    re:run(Name, Re) =/= nomatch.

verify_type_name(Type) ->
    {Name, Ns} = get_name(Type),
    error_if_false(is_correct_name(Name),
                   {error, {invalid_name, Name}}),
    error_if_false(Ns =:= "" orelse is_correct_name(Ns),
                   {error, {invalid_name, Ns}}),
    %% It is important to call canonicalize_name after basic checks,
    %% because it assumes that all names are correct.
    %% We are not interested in the namespace here, so we can ignore
    %% EnclosingExtension value.
    {CanonicalName, _} = canonicalize_name(Name, Ns, ""),
    error_if_false(not lists:member(CanonicalName, reserved_type_names()),
                   {error, reserved_name_is_used_for_type_name}).

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
            {string:substr(FullName, 1, DotPos-1),
             string:substr(FullName, DotPos + 1)}
    end.

make_fullname(Name, Namespace) ->
    Namespace ++ "." ++ Name.

error_if_false(true, _Err) -> ok;
error_if_false(false, Err) -> erlang:error(Err).

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(EUNIT).

-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
