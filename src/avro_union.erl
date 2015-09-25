%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc Implements unions support for Avro.
%%%
%%% Unions may not contain more than one schema with the same type, except
%%% for the named types record, fixed and enum. For example, unions containing
%%% two array types or two map types are not permitted, but two types with
%%% different names are permitted. (Names permit efficient resolution when
%%% reading and writing unions.)
%%%
%%% Unions may not immediately contain other unions.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_union).

%% API
-export([type/1]).
-export([get_types/1]).
-export([lookup_child_type/2]).

-export([cast/2]).
-export([to_term/1]).

-export([new/2]).
-export([get_value/1]).
-export([set_value/2]).

%% API functions which should be used only inside erlavro
-export([new_direct/2]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

type([]) ->
  erlang:error(avro_union_should_have_at_least_one_type);
type(Types) when is_list(Types) ->
  TypesDict =
    case length(Types) > 10 of
      true  -> build_types_dict(Types);
      false -> undefined
    end,
  #avro_union_type
  { types      = Types
  , types_dict = TypesDict
  }.

get_types(#avro_union_type{types = Types}) -> Types.

%% Search for a type by its full name through the children types
%% of the union
-spec lookup_child_type(#avro_union_type{}, string())
                       -> false | {ok, avro_type()}.

lookup_child_type(Union, TypeName) when ?AVRO_IS_UNION_TYPE(Union) ->
  case Union#avro_union_type.types_dict of
    undefined -> lookup_type_direct(TypeName, Union#avro_union_type.types);
    TypesDict -> lookup_type_in_dict(TypeName, TypesDict)
  end.

new(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  case cast(Type, Value) of
    {ok, Union}  -> Union;
    {error, Err} -> erlang:error(Err)
  end.

%% Special optimized version of new which assumes that Value
%% is already casted to one of the union types. Should only
%% be used inside erlavro.
new_direct(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  ?AVRO_VALUE(Type, Value).

%% Get current value of a union type variable
get_value(Union) when ?AVRO_IS_UNION_VALUE(Union) ->
  ?AVRO_VALUE_DATA(Union).

%% Sets new value to a union type variable
set_value(Union, Value) when ?AVRO_IS_UNION_VALUE(Union) ->
  ?AVRO_UPDATE_VALUE(Union, Value).

%%%===================================================================
%%% API: casting
%%%===================================================================

%% Note: in some cases casting to an union type can be ambiguous, for
%% example when it contains both string and enum types. In such cases
%% it is recommended to explicitly specify types for values, or not
%% use such combinations of types at all.

-spec cast(avro_type(), term()) -> {ok, avro_value()} | {error, term()}.

cast(Type, Value) when ?AVRO_IS_UNION_TYPE(Type) ->
  do_cast(Type, Value).

-spec to_term(avro_value()) -> term().
to_term(Union) -> avro:to_term(get_value(Union)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

build_types_dict(Types) ->
  lists:foldl(
    fun(Type, D) ->
        dict:store(get_type_fullname_ex(Type), Type, D)
    end,
    dict:new(),
    Types).

lookup_type_direct(_TypeName, []) ->
  false;
lookup_type_direct(TypeName, [Type|Rest]) ->
  CandidateTypeName = get_type_fullname_ex(Type),
  if TypeName =:= CandidateTypeName -> {ok, Type};
     true                           -> lookup_type_direct(TypeName, Rest)
  end.

%% If type is specified by its name then return this name,
%% otherwise return type's full name.
get_type_fullname_ex(TypeName) when is_list(TypeName) ->
  TypeName;
get_type_fullname_ex(Type) ->
  avro:get_type_fullname(Type).

lookup_type_in_dict(TypeName, Dict) ->
  case dict:find(TypeName, Dict) of
    {ok, _} = Res -> Res;
    error         -> false
  end.

do_cast(Type, Value) when ?AVRO_IS_UNION_VALUE(Value) ->
  %% Unions can't have other unions as their subtypes, so in this case
  %% we cast the value of the source union, not the union itself.
  do_cast(Type, ?AVRO_VALUE_DATA(Value));
do_cast(Type, Value) ->
  case cast_over_types(Type#avro_union_type.types, Value) of
    {ok, V} -> {ok, ?AVRO_VALUE(Type, V)};
    Err     -> Err
  end.

-spec cast_over_types([], _Value) -> {ok, avro_value()} | {error, term()}.

cast_over_types([], _Value) ->
  {error, type_mismatch};
cast_over_types([T|H], Value) ->
  case avro:cast(T, Value) of
    {error, _} -> cast_over_types(H, Value);
    R          -> R %% appropriate type found
  end.

%%%===================================================================
%%% Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(EUNIT).

get_record(N) ->
  Name = "R" ++ integer_to_list(N),
  avro_record:type(Name,
                   [avro_record:define_field("F", avro_primitive:int_type())],
                   [{namespace, "com.klarna.test.bix"}]).
tiny_union() ->
  type([get_record(N) || N <- lists:seq(1,5)]).

big_union() ->
  type([get_record(N) || N <- lists:seq(1,200)]).

new_direct_test() ->
  Type = type([avro_primitive:int_type(), avro_primitive:string_type()]),
  NewVersion = new(Type, "Foo"),
  DirectVersion = new_direct(Type, avro_primitive:string("Foo")),
  ?assertEqual(NewVersion, DirectVersion).

lookup_child_type_from_tiny_union_test() ->
  Type = tiny_union(),
  ExpectedRec = get_record(2),
  ?assertEqual({ok, ExpectedRec},
               lookup_child_type(Type, "com.klarna.test.bix.R2")).

lookup_child_type_from_big_union_test() ->
  Type = big_union(),
  ExpectedRec = get_record(100),
  ?assertEqual({ok, ExpectedRec},
               lookup_child_type(Type, "com.klarna.test.bix.R100")).

to_term_test() ->
  Type = type([avro_primitive:null_type(), avro_primitive:int_type()]),
  Value1 = new(Type, null),
  Value2 = new(Type, 1),
  ?assertEqual(null, avro:to_term(Value1)),
  ?assertEqual(1,    avro:to_term(Value2)).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
