%%% @doc APIs to work with Avro IDL format
%%%
%%% This module allows to convert .avdl format to .avpr and .avsc as well
%%% as create Avro encoders and decoders.
%%% @end
%%% @reference See [https://avro.apache.org/docs/current/idl.html]
%%% @author Sergey Prokhorov <me@seriyps.ru>
-module(avro_idl).

-export([decode_schema/2]).
-export([new_context/1,
         str_to_avpr/2,
         protocol_to_avpr/2,
         typedecl_to_avsc/2]).
-include("idl.hrl").
-include("erlavro.hrl").

-record(st, {cwd}).

decode_schema(SchemaStr, Cwd) ->
    Protocol = str_to_avpr(SchemaStr, Cwd),
    #{<<"types">> := Types0} = Protocol,
    Types1 = lists:filter(
              fun(#{<<"type">> := TName}) ->TName =/= <<"error">> end, Types0),
    Ns = maps:get(<<"namespace">>, Protocol, ?AVRO_NS_GLOBAL),
    Types = lists:map(
        fun(T) ->
                case maps:is_key(<<"namespace">>, T) of
                    false ->
                        T#{<<"namespace">> => Ns};
                    true ->
                        T
                end
        end, Types1),
    avro:decode_schema(Types, [{ignore_bad_default_values, true}]).

new_context(Cwd) ->
    #st{cwd = Cwd}.

str_to_avpr(String, Cwd) ->
    str_to_avpr(String, Cwd, [drop_comments, trim_doc]).

str_to_avpr(String, Cwd, Opts) ->
    {ok, T0, _} =  avro_idl_lexer:string(String),
    T = avro_idl_lexer:preprocess(T0, Opts),
    {ok, Tree} = avro_idl_parser:parse(T),
    protocol_to_avpr(Tree, new_context(Cwd)).

protocol_to_avpr(#protocol{name = Name,
                           meta = Meta,
                           definitions = Defs0}, St) ->
    Defs = process_imports(Defs0, St),
    {Types, Messages} =
        lists:partition(fun(#function{}) -> false;
                           (_) -> true
                        end, Defs),
    Protocol0 =
        #{<<"protocol">> => b(Name),
          <<"types">> =>
              lists:map(
                fun(Type) ->
                        typedecl_to_avsc(Type, St)
                end, Types),
          <<"messages">> =>
              lists:map(
                fun(Message) ->
                        message_to_avsc(Message, St)
                end, Messages)
         },
    meta(Protocol0, Meta).

process_imports(Defs, _St) ->
    %% TODO
    %% https://avro.apache.org/docs/1.9.2/spec.html#names
    %% when importing definitions from avdl or avpr, copy namespaces from
    %% protocol to definitions, if not specified
    lists:filter(fun({import, _, _}) -> false;
                    (_) -> true
                 end, Defs).

typedecl_to_avsc(#enum{name = Name, meta = Meta, variants = Vars}, _St) ->
    meta(
      #{<<"type">> => ?AVRO_ENUM,
        <<"name">> => b(Name),
        <<"symbols">> => lists:map(fun b/1, Vars)
       },
      Meta);
typedecl_to_avsc(#fixed{name = Name, meta = Meta, size = Size}, _St) ->
    meta(
      #{<<"type">> => ?AVRO_FIXED,
        <<"name">> => b(Name),
        <<"size">> => Size},
      Meta);
typedecl_to_avsc(#error{name = Name, meta = Meta, fields = Fields}, St) ->
    meta(
      #{<<"type">> => ?AVRO_ERROR,
        <<"name">> => b(Name),
        <<"fields">> => [field_to_avsc(Field, St) || Field <- Fields]},
      Meta);
typedecl_to_avsc(#record{name = Name, meta = Meta, fields = Fields}, St) ->
    meta(
      #{<<"type">> => ?AVRO_RECORD,
        <<"name">> => b(Name),
        <<"fields">> => [field_to_avsc(Field, St) || Field <- Fields]},
      Meta).

field_to_avsc(#field{name = Name, meta = Meta,
                     type = Type, default = Default}, St) ->
    meta(
      default(
        #{<<"name">> => b(Name),
          <<"type">> => type_to_avsc(Type, St)},
        Default),         % TODO: maybe validate default matches type
      Meta).

message_to_avsc(#function{name = Name, meta = Meta,
                          arguments = Args, return = Return,
                          extra = Extra}, St) ->
    %% TODO: arguments can just reuse `#field{}`
    ArgsSchema =
        [default(
           #{<<"name">> => b(ArgName),
             <<"type">> => type_to_avsc(Type, St)},
           Default)
         || {arg, ArgName, Type, Default} <- Args],
    Schema0 =
        #{<<"name">> => b(Name),
          <<"request">> => ArgsSchema,
          <<"response">> => type_to_avsc(Return, St)},
    Schema1 = case Extra of
                  undefined -> Schema0;
                  oneway ->
                      Schema0#{<<"one-way">> => true};
                  {throws, ThrowsTypes} ->
                      Schema0#{<<"error">> => lists:map(fun b/1, ThrowsTypes)}
              end,
    meta(Schema1, Meta).


type_to_avsc(void, _St) ->
    ?AVRO_NULL;
type_to_avsc(null, _St) ->
    ?AVRO_NULL;
type_to_avsc(T, _St) when T == int;
                          T == long;
                          T == string;
                          T == boolean;
                          T == float;
                          T == double;
                          T == bytes ->
    atom_to_binary(T, utf8);
type_to_avsc({decimal, Precision, Scale}, _St) ->
    #{<<"type">> => ?AVRO_BYTES,
      <<"logicalType">> => <<"decimal">>,
      <<"precision">> => Precision,
      <<"scale">> => Scale};
type_to_avsc(date, _St) ->
    #{<<"type">> => ?AVRO_INT,
      <<"logicalType">> => <<"date">>};
type_to_avsc(time_ms, _St) ->
    #{<<"type">> => ?AVRO_INT,
      <<"logicalType">> => <<"time-millis">>};
type_to_avsc(timestamp_ms, _St) ->
    #{<<"type">> => ?AVRO_LONG,
      <<"logicalType">> => <<"timestamp-millis">>};
type_to_avsc({custom, Id}, _St) ->
    b(Id);
type_to_avsc({union, Types}, St) ->
    [type_to_avsc(Type, St) || Type <- Types];
type_to_avsc({array, Of}, St) ->
    #{<<"type">> => ?AVRO_ARRAY,
      <<"items">> => type_to_avsc(Of, St)};
type_to_avsc({map, ValType}, St) ->
    #{<<"type">> => ?AVRO_MAP,
      <<"values">> => type_to_avsc(ValType, St)}.

meta(Schema, Meta) ->
    {Docs, Annotations} =
        lists:partition(
          fun({doc, _}) -> true;
             (#annotation{}) -> false
          end, Meta),
    Schema1 = case Docs of
                  [] -> Schema;
                  _ ->
                      DocStrings = [S || {doc, S} <- Docs],
                      Schema#{<<"doc">> => b(lists:join(
                                               "\n", DocStrings))}
              end,
    lists:foldl(
     fun(#annotation{name = Name, value = Value}, Schema2) ->
             BName = b(Name),
             BVal = case Value of
                        [] -> <<>>;
                        [C | _] when is_integer(C) -> b(Value);
                        _ ->
                            [b(Str) || Str <- Value]
                    end,
             maps:is_key(BName, Schema2) andalso
                 error({duplicate_annotation, Name, Value, Schema2}),
             Schema2#{BName => BVal}
     end, Schema1, Annotations).

default(Obj, undefined) ->
    Obj;
default(Obj, Default) ->
    Obj#{<<"default">> => Default}.

b(Str) when is_list(Str) ->
    unicode:characters_to_binary(Str).
