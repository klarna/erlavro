-module(avro_idl).

-export([new_context/1,
         str_to_avpr/2,
         protocol_to_avpr/2,
         typedecl_to_avsc/2]).
-include("idl.hrl").

-record(st, {cwd}).

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
        #{protocol => Name,
          types =>
              lists:map(
                fun(Type) ->
                        typedecl_to_avsc(Type, St)
                end, Types),
          messages =>
              lists:map(
                fun(Message) ->
                        message_to_avsc(Message, St)
                end, Messages)
         },
    meta(Protocol0, Meta).

process_imports(Defs, _St) ->
    %% TODO
    lists:filter(fun({import, _, _}) -> false;
                    (_) -> true
                 end, Defs).

typedecl_to_avsc(#enum{name = Name, meta = Meta, variants = Vars}, _St) ->
    meta(
      #{type => enum,
        name => Name,
        variants => Vars
       },
      Meta);
typedecl_to_avsc(#fixed{name = Name, meta = Meta, size = Size}, _St) ->
    meta(
      #{type => fixed,
        name => Name,
        size => Size},
      Meta);
typedecl_to_avsc(#error{name = Name, meta = Meta, fields = Fields}, St) ->
    meta(
      #{type => error,
        name => Name,
        fields => [field_to_avsc(Field, St) || Field <- Fields]},
      Meta);
typedecl_to_avsc(#record{name = Name, meta = Meta, fields = Fields}, St) ->
    meta(
      #{type => record,
        name => Name,
        fields => [field_to_avsc(Field, St) || Field <- Fields]},
      Meta).

field_to_avsc(#field{name = Name, meta = Meta,
                     type = Type, default = Default}, St) ->
    meta(
      default(
        #{name => Name,
          type => type_to_avsc(Type, St)},
        Default),         % TODO: maybe validate default matches type
      Meta).

message_to_avsc(#function{name = Name, meta = Meta,
                          arguments = Args, return = Return,
                          extra = Extra}, St) ->
    %% TODO: arguments can just reuse `#field{}`
    ArgsSchema =
        [default(
           #{name => ArgName,
             type => type_to_avsc(Type, St)},
           Default)
         || {arg, ArgName, Type, Default} <- Args],
    Schema0 =
        #{name => Name,
          request => ArgsSchema,
          response => type_to_avsc(Return, St)},
    Schema1 = case Extra of
                  undefined -> Schema0;
                  oneway ->
                      Schema0#{'one-way' => true};
                  {throws, ThrowsTypes} ->
                      %% Throws = [type_to_avsc(TType, St)
                      %%           || TType <- ThrowsTypes],
                      Schema0#{error => ThrowsTypes}
              end,
    meta(Schema1, Meta).


type_to_avsc(void, _St) ->
    null;
type_to_avsc(null, _St) ->
    null;
type_to_avsc(T, _St) when T == int;
                          T == long;
                          T == string;
                          T == boolean;
                          T == float;
                          T == double;
                          T == bytes ->
    T;
type_to_avsc({decimal, Precision, Scale}, _St) ->
    #{type => bytes,
      'logicalType' => "decimal",
      precision => Precision,
      scale => Scale};
type_to_avsc(date, _St) ->
    #{type => int,
      'logicalType' => "date"};
type_to_avsc(time_ms, _St) ->
    #{type => int,
      'logicalType' => "time-millis"};
type_to_avsc(timestamp_ms, _St) ->
    #{type => long,
      'logicalType' => "timestamp-millis"};
type_to_avsc({custom, Id}, _St) ->
    Id;
type_to_avsc({union, Types}, St) ->
    [type_to_avsc(Type, St) || Type <- Types];
type_to_avsc({array, Of}, St) ->
    #{type => array,
      items => type_to_avsc(Of, St)};
type_to_avsc({map, ValType}, St) ->
    #{type => map,
      values => type_to_avsc(ValType, St)}.

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
                      Schema#{"doc" => lists:flatten(lists:join(
                                                       "\n", DocStrings))}
              end,
    lists:foldl(
     fun(#annotation{name = Name, value = Value}, Schema2) ->
             maps:is_key(Name, Schema2) andalso
                 error({duplicate_annotation, Name, Value, Schema2}),
             Schema2#{Name => Value}
     end, Schema1, Annotations).

default(Obj, undefined) ->
    Obj;
default(Obj, Default) ->
    Obj#{default => Default}.
