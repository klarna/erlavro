%%%-------------------------------------------------------------------
%%% @author Ilya Staheev <ilya.staheev@klarna.com>
%%% @doc
%%% Writes Avro schemas and values to files using JSON format and
%%% mochijson3 as a writer.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_mochijson3_writer).

%% API
-export([write_type/2]).
-export([write_value/2]).

-include_lib("erlavro/include/erlavro.hrl").

%%%===================================================================
%%% API
%%%===================================================================

write_type(Type, Filename) ->
    Json = mochijson3:encode(encode_type(Type)),
    Bin = erlang:iolist_to_binary(Json),
    file:write_file(Filename, Bin),
    binary_to_list(Bin).

write_value(_Value, _Filename) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

optional_field(_Key, Default, Default, _MappingFun) -> [];
optional_field(Key, Value, _Default, MappingFun) -> [{Key, MappingFun(Value)}].

encode_type(Name) when is_list(Name) ->
    encode_string(Name);

encode_type(#avro_primitive_type{name = Name}) ->
    encode_string(Name);

encode_type(#avro_record_type{} = T) ->
    #avro_record_type{ name      = Name
                     , namespace = Namespace
                     , doc       = Doc
                     , aliases   = Aliases
                     , fields    = Fields
                     } = T,
    { struct
    , [ {type,   encode_string("record")}
      , {name,   encode_string(Name)}
      , {fields, lists:map(fun encode_field/1, Fields)}
      ]
      ++ optional_field(namespace, Namespace, "", fun encode_string/1)
      ++ optional_field(doc,       Doc,       "", fun encode_string/1)
      ++ optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
    };

encode_type(#avro_enum_type{} = T) ->
    #avro_enum_type{ name      = Name
                   , namespace = Namespace
                   , aliases   = Aliases
                   , doc       = Doc
                   , symbols   = Symbols} = T,
    { struct
    , [ {type,    encode_string("enum")}
      , {name,    encode_string(Name)}
      , {symbols, lists:map(fun encode_string/1, Symbols)}
      ]
      ++ optional_field(namespace, Namespace, "", fun encode_string/1)
      ++ optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
      ++ optional_field(doc,       Doc,       "", fun encode_string/1)
    };

encode_type(#avro_array_type{type = Type}) ->
    { struct
    , [ {type,  encode_string("array")}
      , {items, encode_type(Type)}
      ]
    };

encode_type(#avro_map_type{type = Type}) ->
    { struct
    , [ {type,   encode_string("map")}
      , {values, encode_type(Type)}
      ]
    };

encode_type(#avro_union_type{types = Types}) ->
    lists:map(fun encode_type/1, Types);

encode_type(#avro_fixed_type{} = T) ->
    #avro_fixed_type{ name = Name
                    , namespace = Namespace
                    , aliases = Aliases
                    , size = Size} = T,
    { struct
    , [ {type, encode_string("fixed")}
      , {name, encode_string(Name)}
      , {size, encode_integer(Size)}
      ]
      ++ optional_field(namespace, Namespace, "", fun encode_string/1)
      ++ optional_field(aliases,   Aliases,   [], fun encode_aliases/1)
    }.

encode_field(Field) ->
    #avro_field{ name    = Name
               , doc     = Doc
               , type    = Type
               , default = Default
               , order   = Order
               , aliases = Aliases} = Field,
    { struct
    , [ {name, encode_string(Name)}
      , {type, encode_type(Type)}
      ]
      ++ optional_field(default, Default, undefined, fun encode_string/1)
      ++ optional_field(doc,     Doc,     "",        fun encode_string/1)
      ++ optional_field(order,   Order,   ascending, fun encode_order/1)
      ++ optional_field(aliases, Aliases, [],        fun encode_aliases/1)
    }.

encode_string(String) ->
    erlang:list_to_binary(String).

encode_integer(Int) when is_integer(Int) ->
    Int.

encode_aliases(Aliases) ->
    lists:map(fun encode_string/1, Aliases).

encode_order(ascending)  -> <<"ascending">>;
encode_order(descending) -> <<"descending">>;
encode_order(ignore)     -> <<"ignore">>.
