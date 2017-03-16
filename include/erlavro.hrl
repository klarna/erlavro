%%%-----------------------------------------------------------------------------
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
%%%-----------------------------------------------------------------------------

%% Include for decoder hook implementation.

-ifndef(_ERLAVRO_HRL_).
-define(_ERLAVRO_HRL_, true).

%% Names of primitive types
-define(AVRO_NULL,    <<"null">>).
-define(AVRO_BOOLEAN, <<"boolean">>).
-define(AVRO_INT,     <<"int">>).
-define(AVRO_LONG,    <<"long">>).
-define(AVRO_FLOAT,   <<"float">>).
-define(AVRO_DOUBLE,  <<"double">>).
-define(AVRO_BYTES,   <<"bytes">>).
-define(AVRO_STRING,  <<"string">>).
%% Other reserved types names
-define(AVRO_RECORD,  <<"record">>).
-define(AVRO_ENUM,    <<"enum">>).
-define(AVRO_ARRAY,   <<"array">>).
-define(AVRO_MAP,     <<"map">>).
-define(AVRO_UNION,   <<"union">>).
-define(AVRO_FIXED,   <<"fixed">>).

-define(IS_AVRO_PRIMITIVE_NAME(N),
        (N =:= ?AVRO_NULL    orelse
         N =:= ?AVRO_BOOLEAN orelse
         N =:= ?AVRO_INT     orelse
         N =:= ?AVRO_LONG    orelse
         N =:= ?AVRO_STRING  orelse
         N =:= ?AVRO_FLOAT   orelse
         N =:= ?AVRO_DOUBLE  orelse
         N =:= ?AVRO_BYTES)).

-define(AVRO_REQUIRED, erlang:error({required_field_missed, ?MODULE, ?LINE})).

-define(AVRO_NS_GLOBAL, <<"">>).
-define(AVRO_NO_DOC, <<"">>).

-record(avro_primitive_type,
        { name      = ?AVRO_REQUIRED  :: avro:name()
        , custom    = []              :: [avro:custom_prop()]
        }).

-record(avro_record_type,
        { name      = ?AVRO_REQUIRED  :: avro:name()
        , namespace = ?AVRO_NS_GLOBAL :: avro:namespace()
        , doc       = ?AVRO_NO_DOC    :: avro:typedoc()
        , aliases   = []              :: [avro:name()]
        , fields    = ?AVRO_REQUIRED  :: [avro:record_field()]
        , fullname  = ?AVRO_REQUIRED  :: avro:fullname()
        , custom    = []              :: [avro:custom_prop()]
        }).

-record(avro_enum_type,
        { name      = ?AVRO_REQUIRED  :: avro:name()
        , namespace = ?AVRO_NS_GLOBAL :: avro:namespace()
        , aliases   = []              :: [avro:name()]
        , doc       = ?AVRO_NO_DOC    :: avro:typedoc()
        , symbols   = ?AVRO_REQUIRED  :: [avro:enum_symbol()]
        , fullname  = ?AVRO_REQUIRED  :: avro:fullname()
        , custom    = []              :: [avro:custom_prop()]
        }).

-record(avro_array_type,
        { type      = ?AVRO_REQUIRED  :: avro:type_or_name()
        , custom    = []              :: [avro:custom_prop()]
        }).

-record(avro_map_type,
        { type      = ?AVRO_REQUIRED  :: avro:type_or_name()
        , custom    = []              :: [avro:custom_prop()]
        }).

-record(avro_union_type,
        { id2type   = ?AVRO_REQUIRED  :: avro_union:id2type()
        , name2id   = ?AVRO_REQUIRED  :: avro_union:name2id()
        }).

-record(avro_fixed_type,
        { name      = ?AVRO_REQUIRED  :: avro:name()
        , namespace = ?AVRO_NS_GLOBAL :: avro:namespace()
        , aliases   = []              :: [avro:name()]
        , size      = ?AVRO_REQUIRED  :: pos_integer()
        , fullname  = ?AVRO_REQUIRED  :: avro:fullname()
        , custom    = []              :: [avro:custom_prop()]
        }).

-record(avro_value,
        { type :: avro:type_or_name()
        , data :: avro:avro_value()
        }).

-type avro_value() :: avro:canonicalized_value()     %% primitive, fixed, enum
                    | #avro_value{}                  %% union
                    | [#avro_value{}]                %% array
                    | [{avro:name(), #avro_value{}}] %% record
                    | avro_map:data()                %% map
                    | {json, binary()}               %% serialized
                    | {binary, binary()}.            %% serialized

-define(IS_AVRO_VALUE(Value), is_record(Value, avro_value)).
-define(AVRO_VALUE(Type,Data), #avro_value{type = Type, data = Data}).
-define(AVRO_VALUE_TYPE(Value), Value#avro_value.type).
-define(AVRO_VALUE_DATA(Value), Value#avro_value.data).

-type avro_encoding() :: avro_json | avro_binary.

-define(AVRO_ENCODED_VALUE_JSON(Type, Value),
        ?AVRO_VALUE(Type, {json, Value})).
-define(AVRO_ENCODED_VALUE_BINARY(Type, Value),
        ?AVRO_VALUE(Type, {binary, Value})).

-define(AVRO_DEFAULT_DECODER_HOOK,
        fun(__Type__, __SubNameOrId__, Data, DecodeFun) -> DecodeFun(Data) end).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
