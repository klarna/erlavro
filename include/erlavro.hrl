%%%-----------------------------------------------------------------------------
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
%%%-----------------------------------------------------------------------------

-ifndef(_ERLAVRO_HRL_).
-define(_ERLAVRO_HRL_, true).

-record(avro_value,
        { type :: avro:avro_type()
        , data :: avro:canonicalize_primitive_value()
                | avro:avro_value() %% recursive
        }).

-type avro_value() :: #avro_value{}.

-define(IS_AVRO_VALUE(Value), is_record(Value, avro_value)).
-define(AVRO_VALUE(Type,Data), #avro_value{type = Type, data = Data}).
-define(AVRO_VALUE_TYPE(Value), Value#avro_value.type).
-define(AVRO_VALUE_DATA(Value), Value#avro_value.data).

-type avro_encoding() :: avro_json | avro_binary.

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
