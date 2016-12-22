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

-ifndef(AVRO_INTERNAL_HRL).
-define(AVRO_INTERNAL_HRL, true).

-include("erlavro.hrl").

-define(IS_PRIMITIVE_NAME(N),
        (N =:= ?AVRO_NULL    orelse
         N =:= ?AVRO_BOOLEAN orelse
         N =:= ?AVRO_INT     orelse
         N =:= ?AVRO_LONG    orelse
         N =:= ?AVRO_STRING  orelse
         N =:= ?AVRO_FLOAT   orelse
         N =:= ?AVRO_DOUBLE  orelse
         N =:= ?AVRO_BYTES)).

-define(IS_AVRO_TYPE(T), is_tuple(T)).
-define(IS_NAME(N), (is_list(N))).
-define(NAMESPACE_NONE, "").

-type name() :: string().
-type namespace() :: string().
-type fullname() :: string().
-type typedoc() :: string().
-type enum_symbol() :: string().
-type union_index() :: non_neg_integer().
-type lkup_fun() :: fun((fullname()) -> avro_type()).
-type schema_store() :: avro_schema_store:store().

-record(header, { magic
  , meta
  , sync
}).

-define(AVRO_SCHEMA_LOOKUP_FUN(Store), avro_schema_store:to_lookup_fun(Store)).

-define(AVRO_ENCODED_VALUE_JSON(Type, Value),
        ?AVRO_VALUE(Type, {json, Value})).
-define(AVRO_ENCODED_VALUE_BINARY(Type, Value),
        ?AVRO_VALUE(Type, {binary, Value})).

%% Throw an exception in case the value is already encoded.
-define(ASSERT_AVRO_VALUE(VALUE),
        case VALUE of
          {json, _}   -> erlang:throw({value_already_encoded, VALUE});
          {binary, _} -> erlang:throw({value_already_encoded, VALUE});
          _           -> ok
        end).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
