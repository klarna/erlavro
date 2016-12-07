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

-define(IS_AVRO_TYPE(T), is_tuple(T)).
-define(IS_NAME(N), (is_list(N))).
-define(NAMESPACE_NONE, "").

-type name() :: string().
-type namespace() :: string().
-type fullname() :: string().
-type typedoc() :: string().
-type enum_symbol() :: string().
-type union_index() :: non_neg_integer().
-type lkup_fun() :: fun((string()) -> avro_type()).
-type schema_store() :: avro_schema_store:store().

-record(header, { magic
  , meta
  , sync
}).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
