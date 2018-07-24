%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2018 Klarna AB
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

%% erlang:get_stacktrace/1 is deprecated by http://erlang.org/eeps/eep-0047.md
%% This file provides macros to support source code compaibility with old and
%% new ways of doing things.

-ifndef(OTP_RELEASE).
-define(CAPTURE_STACKTRACE, ).
-define(GET_STACKTRACE, erlang:get_stacktrace()).
-else.
-define(CAPTURE_STACKTRACE, :MacroStackTrace).
-define(GET_STACKTRACE, MacroStackTrace).
-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
