%%%-------------------------------------------------------------------
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
%%%-------------------------------------------------------------------
%%%
%%% @doc This module is a collection of `eravro' supported decoder hooks
%%
%% Decoder hook is an anonymous function to be evaluated by
%% the JSON or binary decoder to amend either schmea or data (input or output).
%%
%% For example:
%%
%% A hook can be used to fast-skip undesired data fields of records
%% or undesired data of big maps etc.
%% e.g. To dig out only the field named "MyField" in "MyRecord", the
%% hook may probably look like:
%%
%% <pre>
%% fun(Type, SubNameOrIndex, Data, DecodeFun) ->
%%      case {avro:get_type_fullname(Type), SubNameOrIndex} of
%%        {"MyRecord.example.com", "MyField"} ->
%%          DecodeFun(Data);
%%        {"MyRecord.example.com", _OtherFields} ->
%%          ignored;
%%        _OtherType ->
%%          DecodeFun(Data)
%%      end
%% end.
%% </pre>
%%
%% A hook can be used for debug. For example, below hook should print
%% the decoding stack along the decode function traverses through the bytes.
%%
%% <pre>
%% fun(Type, SubNameOrIndex, Data, DecodeFun) ->
%%      SubInfo = case is_integer(SubNameOrIndex) of
%%                  true  -> integer_to_list(SubNameOrIndex);
%%                  false -> SubNameOrIndex
%%                end,
%%      io:format("~s.~s\n", [avro:get_type_name(Type), SubInfo]),
%%      DecodeFun(Data)
%% end
%% </pre>
%%
%% A hook can also be used as a monkey patch to fix some corrupted data.
%%% @end
%%%-------------------------------------------------------------------
-module(avro_decoder_hooks).

-export([ tag_unions/0
        , pretty_print_hist/0
        , print_debug_trace/2
        ]).

-include("avro_internal.hrl").

-define(PD_PP_INDENTATION, '$avro_decoder_pp_indentation').
-define(PD_DECODER_HIST, '$avro_decoder_hist').
-define(REASON_TAG, '$hook-raised').

-type count() :: non_neg_integer().
-type trace_hist_entry() :: {push, _, _} | {pop, _} | pop.

%% @doc By default, decoders do not tag union values.
%% This hook function is to tag union values with union type names
%% NOTE: null values are not tagged
%% @end
-spec tag_unions() -> decoder_hook_fun().
tag_unions() -> fun tag_unions/4.

%% @doc This hook is useful when a decoder has failed on decoding,
%% try to decode it again with this hook to inspect the decode history
%% and the avro type stack where the failure happened
%% NOTE: Always call this API to retrieve the hook, never save the hook
%%       and re-use for different decode attempts
%% @end.
-spec print_debug_trace(fun((iodata()) -> ok), count()) -> decoder_hook_fun().
print_debug_trace(PrintFun, MaxHistoryLength) ->
  ok = erase_hist(),
  fun(T, Sub, Data, DecodeFun) ->
    print_trace_on_failure(T, Sub, Data, DecodeFun, PrintFun, MaxHistoryLength)
  end.

%% @doc This hook prints the type tree with indentation, and the leaf values
%% to the io-device 'user' is directed.
%% @end
-spec pretty_print_hist() -> decoder_hook_fun().
pretty_print_hist() ->
  _ = erase(?PD_PP_INDENTATION),
  fun(T, SubInfo, Data, DecodeFun) ->
    Name = avro:get_type_fullname(T),
    Indentation =
      case get(?PD_PP_INDENTATION) of
        undefined -> 0;
        Indentati -> Indentati
      end,
    IndentationStr = lists:duplicate(Indentation * 2, $\s),
    ToPrint =
      [ IndentationStr
      , Name
      , case SubInfo of
          ""                   -> ": ";
          I when is_integer(I) -> [$., integer_to_list(I), "\n"];
          S when is_list(S)    -> [$., S, "\n"];
          B when is_binary(B)  -> [$., B, "\n"];
          _                    -> "\n"
        end
      ],
    io:format(user, "~s", [ToPrint]),
    _ = put(?PD_PP_INDENTATION, Indentation + 1),
    DecodeResult = DecodeFun(Data),
    ResultToPrint = get_pretty_print_result(DecodeResult),
    _ = pretty_print_result(SubInfo, ResultToPrint, IndentationStr),
    _ = put(?PD_PP_INDENTATION, Indentation),
    DecodeResult
  end.

%%%_* Internal functions =======================================================

%% @private
tag_unions(T, SubInfo, DecodeIn, DecodeFun) when ?AVRO_IS_UNION_TYPE(T) ->
  Result = DecodeFun(DecodeIn),
  Name =
    case SubInfo of
      Id when is_integer(Id) ->
        {ok, ST} = avro_union:lookup_child_type(T, Id),
        avro:get_type_fullname(ST);
      Name_ when ?IS_NAME(Name_) ->
        Name_
    end,
    case Result of
      {Value, Tail} when is_binary(Tail) ->
        %% used as binary decoder hook
        {maybe_tag(Name, Value), Tail};
      Value ->
        %% used as JSON decoder hook
        maybe_tag(Name, Value)
    end;
tag_unions(_T, _SubInfo, DecodeIn, DecodeFun) ->
  %% Not a union, pass through
  DecodeFun(DecodeIn).

%% @private Never tag primitives and unnamed complex types.
maybe_tag(N, Value) when ?IS_PRIMITIVE_NAME(N) -> Value;
maybe_tag(?AVRO_ARRAY, Value) -> Value;
maybe_tag(?AVRO_MAP, Value) -> Value;
maybe_tag(Name, Value) -> {Name, Value}.

%% @private
print_trace_on_failure(T, Sub, Data, DecodeFun, PrintFun, HistCount) ->
  Name = avro:get_type_fullname(T),
  ok = add_hist({push, Name, Sub}),
  try
    decode_and_add_trace(Sub, Data, DecodeFun)
  catch C : R when not (is_tuple(R) andalso element(1, R) =:= ?REASON_TAG) ->
    %% catch only the very first error
    Stack = erlang:get_stacktrace(),
    ok = print_trace(PrintFun, HistCount),
    ok = erase_hist(),
    erlang:raise(C, {?REASON_TAG, R}, Stack)
  end.

%% @private
decode_and_add_trace(Sub, Data, DecodeFun) ->
  Result = DecodeFun(Data),
  Value =
    case Result of
      {V, Tail} when is_binary(Tail) ->
        %% binary decoder
        V;
      _ ->
        %% JSON decoder
        Result
    end,
  case Sub =:= [] orelse Value =:= [] of
    true  -> add_hist({pop, Value}); %% add stack hist with decoded value
    false -> add_hist(pop)
  end,
  Result.

%% @private
-spec erase_hist() -> ok.
erase_hist() ->
  _ = erlang:erase(?PD_DECODER_HIST),
  ok.

%% @private
-spec get_hist() -> ok.
get_hist() ->
  case erlang:get(?PD_DECODER_HIST) of
    undefined -> [];
    S         -> S
  end.

%% @private Use process dictionary to keep the decoder stack trace.
-spec add_hist(trace_hist_entry()) -> ok.
add_hist(NewOp) ->
  erlang:put(?PD_DECODER_HIST, [NewOp | get_hist()]),
  ok.

%% @private Print decoder trace (stack and history) using the given function.
print_trace(PrintFun, HistCount) ->
  Hist = lists:reverse(get_hist()),
  {Stack, History} = format_trace(Hist, _Stack = [], _History = [], HistCount),
  PrintFun(["avro type stack:\n", Stack, "\n",
            "decode history:\n", History]).

%% @private Format the trace hisotry into printable format.
%% Return the type stack and last N decode history entries as iodata().
%% @end
-spec format_trace(TraceHist :: [trace_hist_entry()],
                   TypeStack :: [{name(), atom() | string() | integer()}],
                   FormattedTrace :: iodata(),
                   MaxHistEntryCount :: count()) -> {iodata(), iodata()}.
format_trace([], Stack, Hist, _HistCount) ->
  {io_lib:format("~p", [lists:reverse(Stack)]), lists:reverse(Hist)};
format_trace([{push, Name, Sub} | Rest], Stack, Hist, HistCount) ->
  Padding = lists:duplicate(length(Stack) * 2, $\s),
  Line = bin([Padding, Name,
              case Sub of
                []                   -> "";
                none                 -> "";
                I when is_integer(I) -> [".", integer_to_list(I)];
                S when is_list(S)    -> [".", S]
              end, "\n"]),
  NewHist = lists:sublist([Line | Hist], HistCount),
  format_trace(Rest, [{Name, Sub} | Stack], NewHist, HistCount);
format_trace([{pop, V} | Rest], Stack, Hist, HistCount) ->
  Padding = lists:duplicate(length(Stack) * 2, $\s),
  Line = bin([Padding, io_lib:format("~100000p", [V]), "\n"]),
  NewHist = lists:sublist([Line | Hist], HistCount),
  format_trace(Rest, tl(Stack), NewHist, HistCount);
format_trace([pop | Rest], Stack, Hist, HistCount) ->
  format_trace(Rest, tl(Stack), Hist, HistCount).

%% @private
bin(IoData) -> iolist_to_binary(IoData).

%% @private
get_pretty_print_result(JsonResult) when ?IS_AVRO_VALUE(JsonResult) ->
  %% Wrapped JSON result
 ?AVRO_VALUE_DATA(JsonResult);
get_pretty_print_result({Result, Tail}) when is_binary(Tail) ->
  %% binary decode result
  Result;
get_pretty_print_result(JsonResult) ->
  %% Unwarpped JSON result
  JsonResult.

%% @private
pretty_print_result(_Sub = [], _Result = [], IndentationStr) ->
  %% print empty array and empty map
  io:format(user, "~s  []\n", [IndentationStr]);
pretty_print_result(_Sub = [], Result, _IndentationStr) ->
  %% print the value if it's a leaf in the type tree
  io:format(user, "~1000000p\n", [Result]);
pretty_print_result(_Sub, _Result, _IndentationStr) ->
  ok.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
