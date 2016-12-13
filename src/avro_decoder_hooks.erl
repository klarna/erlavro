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

-export([ binary_decoder_debug_trace/2
        , tag_unions_fun/0
        , pretty_print_hist/0
        ]).

-include("avro_internal.hrl").

-type count() :: non_neg_integer().

%% @doc By default, decoders do not tag union values.
%% This hook function is to tag union values with union type names
%% NOTE: null values are not tagged
%% @end
-spec tag_unions_fun() -> decoder_hook_fun().
tag_unions_fun() -> fun tag_unions/4.

%% @doc This hook is useful when a binary decoder failed on decoding
%% a binary blob, try to decode it again with this hook to inspect
%% the decode history and the avro type stack where the failure happened
%% @end.
-spec binary_decoder_debug_trace(fun((iodata()) -> ok), count()) ->
          decoder_hook_fun().
binary_decoder_debug_trace(PrintFun, MaxHistoryLength) ->
  fun(T, Sub, Data, DecodeFun) ->
    print_trace_on_failure(T, Sub, Data, DecodeFun, PrintFun, MaxHistoryLength)
  end.

%% @doc Return a function to be used as the decoder hook.
%% The hook prints the type tree with indentation, and the leaf values
%% to whichever 'user' io device is directed.
%% @end
-spec pretty_print_hist() -> decoder_hook_fun().
pretty_print_hist() ->
  fun(T, SubInfo, Data, DecodeFun) ->
    Name = avro:get_type_fullname(T),
    Indentation =
      case get(avro_decoder_pp_indentation) of
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
    _ = put(avro_decoder_pp_indentation, Indentation + 1),
    DecodeResult = DecodeFun(Data),
    ResultToPrint =
      case DecodeResult of
        {Result, Tail} when is_binary(Tail) ->
          %% binary decode result
          Result;
        JsonDecodeResult ->
          case ?IS_AVRO_VALUE(JsonDecodeResult) of
            true  -> ?AVRO_VALUE_DATA(JsonDecodeResult);
            false -> JsonDecodeResult
          end
      end,
    %% print empty array and empty map
    case SubInfo =/= [] andalso ResultToPrint =:= [] of
      true  -> io:format(user, "~s  []\n", [IndentationStr]);
      false -> ok
    end,
    %% print the value if it's a leaf in the type tree
    case SubInfo =:= [] of
      true  -> io:format(user, "~1000000p\n", [ResultToPrint]);
      false -> ok
    end,
    _ = put(avro_decoder_pp_indentation, Indentation),
    DecodeResult
  end.

%%%_* Internal functions =======================================================

%% @private
tag_unions(T, SubInfo, DecodeIn, DecodeFun) when ?AVRO_IS_UNION_TYPE(T) ->
  Result = DecodeFun(DecodeIn),
  Tag =
    case SubInfo of
      Id when is_integer(Id) ->
        {ok, ST} = avro_union:lookup_child_type(T, Id),
        avro:get_type_fullname(ST);
      Name when ?IS_NAME(Name) ->
        Name
    end,
    case Result of
      {Value, Tail} when is_binary(Tail) ->
        %% used as binary decoder hook
        {maybe_tag(Tag, Value), Tail};
      Value ->
        %% used as JSON decoder hook
        maybe_tag(Tag, Value)
    end;
tag_unions(_T, _SubInfo, DecodeIn, DecodeFun) ->
  %% Not a union, pass through
  DecodeFun(DecodeIn).

%% @private
%% never tag null
maybe_tag("null", Value) -> Value;
maybe_tag(Name, Value)   -> {Name, Value}.

-define(PD_DECODER_HIST, avro_decoder_hist).
-define(REASON_TAG, '$hook-raised').

%% @private
print_trace_on_failure(T, Sub, Data, DecodeFun, PrintFun, HistCount) ->
  Name = avro:get_type_fullname(T),
  ok = push_stack(Name, Sub),
  try
    {Result, Tail} = DecodeFun(Data),
    Pop = case Sub =:= [] orelse Result =:= [] of
            true  -> {value, Result}; %% non-complex type or empty array
            false -> false
          end,
    ok = pop_stack(Pop),
    {Result, Tail}
  catch C : R when not (is_tuple(R) andalso element(1, R) =:= ?REASON_TAG) ->
    Stack = erlang:get_stacktrace(),
    ok = print_trace(PrintFun, get_hist(), HistCount),
    _ = erlang:erase(?PD_DECODER_HIST),
    erlang:raise(C, {?REASON_TAG, R}, Stack)
  end.

%% @private
get_hist() ->
  case erlang:get(?PD_DECODER_HIST) of
    undefined -> [];
    S         -> S
  end.

%% @private
add_hist(NewOp) ->
  erlang:put(?PD_DECODER_HIST, [NewOp | get_hist()]),
  ok.

%% @private
push_stack(Name, Sub) -> add_hist({push, Name, Sub}).

%% @private
pop_stack({value, Value}) -> add_hist({pop, Value});
pop_stack(false)          -> add_hist(pop).

%% @private
print_trace(PrintFun, Hist, HistCount) ->
  {Stack, History} =
    build_stack(lists:reverse(Hist), _Stack = [], _History = [], HistCount),
  PrintFun(["avro type stack:\n", Stack, "\n",
            "decode history:\n", History]).

%% @private
-spec build_stack([{push, _, _} | {pop, _} | pop],
                  list(), iodata(), count()) ->
        {Stack :: iodata(), Hist:: iodata()}.
build_stack([], Stack, Hist, _HistCount) ->
  {io_lib:format("~p", [lists:reverse(Stack)]), lists:reverse(Hist)};
build_stack([{push, Name, Sub} | Rest], Stack, Hist, HistCount) ->
  Padding = lists:duplicate(length(Stack) * 2, $\s),
  Line = bin([Padding, Name,
              case Sub of
                []                   -> "";
                none                 -> "";
                I when is_integer(I) -> [".", integer_to_list(I)];
                S when is_list(S)    -> [".", S]
              end, "\n"]),
  NewHist = lists:sublist([Line | Hist], HistCount),
  build_stack(Rest, [{Name, Sub} | Stack], NewHist, HistCount);
build_stack([{pop, V} | Rest], Stack, Hist, HistCount) ->
  Padding = lists:duplicate(length(Stack) * 2, $\s),
  Line = bin([Padding, io_lib:format("~100000p", [V]), "\n"]),
  NewHist = lists:sublist([Line | Hist], HistCount),
  build_stack(Rest, tl(Stack), NewHist, HistCount);
build_stack([pop | Rest], Stack, Hist, HistCount) ->
  build_stack(Rest, tl(Stack), Hist, HistCount).

%% @private
bin(IoData) -> iolist_to_binary(IoData).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
