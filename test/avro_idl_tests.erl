%% @doc Tests for IDL converter / loader
%% @end
%% @author Sergey Prokhorov <me@seriyps.ru>
-module(avro_idl_tests).

-include("../src/idl.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("erlavro.hrl").

empty_protocol_avpr_test() ->
    ?assertEqual(
       #{<<"protocol">> => <<"MyProto">>,
         <<"types">> => [],
         <<"messages">> => []},
       idl_to_avpr("empty_protocol")).


annotations_avpr_test() ->
    Proto = idl_to_avpr("annotations"),
    ?assertEqual(
       #{<<"doc">> =>
             <<"My protocol\nNo, really\nIt's some multiline doc\n"
               "bullet points will be stripped\nso no unordered lists">>,
         <<"version">> => <<"1.0">>,
         <<"aliases">> => [<<"ns.Proto1">>, <<"ns.Proto2">>],
         <<"protocol">> => <<"MyProto">>
        },
       maps:without([<<"types">>, <<"messages">>], Proto)
      ),
    #{<<"types">> := Types,
      <<"messages">> := Messages} = Proto,
    ?assertEqual(
            [#{<<"doc">> => <<"My enum">>,
               <<"namespace">> => <<"enums">>,
               <<"type">> => ?AVRO_ENUM,
               <<"name">> => <<"MyEnum">>,
               <<"symbols">> => [<<"A">>, <<"B">>, <<"C">>]},
             #{<<"doc">> => <<"My Fixed">>,
               <<"namespace">> => <<"fixeds">>,
               <<"type">> => ?AVRO_FIXED,
               <<"name">> => <<"MyFixed">>,
               <<"size">> => 16},
             #{<<"doc">> => <<"My Error">>,
               <<"namespace">> => <<"errors">>,
               <<"type">> => ?AVRO_ERROR,
               <<"name">> => <<"MyError">>,
               <<"fields">> =>
                   [#{<<"doc">> => <<"My Err Field">>,
                      <<"order">> => <<"ignore">>,
                      <<"type">> => ?AVRO_STRING,
                      <<"name">> => <<"my_err_field">>}]},
             #{<<"doc">> => <<"My Record">>,
               <<"namespace">> => <<"records">>,
               <<"type">> => ?AVRO_RECORD,
               <<"name">> => <<"MyRecord">>,
               <<"fields">> =>
                   [#{<<"doc">> => <<"My Rec Field Type\nMy Rec Field">>,
                      <<"order">> => <<"ignore">>,
                      <<"aliases">> => [<<"my_alias">>],
                      <<"type">> => ?AVRO_STRING,
                      <<"name">> => <<"my_record_field">>}]}],
       Types),
    ?assertEqual(
       [#{<<"doc">> => <<"My Fun">>,
          <<"name">> => <<"hello">>,
          <<"request">> => [],
          <<"response">> => ?AVRO_STRING}],
       Messages).


full_protocol_avpr_test() ->
    ?assertMatch(
       #{},
      idl_to_avpr("full_protocol")).


protocol_with_typedefs_avpr_test() ->
    Proto = idl_to_avpr("protocol_with_typedefs"),
    ?assertMatch(
       #{<<"namespace">> := <<"org.erlang.www">>,
         <<"protocol">> := <<"MyProto">>,
         <<"types">> := _,
         <<"messages">> := _},
       Proto),
    #{<<"types">> := Types,
      <<"messages">> := Messages} = Proto,
    ?assertMatch(
       [#{<<"name">> := <<"MyEnum1">>},
        #{<<"name">> := <<"MyEnum2">>,
          <<"type">> := ?AVRO_ENUM,
          <<"symbols">> := [<<"VAR21">>, <<"VAR22">>, <<"VAR23">>]},
        #{<<"name">> := <<"MyFix">>,
          <<"type">> := ?AVRO_FIXED,
          <<"size">> := 10},
        #{<<"name">> := <<"MyRec">>,
          <<"fields">> :=
              [#{<<"type">> := ?AVRO_INT},
               #{<<"type">> := ?AVRO_STRING},
               #{<<"type">> := ?AVRO_FLOAT},
               #{<<"type">> := ?AVRO_BOOLEAN},
               #{<<"type">> := <<"MyFix">>},
               #{<<"type">> := [?AVRO_BOOLEAN, ?AVRO_NULL]},
               #{<<"type">> := #{<<"type">> := ?AVRO_INT,
                                 <<"logicalType">> := <<"date">>}},
               #{<<"type">> := #{<<"type">> := ?AVRO_INT,
                                 <<"logicalType">> := <<"time-millis">>}},
               #{<<"type">> := #{<<"type">> := ?AVRO_LONG,
                                 <<"logicalType">> := <<"timestamp-millis">>}},
               #{<<"type">> := #{<<"type">> := ?AVRO_BYTES,
                                 <<"precision">> := 5,
                                 <<"scale">> := 2}},
               #{<<"type">> := #{<<"type">> := ?AVRO_ARRAY,
                                 <<"items">> := ?AVRO_INT}},
               #{<<"type">> := #{<<"type">> := ?AVRO_ARRAY,
                                 <<"items">> := ?AVRO_INT}},
               #{<<"type">> := #{<<"type">> := ?AVRO_ARRAY,
                                 <<"items">> := ?AVRO_STRING}},
               #{<<"type">> := #{<<"type">> := ?AVRO_MAP,
                                 <<"values">> := ?AVRO_FLOAT}}]
         },
        #{<<"name">> := <<"MyAnnotated">>,
          <<"namespace">> := <<"org.erlang.ftp">>,
          <<"fields">> :=
              [#{<<"name">> := <<"error">>,
                 <<"type">> := <<"org.erlang.www.MyError">>}]},
        #{<<"name">> := <<"MyError">>,
          <<"fields">> :=
              [#{<<"type">> := <<"MyEnum2">>},
               #{<<"type">> := ?AVRO_STRING}]}],
       Types),
    ?assertMatch(
       [#{<<"name">> := <<"div">>},
        #{<<"name">> := <<"append">>,
          <<"error">> := [<<"MyError">>, <<"TheirError">>]},
        #{<<"name">> := <<"gen_server_cast">>, <<"one-way">> := true},
        #{<<"name">> := <<"ping">>}],
       Messages).


duplicate_annotation_avpr_test() ->
    ?assertError(
       {duplicate_annotation, "my_decorator", _, _},
       avro_idl:str_to_avpr(
         "@my_decorator(\"a\") @my_decorator(\"b\") protocol MyProto{}", "")
      ).

nested_complex_types_avr_test() ->
    ?assertEqual(
       #{<<"protocol">> => <<"P">>,
         <<"messages">> => [],
         <<"types">> =>
             [#{<<"type">> => ?AVRO_RECORD,
                <<"name">> => <<"R">>,
                <<"fields">> =>
                    [#{<<"name">> => <<"f">>,
                       <<"type">> =>
                           #{<<"type">> => ?AVRO_ARRAY,
                             <<"items">> =>
                                 #{<<"type">> => ?AVRO_MAP,
                                   <<"values">> => [?AVRO_NULL, <<"ns.T">>]}
                            }
                      }
                    ]}]},
       avro_idl:str_to_avpr(
         "protocol P { record R { array<map<union{null, ns.T}>> f; }}", "")
      ).

full_protocol_load_test() ->
    Schema = read_schema("full_protocol"),
    DecSchema = avro_idl:decode_schema(Schema, ""),
    _EncSchema = avro:encode_schema(DecSchema).
    %% ?debugFmt("~n~p~n~s", [DecSchema, EncSchema]).

%% Helpers

read_schema(Name) ->
    File = "test/data/" ++ Name ++ ".avdl",
    {ok, B} = file:read_file(File),
    binary_to_list(B).

idl_to_avpr(Name) ->
    Schema = read_schema(Name),
    avro_idl:str_to_avpr(Schema, "").
