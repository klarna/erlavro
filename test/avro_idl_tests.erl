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
       [#{<<"name">> := <<"FooRecord">>, <<"type">> := ?AVRO_RECORD},
        #{<<"name">> := <<"FooEnum">>, <<"type">> := ?AVRO_ENUM},
        #{<<"name">> := <<"BarRecord">>, <<"type">> := ?AVRO_RECORD},
        #{<<"name">> := <<"BazRecord">>, <<"type">> := ?AVRO_RECORD},
        #{<<"name">> := <<"MyEnum1">>},
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
              [#{<<"name">> := <<"kind">>,
                 <<"type">> := <<"org.erlang.www.MyEnum2">>}]},
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


import_idl_test() ->
    Proto = avro_idl:str_to_avpr(
              "protocol P { import idl \"foo.avdl\"; }",
              "test/data"),
    #{<<"types">> := Types} = Proto,
    ?assertMatch(
       [#{<<"name">> := <<"FooRecord">>, <<"type">> := ?AVRO_RECORD},
        #{<<"name">> := <<"FooEnum">>, <<"type">> := ?AVRO_ENUM}],
       Types).

import_protocol_test() ->
    Proto = avro_idl:str_to_avpr(
              "protocol P { import protocol \"bar.avpr\"; }",
              "test/data"),
    #{<<"types">> := Types} = Proto,
    ?assertMatch(
       [#{<<"name">> := <<"BarRecord">>, <<"type">> := ?AVRO_RECORD}],
       Types).

import_schema_test() ->
    Proto = avro_idl:str_to_avpr(
              "protocol P { import schema \"baz.avsc\"; }",
              "test/data"),
    #{<<"types">> := Types} = Proto,
    ?assertMatch(
       [#{<<"name">> := <<"BazRecord">>, <<"type">> := ?AVRO_RECORD}],
       Types).

import_nested_idl_test() ->
    %% subdir/submodule.avdl imports ../foo.avdl. The top-level caller's
    %% cwd is "test/data" which is also the default rootdir, so the ".."
    %% from subdir/ stays inside the rooted tree and the import resolves
    %% to test/data/foo.avdl. Also verifies that import paths are resolved
    %% relative to the importing file, not the top-level cwd (foo.avdl is
    %% not at "test/data/subdir/../foo.avdl" unless we recurse correctly).
    Proto = avro_idl:str_to_avpr(
              "protocol P { import idl \"subdir/submodule.avdl\"; }",
              "test/data"),
    #{<<"types">> := Types} = Proto,
    ?assertMatch(
       [#{<<"name">> := <<"FooRecord">>, <<"type">> := ?AVRO_RECORD},
        #{<<"name">> := <<"FooEnum">>, <<"type">> := ?AVRO_ENUM},
        #{<<"name">> := <<"SubRecord">>, <<"type">> := ?AVRO_RECORD}],
       Types).

import_with_rootdir_allows_dotdot_within_tree_test() ->
    %% Loading subdir/submodule.avdl directly: its cwd becomes subdir/, so
    %% "../foo.avdl" would escape if rootdir defaulted to that. Passing the
    %% wider rootdir explicitly lets the import resolve while keeping the
    %% confinement boundary.
    SubmoduleFile = test_data("subdir/submodule.avdl"),
    {ok, Bin} = file:read_file(SubmoduleFile),
    RootDir = test_data(""),
    Proto = avro_idl:str_to_avpr(
              binary_to_list(Bin),
              filename:dirname(SubmoduleFile),
              [{rootdir, RootDir}]),
    #{<<"types">> := Types} = Proto,
    ?assertMatch(
       [#{<<"name">> := <<"FooRecord">>, <<"type">> := ?AVRO_RECORD},
        #{<<"name">> := <<"FooEnum">>, <<"type">> := ?AVRO_ENUM},
        #{<<"name">> := <<"SubRecord">>, <<"type">> := ?AVRO_RECORD}],
       Types).

import_with_rootdir_blocks_escape_test() ->
    %% rootdir set to subdir/ — ../foo.avdl escapes the rooted tree and
    %% must be refused even though the file exists.
    SubmoduleFile = test_data("subdir/submodule.avdl"),
    {ok, Bin} = file:read_file(SubmoduleFile),
    Cwd = filename:dirname(SubmoduleFile),
    ?assertError(
       {badmatch, {error, {import_outside_root, "../foo.avdl"}}},
       avro_idl:str_to_avpr(
         binary_to_list(Bin), Cwd, [{rootdir, Cwd}])).

import_with_read_fun_test() ->
    %% All schemas kept in memory; no filesystem access.
    %% dep.avdl transitively imports transitive.avdl, verifying read_fun
    %% is threaded through recursive imports.
    Files = #{
        {"root", "dep.avdl"} =>
            <<"protocol Dep {\n"
              "  import idl \"transitive.avdl\";\n"
              "  record DepRecord { string dep_field; }\n"
              "}">>,
        {"root", "transitive.avdl"} =>
            <<"protocol Trans { record TransRecord { int t_field; } }">>
    },
    ReadFun = fun(Cwd, Path) ->
        case maps:find({Cwd, Path}, Files) of
            {ok, Bin} -> {ok, Bin};
            error     -> {error, enoent}
        end
    end,
    Proto = avro_idl:str_to_avpr(
              "protocol P { import idl \"dep.avdl\"; }",
              "root",
              [{read_fun, ReadFun}]),
    #{<<"types">> := Types} = Proto,
    ?assertMatch(
       [#{<<"name">> := <<"TransRecord">>, <<"type">> := ?AVRO_RECORD},
        #{<<"name">> := <<"DepRecord">>,   <<"type">> := ?AVRO_RECORD}],
       Types).

import_outside_root_rejected_test_() ->
    %% The default read_fun resolves imports relative to the importing
    %% file's directory and refuses paths that point outside it, both
    %% absolute paths and relative paths that escape through "..".
    AttackPaths = ["/etc/passwd", "../../etc/passwd"],
    Variants = ["idl", "protocol", "schema"],
    [?_assertError(
        {badmatch, {error, {import_outside_root, _}}},
        avro_idl:str_to_avpr(
          "protocol P { import " ++ Kind ++ " \"" ++ AP ++ "\"; }",
          "test/data"))
     || Kind <- Variants, AP <- AttackPaths].

import_outside_root_with_read_fun_override_test() ->
    %% The strict default can be bypassed by supplying a custom read_fun;
    %% verify the option still takes effect and the default is not applied
    %% on top of it.
    ReadFun = fun(_Cwd, _Path) ->
                {ok, <<"protocol Bypassed { record R { int n; } }">>}
              end,
    Proto = avro_idl:str_to_avpr(
              "protocol P { import idl \"/anywhere/on/disk.avdl\"; }",
              "test/data",
              [{read_fun, ReadFun}]),
    #{<<"types">> := Types} = Proto,
    ?assertMatch([#{<<"name">> := <<"R">>}], Types).

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

encode_decode_test() ->
    %% subdir/submodule.avdl imports ../foo.avdl (idl); verify types from
    %% both files can be encoded/decoded after loading via schema store.
    %% rootdir is the test/data tree so the "../" import resolves inside it.
    SubmoduleFile = test_data("subdir/submodule.avdl"),
    Store1 = avro_schema_store:new(
               [], [SubmoduleFile], [{rootdir, test_data("")}]),
    LookupFun1 = avro_schema_store:to_lookup_fun(Store1),
    Encoder1 = avro:make_encoder(LookupFun1, []),
    Decoder1 = avro:make_decoder(LookupFun1, []),
    %% FooRecord comes from the imported ../foo.avdl
    FooTerm = [{<<"foo_field">>, <<"hello">>}],
    ?assertEqual(FooTerm,
                 Decoder1("FooRecord",
                          iolist_to_binary(Encoder1("FooRecord", FooTerm)))),
    %% SubRecord is defined locally in submodule.avdl
    SubTerm = [{<<"sub_field">>, <<"world">>}],
    ?assertEqual(SubTerm,
                 Decoder1("SubRecord",
                          iolist_to_binary(Encoder1("SubRecord", SubTerm)))),
    %% protocol_with_typedefs.avdl exercises all three import kinds:
    %%   import idl      "foo.avdl"  -> FooRecord, FooEnum
    %%   import protocol "bar.avpr"  -> BarRecord
    %%   import schema   "baz.avsc"  -> BazRecord
    ProtoFile = test_data("protocol_with_typedefs.avdl"),
    Store2 = avro_schema_store:new([], [ProtoFile]),
    LookupFun2 = avro_schema_store:to_lookup_fun(Store2),
    Encoder2 = avro:make_encoder(LookupFun2, []),
    Decoder2 = avro:make_decoder(LookupFun2, []),
    BarTerm = [{<<"bar_field">>, 42}],
    BarBin = iolist_to_binary(Encoder2("org.erlang.www.BarRecord", BarTerm)),
    ?assertEqual(BarTerm, Decoder2("org.erlang.www.BarRecord", BarBin)),
    BazTerm = [{<<"baz_field">>, true}],
    BazBin = iolist_to_binary(Encoder2("org.erlang.www.BazRecord", BazTerm)),
    ?assertEqual(BazTerm, Decoder2("org.erlang.www.BazRecord", BazBin)),
    %% MyAnnotated is in org.erlang.ftp namespace,
    %% its field references org.erlang.www.MyEnum2
    AnnTerm = [{<<"kind">>, <<"VAR21">>}],
    AnnBin = iolist_to_binary(
               Encoder2("org.erlang.ftp.MyAnnotated", AnnTerm)),
    ?assertEqual(AnnTerm,
                 Decoder2("org.erlang.ftp.MyAnnotated", AnnBin)).

full_protocol_load_test() ->
    Schema = read_schema("full_protocol"),
    DecSchema = avro_idl:decode_schema(Schema, ""),
    _EncSchema = avro:encode_schema(DecSchema).
    %% ?debugFmt("~n~p~n~s", [DecSchema, EncSchema]).

%% Helpers

test_data(FileName) ->
    filename:join([code:lib_dir(erlavro), "test", "data", FileName]).

read_schema(Name) ->
    File = "test/data/" ++ Name ++ ".avdl",
    {ok, B} = file:read_file(File),
    binary_to_list(B).

idl_to_avpr(Name) ->
    File = "test/data/" ++ Name ++ ".avdl",
    {ok, B} = file:read_file(File),
    avro_idl:str_to_avpr(binary_to_list(B), filename:dirname(File)).
