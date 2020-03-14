%% @doc Tests for IDL converter / loader
-module(avro_idl_tests).

-include("../src/idl.hrl").
-include_lib("eunit/include/eunit.hrl").


empty_protocol_avpr_test() ->
    ?assertEqual(
       #{protocol => "MyProto",
         types => [],
         messages => []},
       idl_to_avpr("empty_protocol")).


annotations_avpr_test() ->
    ?assertEqual(
      #{"doc" => ("My protocol\nNo, really\nIt's some multiline doc\n"
                  "bullet points will be stripped\nso no unordered lists"),
        "version" => "1.0",
        "aliases" => ["ns.Proto1", "ns.Proto2"],
        protocol => "MyProto",
        types =>
            [#{"doc" => "My enum",
               "namespace" => "enums",
               type => enum,
               name => "MyEnum",
               variants => ["A", "B", "C"]},
             #{"doc" => "My Fixed",
               "namespace" => "fixeds",
               type => fixed,
               name => "MyFixed",
               size => 16},
             #{"doc" => "My Error",
               "namespace" => "errors",
               type => error,
               name => "MyError",
               fields =>
                   [#{"doc" => "My Err Field",
                      "order" => "ignore",
                      type => string,
                      name => "my_err_field"}]},
             #{"doc" => "My Record",
               "namespace" => "records",
               type => record,
               name => "MyRecord",
               fields =>
                   [#{"doc" => "My Rec Field Type\nMy Rec Field",
                      "order" => "ignore",
                      "aliases" => ["my_alias"],
                      type => string,
                      name => "my_record_field"}]}],
        messages =>
            [#{"doc" => "My Fun",
               name => "hello",
               request => [],
               response => string}]
       },
      idl_to_avpr("annotations")).


full_protocol_avpr_test() ->
    ?assertMatch(
       #{},
      idl_to_avpr("full_protocol")).


protocol_with_typedefs_avpr_test() ->
    ?assertMatch(
      #{"namespace" := "org.erlang.www",
        protocol := "MyProto",
        types :=
            [#{name := "MyEnum1"},
             #{name := "MyEnum2",
               type := enum,
               variants := ["VAR21", "VAR22", "VAR23"]},
             #{name := "MyFix",
               type := fixed,
               size := 10},
             #{name := "MyRec",
               fields :=
                   [#{type := int},
                    #{type := string},
                    #{type := float},
                    #{type := boolean},
                    #{type := "MyFix"},
                    #{type := [boolean, null]},
                    #{type := #{type := int, 'logicalType' := "date"}},
                    #{type := #{type := int, 'logicalType' := "time-millis"}},
                    #{type := #{type := long,
                                'logicalType' := "timestamp-millis"}},
                    #{type := #{type := bytes, precision := 5, scale := 2}},
                    #{type := #{type := array, items := int}},
                    #{type := #{type := array, items := int}},
                    #{type := #{type := array, items := string}},
                    #{type := #{type := map, values := float}}]
              },
             #{name := "MyAnnotated",
               "namespace" := "org.erlang.ftp",
               fields :=
                   [#{name := "error",
                      type := "org.erlang.www.MyError"}]},
             #{name := "MyError",
               fields :=
                   [#{type := "MyEnum2"},
                    #{type := string}]}],
        messages :=
            [#{name := "div"},
             #{name := "append"},
             #{name := "gen_server_cast"},
             #{name := "ping"}]},
       idl_to_avpr("protocol_with_typedefs")).


duplicate_annotation_test() ->
    ?assertError(
       {duplicate_annotation, "my_decorator", _, _},
       avro_idl:str_to_avpr(
         "@my_decorator(\"a\") @my_decorator(\"b\") protocol MyProto{}", "")
      ).

%% Helpers

idl_to_avpr(Name) ->
    File = "test/data/" ++ Name ++ ".avdl",
    {ok, B} = file:read_file(File),
    avro_idl:str_to_avpr(binary_to_list(B), "").
