%% @doc Tests for IDL lexer + parser
%% @end
%% @author Sergey Prokhorov <me@seriyps.ru>
-module(avro_idl_parse_tests).

-include("../src/idl.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_empty_protocol_test() ->
    ?assertEqual(
       #idl_protocol{name = "MyProto"},
       parse_idl("empty_protocol")).

parse_annotations_test() ->
    ?assertEqual(
       #idl_protocol{
          name = "MyProto",
          meta =
              [{doc, "My protocol"},
               {doc, "No, really\nIt's some multiline doc\n"
                "bullet points will be stripped\nso no unordered lists"},
               #idl_annotation{name = "version",
                           value = "1.0"},
               #idl_annotation{name = "aliases",
                           value = ["ns.Proto1", "ns.Proto2"]}
              ],
          definitions =
              [#idl_enum{name = "MyEnum",
                     meta =
                         [{doc, "My enum"},
                          #idl_annotation{name = "namespace",
                                      value = "enums"}],
                     variants = ["A", "B", "C"]},
               #idl_fixed{name = "MyFixed",
                      meta =
                          [{doc, "My Fixed"},
                           #idl_annotation{name = "namespace",
                                       value = "fixeds"}],
                     size = 16},
               #idl_error{name = "MyError",
                      meta =
                          [{doc, "My Error"},
                           #idl_annotation{name = "namespace",
                                       value = "errors"}],
                      fields =
                          [#idl_field{name = "my_err_field",
                                  meta =
                                      [{doc, "My Err Field"},
                                       #idl_annotation{name = "order",
                                                   value = "ignore"}],
                                  type = string}]},
               #idl_record{name = "MyRecord",
                       meta =
                           [{doc, "My Record"},
                            #idl_annotation{name = "namespace",
                                        value = "records"}],
                       fields =
                           [#idl_field{name = "my_record_field",
                                   meta =
                                       [{doc, "My Rec Field Type"},
                                        #idl_annotation{name = "order",
                                                    value = "ignore"},
                                        {doc, "My Rec Field"},
                                        #idl_annotation{name = "aliases",
                                                    value = ["my_alias"]}],
                                   type = string}]},
              #idl_function{name = "hello",
                        meta = [{doc, "My Fun"}],
                        arguments = [],
                        return = string,
                        extra = undefined}]
         },
       parse_idl("annotations")).

full_protocol_test() ->
    ?assertMatch(
      #idl_protocol{name = "Simple",
                meta =
                    [{doc, "An example protocol in Avro IDL"},
                     #idl_annotation{}],
                definitions =
                    [#idl_enum{name = "Kind"},
                     #idl_fixed{name = "MD5"},
                     #idl_record{name = "TestRecord"},
                     #idl_error{name = "TestError"},
                     #idl_function{name = "hello"},
                     #idl_function{name = "echo"},
                     #idl_function{name = "add"},
                     #idl_function{name = "echoBytes"},
                     #idl_function{name = "error"},
                     #idl_function{name = "ping"}]},
      parse_idl("full_protocol")).

protocol_with_typedefs_test() ->
    ?assertMatch(
      #idl_protocol{name = "MyProto",
                definitions =
                    [#idl_import{type = idl, file_path = "foo.avdl"},
                     #idl_import{type = protocol, file_path = "bar.avpr"},
                     #idl_import{type = schema, file_path = "baz.avsc"},
                     #idl_enum{name = "MyEnum1"},
                     #idl_enum{name = "MyEnum2"},
                     #idl_fixed{name = "MyFix"},
                     #idl_record{name = "MyRec",
                             fields =
                                 [#idl_field{name = "my_int", type = int},
                                  #idl_field{name = "my_string", type = string},
                                  #idl_field{name = "my_float", type = float},
                                  #idl_field{name = "my_bool", type = boolean,
                                         default = false},
                                  #idl_field{name = "my_custom",
                                         type = {custom, "MyFix"}},
                                  #idl_field{name = "my_union",
                                         type = {union, [boolean, null]},
                                         default = null},
                                  #idl_field{name = "my_date",
                                         type = date},
                                  #idl_field{name = "my_time",
                                         type = time_ms},
                                  #idl_field{name = "my_timestamp",
                                         type = timestamp_ms},
                                  #idl_field{name = "my_decimal",
                                         type = {decimal, 5, 2}},
                                  #idl_field{name = "my_int_array",
                                         type = {array, int}},
                                  #idl_field{},
                                  #idl_field{},
                                  #idl_field{name = "my_map",
                                         type = {map, float}}
                                 ]},
                     #idl_record{name = "MyAnnotated",
                             fields =
                                 [#idl_field{
                                     name = "kind",
                                     type = {custom,
                                             "org.erlang.www.MyEnum2"}}
                                 ]},
                     #idl_error{name = "MyError"},
                     #idl_function{name = "div",
                               extra = {throws, ["DivisionByZero"]}},
                     #idl_function{name = "append",
                               extra = {throws, ["MyError", "TheirError"]}},
                     #idl_function{name = "gen_server_cast", extra = oneway},
                     #idl_function{name = "ping", extra = undefined}]},
       parse_idl("protocol_with_typedefs")).

empty_record_test() ->
    ?assertEqual(
       #idl_protocol{
          name = "P",
          definitions = [#idl_record{name = "R"}]},
       parse_str("protocol P { record R {} }")).

empty_error_test() ->
    ?assertEqual(
       #idl_protocol{
          name = "P",
          definitions = [#idl_error{name = "E"}]},
       parse_str("protocol P { error E {} }")).

empty_string_default_test() ->
    #idl_protocol{definitions = [#idl_record{fields = [#idl_field{default = Default}]}]} =
        parse_str("protocol P { record R { string f = \"\"; } }"),
    ?assertEqual("", Default).

escaped_quote_in_string_test() ->
    #idl_protocol{definitions = [#idl_record{fields = [#idl_field{default = Default}]}]} =
        parse_str("protocol P { record R { string f = \"foo\\\"bar\"; } }"),
    ?assertEqual("foo\"bar", Default).

function_annotation_test() ->
    ?assertMatch(
       #idl_protocol{
          definitions = [#idl_function{
              name = "hello",
              meta = [#idl_annotation{name = "deprecated", value = "true"},
                      {doc, "Say hello"}],
              return = string}]},
       parse_str("protocol P { @deprecated(\"true\") /** Say hello */ string hello(); }")).

function_multi_meta_test() ->
    ?assertMatch(
       #idl_protocol{
          definitions = [#idl_function{
              name = "hello",
              meta = [{doc, "Doc one"}, {doc, "Doc two"}]}]},
       parse_str("protocol P { /** Doc one */ /** Doc two */ string hello(); }")).

array_types_test() ->
    Probes =
        [{int, "int"},
         {{decimal, 1, 2}, "decimal(1, 2)"},
         {null, "null"},
         {{custom, "MyType"}, "MyType"},
         {{custom, "my_ns.MyType"}, "my_ns.MyType"},
         {{union, [int, null]}, "union{int, null}"},
         {{array, int}, "array<int>"},
         {{map, int}, "map<int>"}],
    lists:foreach(
     fun({ExpectType, IdlType}) ->
             test_field_type({array, ExpectType}, "array<" ++ IdlType ++ ">")
     end, Probes).

map_types_test() ->
    Probes =
        [{int, "int"},
         {{custom, "MyType"}, "MyType"},
         {{array, int}, "array<int>"},
         {{map, int}, "map<int>"}],
    lists:foreach(
     fun({ExpectType, IdlType}) ->
             test_field_type({map, ExpectType}, "map<" ++ IdlType ++ ">")
     end, Probes).

%% Helpers

test_field_type(ExpectType, IdlType) ->
    Idl = ("protocol P {"
           " record R { " ++ IdlType ++ " f; }"
           "}"),
    #idl_protocol{
       definitions =
           [#idl_record{
               fields =
                   [#idl_field{type = Type}]}]} = parse_str(Idl),
    ?assertEqual(ExpectType, Type).%% ,  % ?assertEqual/3 only OTP-20+
                 %% #{proto => Idl,
                 %%   type => IdlType}).

parse_idl(Name) ->
    File = "test/data/" ++ Name ++ ".avdl",
    {ok, B} = file:read_file(File),
    parse_str(binary_to_list(B)).

parse_str(Str) ->
    {ok, T0, _} =  avro_idl_lexer:string(Str),
    %% ?debugFmt("Name: ~p~nTokens:~n~p", [Name, T0]),
    T = avro_idl_lexer:preprocess(T0, [drop_comments, trim_doc]),
    {ok, Tree} = avro_idl_parser:parse(T),
    Tree.
