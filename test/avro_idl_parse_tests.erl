%% @doc Tests for IDL lexer + parser
-module(avro_idl_parse_tests).

-include("../src/idl.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_empty_protocol_test() ->
    ?assertEqual(
       #protocol{name = "MyProto"},
       parse_idl("empty_protocol")).

parse_annotations_test() ->
    ?assertEqual(
       #protocol{
          name = "MyProto",
          meta =
              [{doc, "My protocol"},
               {doc, "No, really\nIt's some multiline doc\n"
                "bullet points will be stripped\nso no unordered lists"},
               #annotation{name = "version",
                           value = "1.0"},
               #annotation{name = "aliases",
                           value = ["ns.Proto1", "ns.Proto2"]}
              ],
          definitions =
              [#enum{name = "MyEnum",
                     meta =
                         [{doc, "My enum"},
                          #annotation{name = "namespace",
                                      value = "enums"}],
                     variants = ["A", "B", "C"]},
               #fixed{name = "MyFixed",
                      meta =
                          [{doc, "My Fixed"},
                           #annotation{name = "namespace",
                                       value = "fixeds"}],
                     size = 16},
               #error{name = "MyError",
                      meta =
                          [{doc, "My Error"},
                           #annotation{name = "namespace",
                                       value = "errors"}],
                      fields =
                          [#field{name = "my_err_field",
                                  meta =
                                      [{doc, "My Err Field"},
                                       #annotation{name = "order",
                                                   value = "ignore"}],
                                  type = string}]},
               #record{name = "MyRecord",
                       meta =
                           [{doc, "My Record"},
                            #annotation{name = "namespace",
                                        value = "records"}],
                       fields =
                           [#field{name = "my_record_field",
                                   meta =
                                       [{doc, "My Rec Field Type"},
                                        #annotation{name = "order",
                                                    value = "ignore"},
                                        {doc, "My Rec Field"},
                                        #annotation{name = "aliases",
                                                    value = ["my_alias"]}],
                                   type = string}]},
              #function{name = "hello",
                        meta = [{doc, "My Fun"}],
                        arguments = [],
                        return = string,
                        extra = undefined}]
         },
       parse_idl("annotations")).

full_protocol_test() ->
    ?assertMatch(
      #protocol{name = "Simple",
                meta =
                    [{doc, "An example protocol in Avro IDL"},
                     #annotation{}],
                definitions =
                    [#enum{name = "Kind"},
                     #fixed{name = "MD5"},
                     #record{name = "TestRecord"},
                     #error{name = "TestError"},
                     #function{name = "hello"},
                     #function{name = "echo"},
                     #function{name = "add"},
                     #function{name = "echoBytes"},
                     #function{name = "error"},
                     #function{name = "ping"}]},
      parse_idl("full_protocol")).

protocol_with_typedefs_test() ->
    ?assertMatch(
      #protocol{name = "MyProto",
                definitions =
                    [#import{type = idl, file_path = "foo.avdl"},
                     #import{type = protocol, file_path = "bar.avpr"},
                     #import{type = schema, file_path = "baz.avsc"},
                     #enum{name = "MyEnum1"},
                     #enum{name = "MyEnum2"},
                     #fixed{name = "MyFix"},
                     #record{name = "MyRec",
                             fields =
                                 [#field{name = "my_int", type = int},
                                  #field{name = "my_string", type = string},
                                  #field{name = "my_float", type = float},
                                  #field{name = "my_bool", type = boolean,
                                         default = false},
                                  #field{name = "my_custom",
                                         type = {custom, "MyFix"}},
                                  #field{name = "my_union",
                                         type = {union, [boolean, null]},
                                         default = null},
                                  #field{name = "my_date",
                                         type = date},
                                  #field{name = "my_time",
                                         type = time_ms},
                                  #field{name = "my_timestamp",
                                         type = timestamp_ms},
                                  #field{name = "my_decimal",
                                         type = {decimal, 5, 2}},
                                  #field{name = "my_int_array",
                                         type = {array, int}},
                                  #field{},
                                  #field{},
                                  #field{name = "my_map",
                                         type = {map, float}}
                                 ]},
                     #record{name = "MyAnnotated",
                             fields =
                                 [#field{
                                     name = "error",
                                     type = {custom,
                                             "org.erlang.www.MyError"}}
                                 ]},
                     #error{name = "MyError"},
                     #function{name = "div",
                               extra = {throws, ["DivisionByZero"]}},
                     #function{name = "append",
                               extra = {throws, ["MyError", "TheirError"]}},
                     #function{name = "gen_server_cast", extra = oneway},
                     #function{name = "ping", extra = undefined}]},
       parse_idl("protocol_with_typedefs")).

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
    #protocol{
       definitions =
           [#record{
               fields =
                   [#field{type = Type}]}]} = parse_str(Idl),
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
