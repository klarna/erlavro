-module(avro).

-export([]).
-export([]).

-include_lib("erlavro/include/erlavro.hrl").



%%%===================================================================
%%% Internal functions
%%%===================================================================


%%%===================================================================
%%% Sample code
%%%===================================================================

-ifdef(EUNIT).

-include_lib("eunit/include/eunit.hrl").

record_creation_test() ->
    Schema = #avro_record_type
             { name = "Test"
             , fields = [#avro_field{name = "invno", type = avro_schema:long()}]
             },
    Record = avro_record:new(Schema),
    avro_record:set("invno", avro_primitive:long(1)),
    ok.

-endif.
