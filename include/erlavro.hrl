-ifndef(_ERLAVRO_HRL_).
-define(_ERLAVRO_HRL_, true).

-define(REQUIRED, =erlang:error("Required field isn't initialized during "
                                "record instantiation in "
                                ++ ?MODULE_STRING ++ ":" ++ ?LINE)).

-type avro_ordering() :: ascending | descending | ignore.

-record(avro_field,
        { name      ?REQUIRED :: string()
        , doc       = ""      :: string()
        , type      ?REQUIRED :: avro_type_or_name()
        , default             :: avro_value() | undefined
        , order  = ascending  :: avro_ordering()
        , aliases   = []      :: [string()]
        }).

-record(avro_primitive_type,
        { name      ?REQUIRED :: string()
        }).

-record(avro_record_type,
        { name      ?REQUIRED :: string()
        , namespace = ""      :: string()
        , doc       = ""      :: string()
        , aliases   = []      :: [string()]
        , fields    ?REQUIRED :: [#avro_field{}]
        }).

-record(avro_enum_type,
        { name      ?REQUIRED :: string()
        , namespace = ""      :: string()
        , aliases   = []      :: [string()]
        , doc       = ""      :: string()
        , symbols   ?REQUIRED :: [string()]
        }).

-record(avro_array_type,
        { type      ?REQUIRED :: avro_type_or_name()
        }).

-record(avro_map_type,
        { type      ?REQUIRED :: avro_type_or_name()
        }).

-record(avro_union_type,
        { types     ?REQUIRED :: [avro_type_or_name()]
        }).

-record(avro_fixed_type,
        { name      ?REQUIRED :: string()
        , namespace = ""      :: string()
        , aliases   = []      :: [string()]
        , size      ?REQUIRED :: integer()
        }).

-record(avro_value,
        { type  :: avro_type()
        , value :: term()
        }).

-type avro_type()  :: #avro_primitive_type{} |
                      #avro_record_type{} |
                      #avro_enum_type{}   |
                      #avro_array_type{}  |
                      #avro_map_type{}    |
                      #avro_union_type{}  |
                      #avro_fixed_type{}.

-type avro_type_or_name() :: avro_type() | string().

-type avro_value() :: #avro_value{}.

-endif.
