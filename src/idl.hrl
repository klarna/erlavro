-record(idl_protocol,
        {name,
         meta = [],
         definitions = []}).

-record(idl_annotation,
        {name,
         value}).

-record(idl_import,
        {type,
         file_path}).

-record(idl_enum,
        {name,
         meta = [],
         variants = []}).

-record(idl_fixed,
        {name,
         meta = [],
         size}).

-record(idl_error,
        {name,
         meta = [],
         fields = []}).

-record(idl_record,
        {name,
         meta = [],
         fields = []}).

-record(idl_field,
        {name,
         meta = [],
         type,
         default}).

-record(idl_function,
        {name,
         meta = [],
         arguments = [],
         return,
         extra}).
