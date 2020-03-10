-record(protocol,
        {name,
         meta = [],
         definitions = []}).

-record(annotation,
        {name,
         value}).

-record(enum,
        {name,
         meta = [],
         variants = []}).

-record(fixed,
        {name,
         meta = [],
         size}).

-record(error,
        {name,
         meta = [],
         fields = []}).

-record(record,
        {name,
         meta = [],
         fields = []}).

-record(field,
        {name,
         meta = [],
         type,
         default}).

-record(function,
        {name,
         %% meta = [],
         arguments = [],
         return,
         extra}).
