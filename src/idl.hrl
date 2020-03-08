-record(protocol,
        {name,
         annotations = [],
         definitions = []}).

-record(annotation,
        {name,
         value}).

-record(enum,
        {name,
         annotations = [],
         variants = []}).

-record(fixed,
        {name,
         annotations = [],
         size}).

-record(error,
        {name,
         annotations = [],
         fields = []}).

-record(record,
        {name,
         annotations = [],
         fields = []}).

-record(field,
        {name,
         annotations = [],
         type,
         default}).

-record(function,
        {name,
         %% annotations = [],
         arguments = [],
         return,
         extra}).
