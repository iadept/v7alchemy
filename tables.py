from  engine import Field, Table


class PeriodField(Field):

    def __init__(self):
        super().__init__("period")


class TableJournal(Table):

    prefix: str = "1SJOURN"
    name: str = "Journal"

    doc = Field("IDDOC")
    docref = Field("IDDOCREF")
    date = Field("DATE")
    number = Field("DOCNO")
    ismark = Field("ISMARK")


class TableSC(Table):
    prefix = "SC"
    file: str = None

    id = Field("ID")
    code = Field("CODE")
    description = Field("DESCR")


class TableRG(Table):
    prefix = "RG"
    file: str = None

    period = PeriodField()


class TableRA(Table):
    prefix = "RA"
    file: str = None

    doc = Field("IDDOC", join=TableJournal.doc)
    lineno = Field("LINENO")


