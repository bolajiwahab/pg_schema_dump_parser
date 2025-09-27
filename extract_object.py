"""Sorts dumped PostgreSQL schema into manifests."""
import enum
import typing

from pglast import ast, parser, stream, visitors

SCHEMA_QUALIFIED_LENGTH = 2


class ObjectType(enum.StrEnum):
    """Object types."""
    TABLE = enum.auto()
    MATERIALIZED_VIEW = enum.auto()
    INDEX = enum.auto()
    VIEW = enum.auto()
    FUNCTION = enum.auto()
    AGGREGATE = enum.auto()
    TYPE = enum.auto()
    SEQUENCE = enum.auto()
    EXTENSION = enum.auto()
    UTILITY = enum.auto()


class ManifestObject(typing.NamedTuple):
    """A manifest object."""
    schema: str | None
    name: str
    ddl: str
    # type: ObjectType


class SchemaSorter(visitors.Visitor): # type: ignore[misc]
    """Sorts dumped PostgreSQL schema into manifests."""
    def __init__(self) -> None:
        """Initialize variables."""
        self.extracted_objects: set[ManifestObject] = set()

    def visit_CreateStmt(self, ancestors: visitors.Ancestor, node: ast.CreateStmt) -> None:
        """Sorts tables."""
        if node.partspec:
            print(node.partspec)
        obj = ManifestObject(
            schema=node.relation.schemaname,
            name=node.relation.relname,
            ddl=stream.IndentedStream(semicolon_after_last_statement=True)(node) + "\n",
            # type=ObjectType.TABLE,
        )
        self.extracted_objects.add(obj)

    def visit_DropStmt(self, ancestors: visitors.Ancestor, node: ast.DropStmt) -> None:
        """Sorts tables."""
        # if node.partspec:
        print(node)
        # obj = ManifestObject(
        #     schema=node.relation.schemaname,
        #     name=node.relation.relname,
        #     ddl=stream.IndentedStream(semicolon_after_last_statement=True)(node) + "\n",
        #     type=ObjectType.TABLE,
        # )
        # self.extracted_objects.add(obj)

    def visit_CreateTableAsStmt(
        self, ancestors: visitors.Ancestor, node: ast.CreateTableAsStmt
    ) -> None:
        """Sorts materialized views."""
        obj = ManifestObject(
            schema=node.into.rel.schemaname,
            name=node.into.rel.relname,
            ddl=stream.IndentedStream(semicolon_after_last_statement=True)(node) + "\n",
            type=ObjectType.MATERIALIZED_VIEW,
        )
        self.extracted_objects.add(obj)

    def visit_IndexStmt(self, ancestors: visitors.Ancestor, node: ast.IndexStmt) -> None:
        """Sorts indexes."""
        obj = ManifestObject(
            schema=node.relation.schemaname,
            name=node.idxname,
            ddl=stream.IndentedStream(semicolon_after_last_statement=True)(node) + "\n",
            type=ObjectType.INDEX,
        )
        self.extracted_objects.add(obj)

    def visit_ViewStmt(self, ancestors: visitors.Ancestor, node: ast.ViewStmt) -> None:
        """Sorts views."""
        obj = ManifestObject(
            schema=node.view.schemaname,
            name=node.view.relname,
            ddl=stream.IndentedStream(semicolon_after_last_statement=True)(node) + "\n",
            type=ObjectType.VIEW,
        )
        self.extracted_objects.add(obj)

    def visit_CreateFunctionStmt(
        self, ancestors: visitors.Ancestor, node: ast.CreateFunctionStmt
    ) -> None:
        """Sorts functions."""
        obj = ManifestObject(
            name=node.funcname[-1].sval,
            schema=node.funcname[0].sval
            if len(node.funcname) > SCHEMA_QUALIFIED_LENGTH
            else None,
            ddl=stream.IndentedStream(semicolon_after_last_statement=True)(node) + "\n",
            type=ObjectType.FUNCTION,
        )
        self.extracted_objects.add(obj)


def extract_objects(source_code: str) -> set[ManifestObject]:
    """Sorts dumped PostgreSQL schema into manifests."""
    stmts = parser.parse_sql(source_code)
    print(stmts)
    extractor = SchemaSorter()

    extractor(stmts)

    return extractor.extracted_objects

# From SQL string
sql = """
CREATE TABLE measurement (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int,
    constraint measurement_pkey primary key (city_id, logdate)
) PARTITION BY RANGE (logdate);

CREATE TABLE measurement_y2006m02 PARTITION OF measurement
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');

DROP TABLE measurement;
"""
extracted = extract_objects(sql)
print(extracted)

# Last known state
table_states = {}

for event in extracted:
    print(event)
    # table = event["table"]
    # if event["type"] == "create":
    #     table_states[table] = event["sql"]
    # elif event["type"] == "drop":
    #     table_states.pop(table, None)  # Drop removes from current state
