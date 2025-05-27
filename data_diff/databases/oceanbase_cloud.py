from typing import Any, ClassVar, Dict, List, Optional, Type

import attrs
import jaydebeapi
import jpype

from data_diff.schema import RawColumnInfo
from data_diff.utils import match_regexps
from data_diff.abcs.database_types import (
    Decimal, Float, Text, DbPath, TemporalType, ColType, DbTime,
    ColType_UUID, Timestamp, TimestampTZ, FractionalType,
)
from data_diff.databases.base import (
    BaseDialect, ThreadedDatabase, import_helper, ConnectError, QueryError,
    CHECKSUM_OFFSET, CHECKSUM_HEXDIGITS, MD5_HEXDIGITS, TIMESTAMP_PRECISION_POS,
)

SESSION_TIME_ZONE = None  # Set by tests or externally


@import_helper("oceanbase_cloud")
def import_oceanbase():
    import jaydebeapi
    return jaydebeapi


@attrs.define(frozen=False)
class Dialect(BaseDialect):
    name = "OceanBaseOracle"
    SUPPORTS_PRIMARY_KEY: ClassVar[bool] = True
    SUPPORTS_INDEXES = True
    TYPE_CLASSES: Dict[str, type] = {
        "NUMBER": Decimal,
        "FLOAT": Float,
        "CHAR": Text,
        "NCHAR": Text,
        "NVARCHAR2": Text,
        "VARCHAR2": Text,
        "DATE": Timestamp,
        "TIMESTAMP": Timestamp,
    }
    ROUNDS_ON_PREC_LOSS = True
    PLACEHOLDER_TABLE = "DUAL"

    def quote(self, s: str) -> str:
        return f'"{s}"'

    def to_string(self, s: str) -> str:
        return f"cast({s} as varchar(1024))"

    def limit_select(self, select_query: str, offset: Optional[int] = None, limit: Optional[int] = None, has_order_by: Optional[bool] = None) -> str:
        if offset:
            raise NotImplementedError("OFFSET is not supported in OceanBase Oracle mode")
        return f"SELECT * FROM ({select_query}) FETCH NEXT {limit} ROWS ONLY"

    def concat(self, items: List[str]) -> str:
        return f"({' || '.join(items)})"

    def timestamp_value(self, t: DbTime) -> str:
        return "timestamp '%s'" % t.isoformat(" ")

    def random(self) -> str:
        return "dbms_random.value"

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"DECODE({a}, {b}, 1, 0) = 0"

    def type_repr(self, t) -> str:
        return {str: "VARCHAR(1024)"}.get(t, super().type_repr(t))

    def constant_values(self, rows) -> str:
        return " UNION ALL ".join(
            "SELECT %s FROM DUAL" % ", ".join(self._constant_value(v) for v in row) for row in rows
        )

    def md5_as_int(self, s: str) -> str:
        return f"to_number(substr(DBMS_CRYPTO.Hash(utl_raw.cast_to_raw({s}),2), {1 + MD5_HEXDIGITS - CHECKSUM_HEXDIGITS}), 'xxxxxxxxxxxxxxx') - {CHECKSUM_OFFSET}"

    def md5_as_hex(self, s: str) -> str:
        return f"DBMS_CRYPTO.Hash(utl_raw.cast_to_raw({s}), 2)"

    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        return f"CAST(TRIM({value}) AS VARCHAR(36))"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            return f"to_char(cast({value} as timestamp({coltype.precision})), 'YYYY-MM-DD HH24:MI:SS.FF6')"
        if coltype.precision > 0:
            truncated = f"to_char({value}, 'YYYY-MM-DD HH24:MI:SS.FF{coltype.precision}')"
        else:
            truncated = f"to_char({value}, 'YYYY-MM-DD HH24:MI:SS.')"
        return f"RPAD({truncated}, {TIMESTAMP_PRECISION_POS + 6}, '0')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        format_str = "FM" + "9" * (38 - coltype.precision)
        if coltype.precision:
            format_str += "0." + "9" * (coltype.precision - 1) + "0"
        return f"to_char({value}, '{format_str}')"

    def set_timezone_to_utc(self) -> str:
        return "ALTER SESSION SET TIME_ZONE = 'UTC'"

    def current_timestamp(self) -> str:
        return "LOCALTIMESTAMP"

    def parse_type(self, table_path: DbPath, info: RawColumnInfo) -> ColType:
    # 转成 Python 字符串，None 会变成 'None'
        data_type_str = str(info.data_type) if info.data_type is not None else ''
        if not data_type_str or data_type_str == 'None':
            print(f"[WARN] Unexpected data_type for column '{info.column_name}': {info.data_type!r}")
            return super().parse_type(table_path, info)
    
        regexps = {
        r"TIMESTAMP\((\d)\) WITH LOCAL TIME ZONE": Timestamp,
        r"TIMESTAMP\((\d)\) WITH TIME ZONE": TimestampTZ,
        r"TIMESTAMP\((\d)\)": Timestamp,
    }
        for m, t_cls in match_regexps(regexps, data_type_str):
            precision = int(m.group(1))
            return t_cls(precision=precision, rounds=self.ROUNDS_ON_PREC_LOSS)
        return super().parse_type(table_path, info)



##@attrs.define(frozen=False, init=False, kw_only=True)
class OceanBaseOracle(ThreadedDatabase):
    DIALECT_CLASS: ClassVar[Type[BaseDialect]] = Dialect
    CONNECT_URI_HELP = "oceanbase://<user>:<password>@<host>:<port>/<database>?jar=<jar-path>&driver=<driver-class>"
    CONNECT_URI_PARAMS = ["database?", "jar", "driver"]

    kwargs: Dict[str, Any]

    def __init__(self, *, host, database, jar, driver, user, password, port=1521, thread_count=4, **kw) -> None:
        super().__init__(thread_count=thread_count)
        self.default_schema = user.upper()
        self.jar = jar
        self.driver = driver
        self.database = database
        self.conn_args = {
            "jclassname": driver,
            "url": f"jdbc:oceanbase://{host}:{port}/{database}",
            "driver_args": [user, password],
            "jars": jar,
        }

    def create_connection(self):
        try:
            if not jpype.isJVMStarted():
                jvm_path = "/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home/jre/lib/server/libjvm.dylib"
                jpype.startJVM(jvm_path, "-ea", f"-Djava.class.path={self.conn_args['jars']}", convertStrings=False)
            
            # url = 'jdbc:oceanbase://obot6ql4n6xk9h7k-mi.aliyun-cn-hangzhou-internet.oceanbase.cloud:1521/CLIFTON'
            # user = 'CLIFTON'
            # password = 'Skp13775772708'
            # driver = 'com.oceanbase.jdbc.Driver'
            # jarFile = '/Users/clifton/oceanbase-client-2.4.9.1.jar'
            print(f"[DEBUG] loading jar: {self.conn_args['jars']}")


            ob = import_oceanbase()
            conn = ob.connect(**self.conn_args)
            if SESSION_TIME_ZONE:
                conn.cursor().execute(f"ALTER SESSION SET TIME_ZONE = '{SESSION_TIME_ZONE}'")
            return conn
        except Exception as e:
            raise ConnectError(*e.args) from e

    def _query_cursor(self, c, sql_code: str):
        try:
            return super()._query_cursor(c, sql_code)
        except Exception as e:
            raise QueryError(e)

    def select_table_schema(self, path: DbPath) -> str:
        schema, name = self._normalize_table_path(path)
        return (
            f"SELECT column_name, data_type, 6 as datetime_precision, data_precision as numeric_precision, data_scale as numeric_scale "
            f"FROM ALL_TAB_COLUMNS WHERE table_name = '{name}' AND owner = '{schema}'"
        )
