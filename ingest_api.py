from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType
import requests

def get_db_data(field_name: str):
    res = requests.get(f"localhost:5273/{field_name}")


class DBAddressDataSource(DataSource):

    @classmethod
    def name(cls):
        return "db_address"

    def schema(self):
        return "id int, street string, state string, country string, zip string, timezone_offset int, updated_at timestamp"

    def reader(self, schema: StructType):
        return DBAddressDataSourceReader(schema, self.options)


class DBAddressDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options

    def read(self, partition):
        # Every value in this `self.options` dictionary is a string.
        num_rows = int(self.options.get("numRows", 3))
        for _ in range(num_rows):
            row = []
            for field in self.schema.fields:
                value = getattr(fake, field.name)()
                row.append(value)
            yield tuple(row)

