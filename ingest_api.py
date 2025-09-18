from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType
import requests

def get_db_data(field_name: str):
    res = requests.get(f"http://localhost:5231/api/batch/{field_name}")
    if res.status_code == 200:
        return res.json()
    else:
        print(res.status_code)
        print(f"error fetching data using {field_name}")
        return {"data": []}

class Promotion(DataSource):
    @classmethod
    def name(cls):
        return "promotion"

    def schema(self):
        return "id int, promotionDescription string, startTime string, endTime string, discountAmount string, updatedAt string"

    def reader(self, schema: StructType):
        return UFPIAPIReader(schema, self.options, self.name())


class Orders(DataSource):
    @classmethod
    def name(cls):
        return "orders"

    def schema(self):
        return "id int, customerId int, lineItems Array<bigint>, shippingStatus string, paymentType string, totalPrice string, totalTax string, shippingCost string, currency string, createdAt string, updatedAt string"

    def reader(self, schema: StructType):
        return UFPIAPIReader(schema, self.options, self.name())


class LineItem(DataSource):
    @classmethod
    def name(cls):
        return "line_item"

    def schema(self):
        return "id int, productId string, promotionIds Array<int>, quantity int, updatedAt string"

    def reader(self, schema: StructType):
        return UFPIAPIReader(schema, self.options, self.name())


class Address(DataSource):
    @classmethod
    def name(cls):
        return "address"

    def schema(self):
        return "id int, street string, state string, country string, zip string, timezoneOffset int, updatedAt string"

    def reader(self, schema: StructType):
        return UFPIAPIReader(schema, self.options, self.name())


class Customer(DataSource):
    @classmethod
    def name(cls):
        return "customer"

    def schema(self):
        return "id int, companyName string, email string, phone string, industry string, deliveryAddressId string, corporateAddressId string, createdAt string, updatedAt string"

    def reader(self, schema: StructType):
        return UFPIAPIReader(schema, self.options, self.name())


class UFPIAPIReader(DataSourceReader):
    def __init__(self, schema, options, table_name):
        self.schema: StructType = schema
        self.options = options
        self.table_name = table_name

    def read(self, partition):
        data = get_db_data(self.table_name)["data"]
        for r in data:
            row = []
            for field in self.schema.fields:
                row.append(r[field.name])
            yield tuple(row)

def run():
    spark = (SparkSession.builder
             .getOrCreate())

    spark.dataSource.register(Address)
    spark.dataSource.register(Customer)
    spark.dataSource.register(Promotion)
    spark.dataSource.register(Orders)
    spark.dataSource.register(LineItem)
    spark.read.format("address").load().show()
    spark.read.format("customer").load().show()
    spark.read.format("promotion").load().show()
    spark.read.format("orders").load().show()
    spark.read.format("line_item").load().show()

run()
