from uuid import uuid4
from decimal import Decimal

from boto3.dynamodb.conditions import Key

from databases.database import Database
from databases.utils import measure_time, write_results_to_file
import boto3


class DynamodbManager(Database):
    def connect(self):
        client = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
        return None, client

    @measure_time
    def clear_database(self, client) -> None:
        table = client.Table('products')
        table.delete()
        table.wait_until_not_exists()
        client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': '_id',
                    'AttributeType': 'S'
                },
            ],
            TableName='products',
            KeySchema=[
                {
                    'AttributeName': '_id',
                    'KeyType': 'HASH'
                },
            ],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
        )

    @measure_time
    def insert_database(self, client, data) -> None:
        request_items = {'products': []}
        for i in range(1000):
            print(f"{i} - dynamo")
            for record in data:
                request_items['products'].append({
                    'PutRequest': {
                        'Item': {
                            "_id": str(uuid4()),
                            "name": record.name,
                            "price": Decimal(record.price),
                            "rating": Decimal(record.rating),
                            "rating_count": int(record.rating_count),
                            "timestamp": str(record.timestamp)
                        }
                    }
                })
                if len(request_items['products']) == 25:
                    client.batch_write_item(RequestItems=request_items)
                    request_items['products'] = []

    def update_database(self, client, query) -> None:
        query = query[0][1]
        table = client.Table('products')

        values_to_update, query_raw = query.split(" WHERE ")
        data = self.fetch_data(table, query_raw)
        update_kwargs = self._build_update_kwargs(values_to_update)

        self._update_data(data, table, update_kwargs)

    def fetch_data(self, table, query):
        records = []
        filter_expression = self._get_filter_expression(query) if query else {}
        done = False
        start_key = None
        while not done:
            if start_key:
                filter_expression['ExclusiveStartKey'] = start_key
            response = table.scan(**filter_expression)
            records.extend(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        return records

    def _get_filter_expression(self, query):
        result = ''
        for or_expression in query.split(" OR "):
            for and_expression in or_expression.split(" AND "):
                name, value = self._build_dict_values(and_expression)
                result += f"Key('{name}').eq({value}) and "
            result = result[:-5]
            result += " or "
        result = result[:-4]
        return {'FilterExpression': eval(result)}

    def _build_dict_values(self, value):
        parameter_name, parameter_value = value.split("=")
        return parameter_name,  self._parse_value(parameter_name, parameter_value)

    @staticmethod
    def _parse_value(param_name, param_value):
        try:
            if param_name in ['rating', 'price']:
                return 'Decimal(' + param_value + ')'
            if param_name == 'rating_count':
                return int(param_value)
            return param_value
        except Exception as e:
            print(f"DynamoDB update Exception -> Bad query! {str(e)}")

    def _build_update_kwargs(self, values_to_update):
        values_to_update = values_to_update.replace("'", "").replace('"', "")
        result = {
            "UpdateExpression": "set",
            "ExpressionAttributeValues": {},
            "ExpressionAttributeNames": {}
        }
        values_to_update = values_to_update[4:]
        for value in values_to_update.split(", "):
            name, value = self._build_dict_values(value)
            result["UpdateExpression"] += f" #{name} = :{name}, "
            result["ExpressionAttributeValues"][f":{name}"] = value
            result["ExpressionAttributeNames"][f"#{name}"] = name
        result["UpdateExpression"] = result["UpdateExpression"][:-2]
        return result

    @measure_time
    def _update_data(self, data, table, update_kwargs):
        try:
            for record in data:
                table.update_item(
                    Key={
                        '_id': record['_id']
                    },
                    ReturnValues="UPDATED_NEW",
                    **update_kwargs,
                )
            print(f"DynamoDB update success! updated records: {len(data)}")
        except Exception as e:
            print(f"DynamoDB update error -> {str(e)}")

    def select_from_database(self, client, args) -> None:
        query = args[0][1]
        catalog = args[0][2]
        data = self.select_data(client, query)
        write_results_to_file(catalog, "DynamoDB", data)

    @measure_time
    def select_data(self, client, query):
        table = client.Table('products')
        try:
            data = self.fetch_data(table, query)
            print(f"DynamoDB select success! {len(data)}")
            return data
        except Exception as e:
            print(f"DynamoDB select error -> {str(e)}")