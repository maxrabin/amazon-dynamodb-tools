from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Generator

import boto3
from botocore.exceptions import ClientError

athena = boto3.client("athena")
sts = boto3.client("sts")


@dataclass(frozen=True)
class AccountRegionPair:
    account_id: str
    region: str


@dataclass()
class QueryResultData:
    region: str
    account_id: str
    table_name: str
    recommendation: str
    update_result: str

    @property
    def as_account_and_region(self) -> AccountRegionPair:
        return AccountRegionPair(account_id=self.account_id, region=self.region)


def get_dynamodb_client(account_id: str, region_name: str):
    response = sts.assume_role(
        RoleArn=f"arn:aws:iam::{account_id}:role/DynamoDBStorageClassOptimizer",
        RoleSessionName="DynamoDBStorageClassOptimizer",
    )
    credentials = response["Credentials"]
    dynamodb = boto3.client(
        "dynamodb",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=region_name,
    )
    return dynamodb


def get_query_results(query_id: str) -> Generator[QueryResultData, None, None]:
    paginator = athena.get_paginator("get_query_results")
    response_iterator = paginator.paginate(QueryExecutionId=query_id)
    keys: None | list[str] = None
    for page in response_iterator:
        assert "Rows" in page["ResultSet"]
        for row in page["ResultSet"]["Rows"]:
            assert "Data" in row
            values: list[str] = [
                item["VarCharValue"] for item in row["Data"] if "VarCharValue" in item
            ]
            if not keys:
                keys = values
            else:
                yield QueryResultData(**dict(zip(keys, values)))


def main(query_id: str, is_dry_run: bool) -> Generator[QueryResultData, None, None]:
    tables_per_account_and_region: defaultdict[
        AccountRegionPair, list[QueryResultData]
    ] = defaultdict(list)
    for result in get_query_results(query_id):
        tables_per_account_and_region[result.as_account_and_region].append(result)
    for key, changes in tables_per_account_and_region.items():
        dynamodb_client = get_dynamodb_client(
            account_id=key.account_id, region_name=key.region
        )
        for change in changes:
            assert change.recommendation in ("STANDARD", "STANDARD_INFREQUENT_ACCESS")
            print(
                f"Updating table {change.table_name} to storage class {change.recommendation}"
            )
            if is_dry_run:
                change.update_result = "Dry Run - Did not update"
            else:
                try:
                    update_result = dynamodb_client.update_table(
                        TableName=change.table_name, TableClass=change.recommendation
                    )
                    assert "TableDescription" in update_result
                    assert "TableStatus" in update_result["TableDescription"]
                    change.update_result = update_result["TableDescription"][
                        "TableStatus"
                    ]
                except ClientError as error:
                    assert "Error" in error.response
                    assert "Message" in error.response["Error"]
                    change.update_result = error.response["Error"]["Message"]
            yield change


def lambda_handler(event: dict[str, Any], _) -> list[dict]:
    assert isinstance((query_id := event.get("QueryExecutionId")), str)
    is_dry_run: bool = (
        passed_value
        if "IsDryRun" in event
        and (passed_value := event["IsDryRun"])
        and isinstance(passed_value, bool)
        else False
    )
    result_data: Generator[QueryResultData, None, None] = main(query_id, is_dry_run)
    return [asdict(result) for result in result_data]
