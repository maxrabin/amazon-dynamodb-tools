import io
import os
import zipfile
from collections import defaultdict
from collections.abc import Generator, Iterable
from csv import DictWriter
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Self

import boto3
from botocore.exceptions import ClientError

athena = boto3.client("athena")
sts = boto3.client("sts")
ses = boto3.client("sesv2")


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
    potential_savings_per_month: int
    update_result: str | None = None
    updated: bool = False

    @classmethod
    def from_dict(cls, dct: dict[str, str]) -> Self:
        return cls(
            region=dct["region"],
            account_id=dct["account_id"],
            table_name=dct["table_name"],
            recommendation=dct["recommendation"],
            potential_savings_per_month=int(dct["potential_savings_per_month"]),
        )

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
                yield QueryResultData.from_dict(dict(zip(keys, values)))


def update_tables_in_account(
    key: AccountRegionPair,
    changes: Iterable[QueryResultData],
    is_dry_run: bool,
):
    dynamodb_client = get_dynamodb_client(
        account_id=key.account_id, region_name=key.region
    )
    for change in changes:
        # The Athena query result returns recommendation "Candiate for Standard"
        # or "Candidate for Standard_IA" but the API is expecting either "STANDARD"
        # or "STANDARD_INFREQUENT_ACCESS" so we get the last word, uppercase it,
        # and switch _IA to _INFREQUENT_ACCESS
        change.recommendation = (
            change.recommendation.split(" ")[-1]
            .upper()
            .replace("_IA", "_INFREQUENT_ACCESS")
        )
        assert change.recommendation in ("STANDARD", "STANDARD_INFREQUENT_ACCESS")
        print(
            f"Updating table {change.table_name} to storage class {change.recommendation}"
        )
        try:
            current_class = dynamodb_client.describe_table(TableName=change.table_name)
            if (
                "TableClassSummary" in current_class["Table"]
                and "TableClass" in current_class["Table"]["TableClassSummary"]
                and current_class["Table"]["TableClassSummary"]["TableClass"]
                == change.recommendation
            ):
                change.update_result = (
                    "Table Class already optimized. Not performing any change."
                )
            elif is_dry_run:
                change.update_result = "Dry Run - Did not update"
            else:
                update_result = dynamodb_client.update_table(
                    TableName=change.table_name,
                    TableClass=change.recommendation,
                )
                change.updated = True
                assert "TableDescription" in update_result
                assert "TableStatus" in update_result["TableDescription"]
                change.update_result = update_result["TableDescription"]["TableStatus"]
        except ClientError as error:
            assert "Error" in error.response
            assert "Message" in error.response["Error"]
            change.update_result = error.response["Error"]["Message"]
        yield change


def main(query_id: str, is_dry_run: bool) -> Generator[QueryResultData, None, None]:
    tables_per_account_and_region: defaultdict[
        AccountRegionPair, list[QueryResultData]
    ] = defaultdict(list)
    for result in get_query_results(query_id):
        tables_per_account_and_region[result.as_account_and_region].append(result)
    for key, changes in tables_per_account_and_region.items():
        yield from update_tables_in_account(key, changes, is_dry_run)


def publish_results(result_data: Iterable[QueryResultData], dry_run: bool) -> None:
    sender: str = os.environ["SENDER_ADDRESS"]
    recipients_str: str = os.environ["RECIPIENTS"]
    recipients_list: list[str] = recipients_str.split(",")
    SUBJECT = "[Action Required] DynamoDB Table Class Optimizer Report"
    # The character encoding for the email.
    CHARSET = "utf-8"
    msg = MIMEMultipart("mixed")
    # Add subject, from and to lines.
    msg["Subject"] = SUBJECT
    msg["From"] = sender
    msg["To"] = recipients_str

    csv_name = (
        f"DDB_Table_Classs_Report_{datetime.now().isoformat(timespec="seconds")}.csv"
    )
    output = io.StringIO()
    my_fields = [f.name for f in fields(QueryResultData)]
    writer = DictWriter(output, fieldnames=my_fields)
    writer.writeheader()
    sum_savings = 0
    recommendation_count = 0
    for data in result_data:
        sum_savings += data.potential_savings_per_month
        recommendation_count += 1
        writer.writerow(asdict(data))
    # Define the attachment part and encode it using MIMEApplication.
    csv_str: str = output.getvalue()
    bytes_io = io.BytesIO()
    with zipfile.ZipFile(bytes_io, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(csv_name, csv_str)
    att = MIMEApplication(bytes_io.getvalue())

    # Add a header to tell the email client to treat this part as an attachment,
    # and to give the attachment a name.
    att.add_header(
        "Content-Disposition",
        "attachment",
        filename=f"{csv_name}.zip",
    )

    BODY_TEXT = f"The DynamoDB Table Class Optimizer has found {recommendation_count:,} DynamoDB tables whose Table Class can potentially be optimized.\nThe calculated potential savings is *${sum_savings:,}*.\nAttached to this email is a CSV of recommended modifications."

    CONTENT_HTML = (
        "<p>"
        + "</p><p>".join(
            BODY_TEXT.replace(
                "*",
                "<b>",
                count=1,
            )
            .replace(
                "*",
                "</b>",
            )
            .split("\n")
        )
        + "</p>"
    )
    # The HTML body of the email.
    BODY_HTML = f"""
    <html>
    <head/>
    <body>
    {CONTENT_HTML}
    </body>
    </html>
    """

    # Create a multipart/alternative child container.
    msg_body = MIMEMultipart("alternative")

    # Encode the text and HTML content and set the character encoding. This step is
    # necessary if you're sending a message with characters outside the ASCII range.
    textpart = MIMEText(BODY_TEXT, "plain", CHARSET)
    htmlpart = MIMEText(BODY_HTML, "html", CHARSET)

    # Add the text and HTML parts to the child container.
    msg_body.attach(textpart)
    msg_body.attach(htmlpart)

    # Attach the multipart/alternative child container to the multipart/mixed
    # parent container.
    msg.attach(msg_body)
    msg.attach(att)

    # changes start from here
    strmsg = str(msg)
    body = bytes(strmsg, "utf-8")

    response = ses.send_email(
        FromEmailAddress=sender,
        Destination={
            "ToAddresses": recipients_list,
        },
        Content={"Raw": {"Data": body}},
    )
    print(response)


def lambda_handler(event: dict, _) -> None:
    assert isinstance((query_id := event.get("QueryExecutionId")), str)
    is_dry_run: bool = (
        passed_value
        if "IsDryRun" in event
        and (passed_value := event["IsDryRun"])
        and isinstance(passed_value, bool)
        else False
    )
    result_data: Iterable[QueryResultData] = main(query_id, is_dry_run)
    publish_results(result_data, is_dry_run)
