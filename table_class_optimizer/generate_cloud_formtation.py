from textwrap import indent


def my_indenter(num: int, text: str) -> str:
    return indent(text, " " * num)[num:]


def replace_query_parameters(query: str) -> str:
    return (
        query.replace(
            "ARRAY['ALL'] AS account_ids", "ARRAY[${AccountIds}] AS account_ids"
        )
        .replace("ARRAY['ALL'] AS payer_ids", "ARRAY[${PayerIds}] AS payer_ids")
        .replace("ARRAY['ALL'] AS table_names", "ARRAY[${TableNames}] AS table_names")
        .replace(
            "ARRAY['ALL'] AS region_names", "ARRAY[${RegionNames}] AS region_names"
        )
        .replace(
            "50 AS min_savings_per_month", "${MinimumSavings} AS min_savings_per_month"
        )
        .replace("'NET' AS cost_type", "'${PricingTerms}' AS cost_type")
        .replace("[CUR_DB]", "${AthenaCURDatabase}")
        .replace("[CUR_TABLE]", "${AthenaCURTable}")
    )


def main():
    with open("./DDB_TableClassReco.sql", "r") as sql, open(
        "./lambda_handler.py", "r"
    ) as lambda_handler, open("./raw_template.yaml", "r") as stack_template, open(
        "./template.yaml", "w"
    ) as output:
        result: str = (
            stack_template.read()
            .replace(
                "{{ athena_query_string }}",
                my_indenter(10, replace_query_parameters(sql.read())),
            )
            .replace(
                "{{ lambda_handler_code }}", my_indenter(10, lambda_handler.read())
            )
        )
        output.write(result)


if __name__ == "__main__":
    main()
