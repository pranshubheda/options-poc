from nsepython import option_chain
from nsepython import nse_eq
import pandas as pd
from google.cloud import bigquery
import os
from zoneinfo import ZoneInfo
from datetime import datetime
import time

while True:

    STOCK = "RELIANCE"
    option_chain_dict = option_chain(STOCK)
    option_items = option_chain_dict["filtered"]["data"]
    equity_dict = nse_eq(STOCK)

    rows_to_insert = []
    headers = [
        "Symbol",
        "CompanyName",
        "Industry",
        "LastPrice",
        "Week-Low",
        "Week-High",
        "Open",
        "IntraDay-Low",
        "IntraDay-High",
        "OptionType",
        "StrikePrice",
        "ExpiryDate",
        "AskPrice",
        "Bidprice",
        "ChangeinOpenInterest",
        "TotalTradedVolume",
        "Date"
    ]
    dtobj = datetime.now(tz=ZoneInfo('Asia/Kolkata')).strftime("%d/%m/%Y, %H:%M:%S")

    for oi in option_items:
        rows_to_insert.append(
            [
                equity_dict["info"]["symbol"],
                equity_dict["info"]["companyName"],
                equity_dict["info"]["industry"],
                equity_dict["priceInfo"]["lastPrice"],
                equity_dict["priceInfo"]["weekHighLow"]["min"],
                equity_dict["priceInfo"]["weekHighLow"]["max"],
                equity_dict["priceInfo"]["open"],
                equity_dict["priceInfo"]["intraDayHighLow"]["min"],
                equity_dict["priceInfo"]["intraDayHighLow"]["max"],
                "CE",
                oi["strikePrice"],
                oi["expiryDate"],
                oi["CE"]["askPrice"],
                oi["CE"]["bidprice"],
                oi["CE"]["changeinOpenInterest"],
                oi["CE"]["totalTradedVolume"],
                dtobj
            ]
        )
        rows_to_insert.append(
            [
                equity_dict["info"]["symbol"],
                equity_dict["info"]["companyName"],
                equity_dict["info"]["industry"],
                equity_dict["priceInfo"]["lastPrice"],
                equity_dict["priceInfo"]["weekHighLow"]["min"],
                equity_dict["priceInfo"]["weekHighLow"]["max"],
                equity_dict["priceInfo"]["open"],
                equity_dict["priceInfo"]["intraDayHighLow"]["min"],
                equity_dict["priceInfo"]["intraDayHighLow"]["max"],
                "PE",
                oi["strikePrice"],
                oi["expiryDate"],
                oi["PE"]["askPrice"],
                oi["PE"]["bidprice"],
                oi["PE"]["changeinOpenInterest"],
                oi["PE"]["totalTradedVolume"],
                dtobj
            ]
        )
        
    credentials_path = 'stocks-poc-key.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    client = bigquery.Client()
    table_id = 'stocks-poc.options.options-chain-v1'

    df = pd.DataFrame(rows_to_insert)
    df.columns = headers

    schema=[
                bigquery.SchemaField("Symbol", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("CompanyName", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("Industry", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("LastPrice", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("Week-Low", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("Week-High", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("Open", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("IntraDay-Low", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("IntraDay-High", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("OptionType", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("StrikePrice", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("ExpiryDate", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("AskPrice", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("Bidprice", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("ChangeinOpenInterest", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("TotalTradedVolume", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("Date", bigquery.enums.SqlTypeNames.STRING)
            ]
    errors = client.insert_rows_from_dataframe(table_id, df, schema)
    if errors == [[]]:
        print('New rows have been added.')
    else:
        print(f'Encountered errors while inserting rows: {errors}')

    time.sleep(50)
