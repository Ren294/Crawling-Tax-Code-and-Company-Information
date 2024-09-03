from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import os
import pendulum
import json

URL = "https://doanhnghiep.biz"
home = "/opt/airflow/data"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

with open(f"{home}/city.json", "r") as f:
    city = json.load(f)

item_list = list(
    [
        "taxID",
        "alternateName",
        "StartDate",
        "Owner",
        "address",
        "Phone",
        "BusinessClass",
        "BusinessLine",
        "Status",
    ]
)
key_list = [
    "TaxCode",
    "Name",
    "TradingName",
    "OperationDate",
    "Representation",
    "Address",
    "PhoneNumber",
    "Type",
    "Profession",
    "Status",
    "Province",
]


def check_and_apply(data, content, item, data_item):
    if data_item == "Representation":
        st = content.find("span", itemprop=item)
    else:
        st = content.find("td", itemprop=item)
    if st is not None:
        try:
            data[data_item].append(st.string.strip())
        except:
            with open(f"{home}/error/error.txt", "w") as file:
                file.write(str(data_item) + " : " + str(content))
            data[data_item].append("N/A")
    else:
        data[data_item].append("N/A")


def output_f(data, start, i):
    if i > 0 and i % 300 == 0:
        pd.DataFrame(data).to_excel(
            f"{home}/data_crawling/{start}-to-{i}-row.xlsx", index=False
        )


def update_page(index):
    for ci in city:
        if ci["page"] * 30 > index:
            ci["start"] = index // 30 + 1
            break
        else:
            ci["start"] = ci["page"]
            index -= ci["page"] * 30


def get_max():
    file_con = os.listdir(f"{home}/data_concat/")
    if len(file_con) == 0:
        return 0
    file_recent = [int(x.split("-")[0])
                   for x in file_con if len(x.split("-")) == 2]
    return max(file_recent)


def reset_data():
    return {
        "TaxCode": [],
        "Name": [],
        "TradingName": [],
        "OperationDate": [],
        "Representation": [],
        "Address": [],
        "PhoneNumber": [],
        "Type": [],
        "Profession": [],
        "Status": [],
        "Province": [],
    }


def main_default():
    Index = get_max()
    IndexS = Index
    update_page(Index)
    data = reset_data()
    for city_page in city:
        if city_page["page"] == city_page["start"]:
            continue
        for page in range(city_page["start"], city_page["page"] + 1):
            list_page_href = BeautifulSoup(
                requests.get(
                    f"{URL}{city_page['href']}/?p={page}", headers=headers
                ).content,
                "html.parser",
            ).find_all("h6")
            for firm_tag in list_page_href:
                firm_content = BeautifulSoup(
                    requests.get(
                        f"{URL}{firm_tag.a.attrs['href']}", headers=headers
                    ).content,
                    "html.parser",
                )
                # ad Name
                if firm_content.find("th") is not None:
                    data["Name"].append(firm_content.find("th").string)
                else:
                    data["Name"].append("N/A")
                for i, a in zip(key_list[0:1] + key_list[2:-1], item_list):
                    check_and_apply(data, firm_content, a, i)
                data["Province"].append(city_page["name"])
                Index = Index + 1
                if Index > IndexS + 300:
                    return
                output_f(data, IndexS, Index)


def concat_file_data():
    last_index = get_max()
    file_craw = os.listdir(f"{home}/data_crawling/")
    file = [int(x.split("-")[2]) for x in file_craw if len(x.split("-")) == 4]
    while last_index < max(file):
        file_search = [
            x
            for x in file_craw
            if len(x.split("-")) == 4 and int(x.split("-")[0]) == last_index
        ]
        index_dest = max(
            [int(x.split("-")[2])
             for x in file_search if len(x.split("-")) == 4]
        )
        pd.concat(
            [
                pd.read_excel(
                    f"{home}/data_concat/{last_index}-row.xlsx", index_col=None),
                pd.read_excel(
                    f"{home}/data_crawling/{last_index}-to-{index_dest}-row.xlsx", index_col=None
                ),
            ]
        ).to_excel(f"{home}/data_concat/{index_dest}-row.xlsx", index=False)
        last_index = index_dest


local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 22, 20, 0, 0, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


with DAG('Crawling_TaxCode_1',
         default_args=default_args,
         description='Crawling form web',
         schedule_interval=timedelta(hours=1),
         catchup=False) as dag:

    crawling_data = PythonOperator(
        task_id='crawling_data_id',
        python_callable=main_default
    )

    concat_file = PythonOperator(
        task_id='concat_file_id',
        python_callable=concat_file_data
    )

crawling_data >> concat_file
