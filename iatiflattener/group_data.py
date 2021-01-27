import requests
import pandas as pd
import numpy as np
from pyexcelerate import Workbook
import json
import re
import time
import datetime
import os

from iatiflattener.lib.variables import CSV_HEADERS, GROUP_BY_HEADERS, OUTPUT_HEADERS, _DTYPES

CSV_HEADER_DTYPES = dict(map(lambda csv_header: (csv_header[1], _DTYPES[csv_header[0]]), enumerate(CSV_HEADERS)))

REGIONS_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/Region.json"
COUNTRIES_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/Country.json"
SECTORS_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/Sector.json"
M49_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/RegionM49.json"
SECTOR_GROUPS_URL = "https://codelists.codeforiati.org/api/json/en/SectorGroup.json"

class GroupFlatIATIData():
    def setup_codelists(self):
        country_req = requests.get(COUNTRIES_CODELIST_URL)
        region_req = requests.get(REGIONS_CODELIST_URL)
        sector_req = requests.get(SECTORS_CODELIST_URL)
        sector_groups_req = requests.get(SECTOR_GROUPS_URL)
        self.sector_groups = dict(map(lambda code: (code['codeforiati:group-code'], code['codeforiati:group-name']), sector_groups_req.json()['data']))

        self.country_names = dict(map(lambda country: (country['code'], country['name']), country_req.json()["data"]))
        self.region_names = dict(map(lambda region: (region['code'], region['name']), region_req.json()["data"]))
        self.country_names.update(self.region_names)
        self.sector_names = dict(map(lambda country: (country['code'], country['name']), sector_req.json()["data"]))

        required_codelists = {
            'reporting_org_type': 'OrganisationType',
            'aid_type': 'AidType',
            'finance_type': 'FinanceType',
            'transaction_type': 'TransactionType',
            'sector_code': 'Sector',
            'provider_org_type': 'OrganisationType',
            'receiver_org_type': 'OrganisationType'
        }
        self.column_codelist = {}
        generic_codelists_url = "https://codelists.codeforiati.org/api/json/en/{}.json"
        for _cl in required_codelists.items():
            req = requests.get(generic_codelists_url.format(_cl[1]))
            self.column_codelist[_cl[0]] = dict(map(lambda item: (item['code'], item['name']), req.json()["data"]))
        self.column_codelist['transaction_type']['budget'] = 'Budget'
        self.column_codelist['sector_category'] = self.sector_groups
        self.column_codelist['country_code'] = self.country_names

    def make_conditions_outputs(self, codelist, dataframe):
        cl_key = codelist[0]
        cl_items = codelist[1]
        conditions = list(map(lambda cl_item: dataframe[cl_key] == cl_item, cl_items.keys()))
        outputs = list(map(lambda cl_item: "{} - {}".format(cl_item[0], cl_item[1]), cl_items.items()))
        return conditions, outputs


    def relabel_dataframe(self, dataframe):
        """
        Fast way to add new column based on existing column.
        https://stackoverflow.com/a/53505512/11841218
        """
        for codelist in self.column_codelist.items():
            cl_key = codelist[0]
            cl_items = codelist[1]
            conditions, outputs = self.make_conditions_outputs(codelist, dataframe)
            res = np.select(conditions, outputs, '')
            dataframe[cl_key] = pd.Series(res)
        return dataframe


    def write_dataframe_to_excel(self, dataframe, filename):
        """
        Function to write a pandas dataframe to Excel.
        This uses pyExcelerate, which is about 2x as fast as the built-in
        pandas library.
        """
        wb = Workbook()
        headers = OUTPUT_HEADERS
        data = dataframe.values.tolist()
        data.insert(0, headers)
        ws = wb.new_sheet("Data", data=data)
        wb.save(filename)


    def group_results(self, country_code):
        df = pd.read_csv("output/csv/{}.csv".format(country_code), dtype=CSV_HEADER_DTYPES)
        if (not "reporting_org" in df.columns.values) or (len(df)==0):
            return
        out = df.fillna("").groupby(GROUP_BY_HEADERS)
        out = out["value_usd"].agg("sum").reset_index().fillna("")
        out = self.relabel_dataframe(out)
        self.write_dataframe_to_excel(out, "output/xlsx/{}.xlsx".format(country_code))


    def group_data(self):
        csv_files = os.listdir("output/csv/")
        csv_files.sort()
        print("BEGINNING PROCESS AT {}".format(datetime.datetime.utcnow()))
        list_of_files = []
        for country in csv_files:
            start = time.time()
            if country.endswith(".csv"):
                country_code, _ = country.split(".")
                country_name = self.country_names.get(country_code)
                country_or_region = {True: 'region', False: 'country'}[re.match('^\d*$', country_code) is not None]
                self.group_results(country_code)
                if not country.startswith("budget-"):
                    list_of_files.append({
                        'country_code': country_code,
                        'country_name': country_name,
                        'country_or_region': country_or_region,
                        'filename': "{}.xlsx".format(country_code)
                    })
            end = time.time()
            print("Processing {} took {}s".format(country, end-start))
        with open('output/xlsx/index.json', 'w') as json_file:
            json.dump({
                'lastUpdated': datetime.datetime.utcnow().date().isoformat(),
                'countries': list_of_files
            }, json_file)
        print("FINISHED PROCESS AT {}".format(datetime.datetime.utcnow()))

    def __init__(self):
        self.setup_codelists()
        self.group_data()
