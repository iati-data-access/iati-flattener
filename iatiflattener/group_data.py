import requests
import pandas as pd
import numpy as np
from pyexcelerate import Workbook
import json
import re
import time
import datetime
import os
import math

from iatiflattener.lib import variables

CODELISTS_URL = "https://codelists.codeforiati.org/api/json/{}/{}.json"

class GroupFlatIATIData():
    def get_codelist_with_fallback(self, lang, codelist_name):
        req = requests.get(CODELISTS_URL.format(lang, codelist_name))
        if req.status_code == 404:
            req = requests.get(CODELISTS_URL.format('en', codelist_name))
        return req

    def setup_codelists(self):
        self.country_names, self.column_codelist = {}, {}
        for lang in self.langs:
            country_req = self.get_codelist_with_fallback(lang, "Country")
            region_req = self.get_codelist_with_fallback(lang, "Region")
            sector_req = self.get_codelist_with_fallback(lang, "Sector")
            sector_groups_req = self.get_codelist_with_fallback(lang, "SectorGroup")

            country_names = dict(map(lambda country: (country['code'], country['name']), country_req.json()["data"]))
            region_names = dict(map(lambda region: (region['code'], region['name']), region_req.json()["data"]))
            country_names.update(region_names)
            self.country_names[lang] = country_names
            sector_names = dict(map(lambda country: (country['code'], country['name']), sector_req.json()["data"]))

            required_codelists = {
                'reporting_org_type': 'OrganisationType',
                'aid_type': 'AidType',
                'finance_type': 'FinanceType',
                'flow_type': 'FlowType',
                'transaction_type': 'TransactionType',
                'sector_code': 'Sector',
                'provider_org_type': 'OrganisationType',
                'receiver_org_type': 'OrganisationType'
            }
            self.column_codelist[lang] = {}
            for _cl in required_codelists.items():
                req = self.get_codelist_with_fallback(lang, _cl[1])
                self.column_codelist[lang][_cl[0]] = dict(map(lambda item: (item['code'], item['name']), req.json()["data"]))
            self.column_codelist[lang]['transaction_type']['budget'] = 'Budget'
            sector_sector_categories = dict(map(lambda code: (code['codeforiati:group-code'], code['codeforiati:group-name']), sector_groups_req.json()['data']))
            self.column_codelist[lang]['sector_category'] = sector_sector_categories
            self.column_codelist[lang]['country_code'] = self.country_names[lang]

    def make_conditions_outputs(self, codelist, dataframe):
        cl_key = codelist[0]
        cl_items = codelist[1]
        conditions = list(map(lambda cl_item: dataframe[cl_key] == cl_item, cl_items.keys()))
        outputs = list(map(lambda cl_item: "{} - {}".format(cl_item[0], cl_item[1]), cl_items.items()))
        return conditions, outputs


    def relabel_dataframe(self, dataframe, lang):
        """
        Fast way to add new column based on existing column.
        https://stackoverflow.com/a/53505512/11841218
        """
        for codelist in self.column_codelist[lang].items():
            cl_key = codelist[0]
            cl_items = codelist[1]
            conditions, outputs = self.make_conditions_outputs(codelist, dataframe)
            no_data = variables.TRANSLATIONS.get(lang).get('no-data')
            res = np.select(conditions, outputs, no_data)
            dataframe[cl_key] = pd.Series(res)
        return dataframe


    def write_dataframe_to_excel(self, dataframe, filename, lang):
        """
        Function to write a pandas dataframe to Excel.
        This uses pyExcelerate, which is about 2x as fast as the built-in
        pandas library.
        """
        wb = Workbook()
        headers = variables.OUTPUT_HEADERS.get(lang)
        data = dataframe.values.tolist()
        data.insert(0, headers)
        ws = wb.new_sheet("Data", data=data)
        wb.save(filename)


    def get_dataframe(self, country_code, transaction_budget, lang):
        full_df = pd.DataFrame()
        print("Read CSV {}-{}.csv".format(transaction_budget, country_code))
        for df in pd.read_csv("output/csv/{}-{}.csv".format(transaction_budget, country_code),
            dtype=self.CSV_HEADER_DTYPES,
            chunksize=100000):
            print("Reading chunk...")
            if (not "iati_identifier" in df.columns.values) or (len(df)==0):
                print("df length is 0")
                return
            headers_with_langs = variables.group_by_headers_with_langs([lang])
            all_relevant_headers = headers_with_langs + ['value_usd', 'value_eur', 'value_local']
            df = df[all_relevant_headers]
            no_data = variables.TRANSLATIONS.get(lang).get('no-data')
            df = df.fillna(no_data)
            df = df.groupby(headers_with_langs)
            df = df.agg({'value_usd':'sum','value_eur':'sum','value_local':'sum'})
            df = df.reset_index().fillna(no_data)
            df = self.relabel_dataframe(df, lang)
            full_df = pd.concat([full_df, df])

        return full_df


    def group_results(self, country_code):
        for lang in self.langs:
            df_transaction = self.get_dataframe(country_code, 'transaction', lang)
            df_budget = self.get_dataframe(country_code, 'budget', lang)
            if (df_transaction is None) and (df_budget is None):
                continue
            elif (df_transaction is not None) and (df_budget is not None):
                df = pd.concat([df_transaction, df_budget], ignore_index=True)
            else:
                if df_transaction is not None:
                    df = df_transaction
                else:
                    df = df_budget

            if df is not None:
                num_rows = len(df)
                output_rows = 500000
                if num_rows > output_rows:
                    for start in range(0, num_rows, output_rows):
                        df_part = df.iloc[start:start+output_rows, :]
                        page = (start/output_rows)+1
                        self.write_dataframe_to_excel(
                            dataframe = df_part,
                            filename = "output/xlsx/{}/{}-{}.xlsx".format(lang, country_code, page),
                            lang = lang)
                else:
                    self.write_dataframe_to_excel(
                        dataframe = df,
                        filename = "output/xlsx/{}/{}.xlsx".format(lang, country_code),
                        lang = lang)


    def group_data(self):
        for lang in self.langs:
            os.makedirs('output/xlsx/{}/'.format(lang), exist_ok=True)
        csv_files = os.listdir("output/csv/")
        csv_files.sort()
        print("BEGINNING PROCESS AT {}".format(datetime.datetime.utcnow()))
        list_of_files = []
        for country_code, country_name in sorted(self.country_names[lang].items()):
            start = time.time()
            country_or_region = {True: 'region', False: 'country'}[re.match('^\d*$', country_code) is not None]
            self.group_results(country_code)
            list_of_files.append({
                'country_code': country_code,
                'country_name': country_name,
                'country_or_region': country_or_region,
                'filename': "{}.xlsx".format(country_code)
            })
            end = time.time()
            print("Processing {} took {}s".format(country_code, end-start))
        for lang in self.langs:
            with open('output/xlsx/{}/index.json'.format(lang), 'w') as json_file:
                countries = [{
                    'country_code': country_code,
                    'country_name': country_name,
                    'country_or_region': country_or_region,
                    'filename': "{}.xlsx".format(country_code)
                } for country_code, country_name in self.country_names[lang].items()]
                json.dump({
                    'lastUpdated': datetime.datetime.utcnow().date().isoformat(),
                    'countries': countries,
                    'langs': self.langs
                }, json_file)
        print("FINISHED PROCESS AT {}".format(datetime.datetime.utcnow()))

    def __init__(self, langs=['en']):
        self.langs = langs
        self.CSV_HEADERS = variables.headers(langs)
        self._DTYPES = variables.dtypes(langs)
        self.CSV_HEADER_DTYPES = dict(map(lambda csv_header: (csv_header[1], self._DTYPES[csv_header[0]]), enumerate(self.CSV_HEADERS)))
        self.HEADERS_WITH_LANGS = variables.headers_with_langs(langs)
        self.setup_codelists()
        self.group_data()
