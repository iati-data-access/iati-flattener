from lxml import etree
import os
import shutil
import requests
import csv
import time
import datetime
import exchangerates
import iatikit

from iatiflattener.lib.variables import CSV_HEADERS, GROUP_BY_HEADERS, OUTPUT_HEADERS, _DTYPES

from iatiflattener.data_quality import report as data_quality_report
from iatiflattener.transaction import FlatIATITransaction
from iatiflattener.budget import FlatIATIBudget

CSV_HEADER_DTYPES = dict(map(lambda csv_header: (csv_header[1], _DTYPES[csv_header[0]]), enumerate(CSV_HEADERS)))

EXCHANGE_RATES_URL = "https://codeforiati.org/imf-exchangerates/imf_exchangerates.csv"
EXCLUDED_PUBLISHERS=["aiddata"]
REGIONS_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/Region.json"
COUNTRIES_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/Country.json"
SECTORS_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/Sector.json"
M49_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/RegionM49.json"
SECTOR_GROUPS_URL = "https://codelists.codeforiati.org/api/json/en/SectorGroup.json"
PUBLISHER_NAMES_URL = "https://codelists.codeforiati.org/api/json/en/ReportingOrganisation.json"
IATI_DUMP_DIR = os.path.join("__iatikitcache__", "registry")


# Exchange rates
def get_exchange_rates(get_rates=True):
    if get_rates:
        print("Getting exchange rates data")
        r_rates = requests.get(EXCHANGE_RATES_URL, stream=True)
        with open("rates.csv", 'wb') as fd:
            for chunk in r_rates.iter_content(chunk_size=128):
                fd.write(chunk)
        print("Reading in exchange rates data")
    return exchangerates.CurrencyConverter(
    update=False, source="rates.csv")


class FlattenIATIData():

    def setup_codelists(self, refresh_rates):
        self.activity_data = {}
        country_req = requests.get(COUNTRIES_CODELIST_URL)
        region_req = requests.get(REGIONS_CODELIST_URL)
        sector_req = requests.get(SECTORS_CODELIST_URL)
        sector_groups_req = requests.get(SECTOR_GROUPS_URL)

        self.countries = list(map(lambda country: country['code'], country_req.json()["data"]))
        self.regions = list(map(lambda region: region['code'], region_req.json()["data"]))
        self.countries += self.regions
        self.category_group = dict(map(lambda code: (code['codeforiati:category-code'], code['codeforiati:group-code']), sector_groups_req.json()['data']))

        publishers_req = requests.get(PUBLISHER_NAMES_URL)
        self.organisations = dict(map(lambda org: (org['code'], org['name']), publishers_req.json()['data']))

        self.exchange_rates = get_exchange_rates(refresh_rates)


    def setup_countries(self):
        for country in self.countries:
            with open('output/csv/{}.csv'.format(country), 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(CSV_HEADERS)
            with open('output/csv/budget-{}.csv'.format(country), 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(CSV_HEADERS)


    def process_transaction(self, activity, transaction):
        t = FlatIATITransaction(self, activity, transaction)
        if t.output == True:
            t.output_transaction()


    def process_activity_for_budgets(self, activity):
        b = FlatIATIBudget(self, activity)
        if b.output == True:
            b.output_budget()


    def write_csv_files(self):
        for _file in self.csv_files_transactions.values():
            _file['csv'].writerows(_file['rows'])
            _file['file'].close()
        for _file in self.csv_files_budgets.values():
            _file['csv'].writerows(_file['rows'])
            _file['file'].close()


    def process_package(self, publisher, package):
        self.csv_files_transactions = {}
        self.csv_files_budgets = {}
        doc = etree.parse(os.path.join(IATI_DUMP_DIR, "data", "{}".format(publisher), "{}".format(package)))
        if doc.getroot().get("version") not in ['2.01', '2.02', '2.03']: return
        self.activity_data = {}

        transactions = doc.xpath("//transaction")
        for transaction in transactions:
            self.process_transaction(transaction.getparent(), transaction)

        activities = doc.xpath("//iati-activity[budget]")
        for activity in activities:
            self.process_activity_for_budgets(activity)

        self.write_csv_files()


    def run_for_publishers(self):
        print("BEGINNING PROCESS AT {}".format(datetime.datetime.utcnow()))
        beginning = time.time()
        for publisher in self.publishers:
            if publisher in EXCLUDED_PUBLISHERS: continue
            start = time.time()
            try:
                print("Processing {}".format(publisher))
                packages = os.listdir(os.path.join(IATI_DUMP_DIR, "data", "{}".format(publisher)))
                packages.sort()
                for package in packages:
                    try:
                        if package.endswith(".xml"):
                            self.process_package(publisher, package)
                    except Exception as e:
                        print("Exception with package {}".format(package))
                        print("Exception was {}".format(e))
                        continue
            except NotADirectoryError:
                continue
            end = time.time()
            print("Processing {} took {}s".format(publisher, end-start))
        print("FINISHED PROCESS AT {}".format(datetime.datetime.utcnow()))
        finishing = time.time()
        print("PROCESSING TOOK {}".format(finishing-beginning))


    def __init__(self,
            refresh_rates=False,
            output='output',
            publishers=None):
        self.output_dir = output
        os.makedirs(self.output_dir, exist_ok=True)
        if publishers is None:
            self.publishers = os.listdir(os.path.join(IATI_DUMP_DIR, "data"))
        else:
            self.publishers = publishers
        self.publishers.sort()
        self.setup_codelists(refresh_rates=refresh_rates)
        self.setup_countries()
        self.run_for_publishers()
