import os
import shutil
import csv
import time
import datetime
import collections

from lxml import etree
import requests
import exchangerates
import iatikit

from iatiflattener.lib import variables
from iatiflattener import model
from iatiflattener.data_quality import report as data_quality_report

EXCHANGE_RATES_URL = "https://codeforiati.org/imf-exchangerates/imf_exchangerates.csv"
COUNTRIES_CURRENCIES_URL = "https://codeforiati.org/imf-exchangerates/currencies.json"
EXCLUDED_PUBLISHERS=["aiddata"]
CODELIST_URL_LANG = "https://codelists.codeforiati.org/api/json/{}/{}.json"
CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/{}.json"


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
        country_req = requests.get(CODELIST_URL.format("Country"))
        region_req = requests.get(CODELIST_URL.format("Region"))
        sector_req = requests.get(CODELIST_URL.format("Sector"))
        sector_groups_req = requests.get(CODELIST_URL.format("SectorGroup"))

        self.countries = list(map(lambda country: country['code'], country_req.json()["data"]))
        self.regions = list(map(lambda region: region['code'], region_req.json()["data"]))
        self.countries += self.regions
        self.category_group = dict(map(lambda code: (code['codeforiati:category-code'], code['codeforiati:group-code']), sector_groups_req.json()['data']))

        self.organisations = collections.defaultdict()
        for lang in self.langs:
            publishers_req = requests.get(CODELIST_URL_LANG.format(lang, "ReportingOrganisation"))
            self.organisations[lang] = dict(map(lambda org: (org['code'], org['name']), publishers_req.json()['data']))

        self.exchange_rates = get_exchange_rates(refresh_rates)

        countries_currencies_req = requests.get(COUNTRIES_CURRENCIES_URL)
        self.countries_currencies = countries_currencies_req.json()


    def setup_countries(self):
        for country in self.countries:
            with open('output/csv/transaction-{}.csv'.format(country), 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(self.csv_headers)
            with open('output/csv/budget-{}.csv'.format(country), 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(self.csv_headers)


    def setup_organisations(self):
        for organisation in self.organisations['en'].keys():
            with open('output/csv/activities/{}.csv'.format(organisation.replace("/", "_")), 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(self.activity_csv_headers)


    def process_transaction(self, csvwriter, activity, transaction):
        _transaction = model.Transaction(activity, transaction, self.activity_cache,
            self.exchange_rates, self.countries_currencies, True, self.organisations, self.langs)
        generated = _transaction.generate()
        if generated is not False:
            _flat_transaction = model.FlatTransaction(_transaction, self.category_group).flatten()
            for _part_flat_transaction in _flat_transaction:
                transaction_csv = model.FlatTransactionBudgetCSV(
                countries=self.countries,
                csv_writer=csvwriter,
                flat_transaction_budget=_part_flat_transaction).output()


    def process_activity_for_budgets(self, csvwriter, activity):
        _budget = model.ActivityBudget(activity, self.activity_cache,
            self.exchange_rates, self.countries_currencies, self.organisations, self.langs)
        generated = _budget.generate()
        if generated is not False:
            _flat_budget = model.FlatBudget(_budget, self.category_group).flatten()
        for _part_flat_budget in _flat_budget:
            transaction_csv = model.FlatTransactionBudgetCSV(
            countries=self.countries,
            csv_writer=csvwriter,
            flat_transaction_budget=_part_flat_budget).output()


    def process_activity(self, csvwriter, activity):
        _activity = model.Activity(activity, self.activity_cache,
            self.organisations, self.langs)
        generated = _activity.generate()
        model.ActivityCSV(
            organisations = self.organisations['en'].keys(),
            csv_writer=csvwriter,
            activity_data=_activity.as_csv_dict()).output()


    def process_package(self, publisher, package):

        doc = etree.parse(os.path.join(self.iatikitcache_dir, "data", "{}".format(publisher), "{}".format(package)))
        if doc.getroot().get("version") not in ['2.01', '2.02', '2.03']: return
        self.activity_cache = model.ActivityCache()

        activity_csvwriter = model.ActivityCSVFilesWriter(
            self.output_dir,
            headers=self.activity_csv_headers)
        activities = doc.xpath("//iati-activity")
        for activity in activities:
            self.process_activity(activity_csvwriter, activity)

        activity_csvwriter.write()

        csvwriter = model.CSVFilesWriter(budget_transaction='transaction',
            headers=self.csv_headers)
        transactions = doc.xpath("//transaction")
        for transaction in transactions:
            self.process_transaction(csvwriter, transaction.getparent(), transaction)

        csvwriter.write()

        csvwriter = model.CSVFilesWriter(budget_transaction='budget',
            headers=self.csv_headers)
        activities = doc.xpath("//iati-activity[budget]")
        for activity in activities:
            self.process_activity_for_budgets(csvwriter, activity)

        csvwriter.write()


    def run_for_publishers(self):
        print("BEGINNING PROCESS AT {}".format(datetime.datetime.utcnow()))
        beginning = time.time()
        for publisher in self.publishers:
            if publisher in EXCLUDED_PUBLISHERS: continue
            start = time.time()
            try:
                print("Processing {}".format(publisher))
                packages = os.listdir(os.path.join(self.iatikitcache_dir, "data", "{}".format(publisher)))
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
            iatikitcache_dir=os.path.join("__iatikitcache__", "registry"),
            output='output',
            publishers=None,
            langs=['en', 'fr']):
        self.iatikitcache_dir = iatikitcache_dir
        self.langs = langs
        self.csv_headers = variables.headers(langs)
        self.activity_csv_headers = variables.activity_headers(langs)
        self.output_dir = output
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, 'csv', 'activities'), exist_ok=True)
        if publishers is None:
            self.publishers = os.listdir(os.path.join(self.iatikitcache_dir, "data"))
        else:
            self.publishers = publishers
        self.publishers.sort()
        print("Setting up codelists...")
        self.setup_codelists(refresh_rates=refresh_rates)
        print("Setting up countries...")
        self.setup_countries()
        print("Setting up organisations...")
        self.setup_organisations()
        print("Processing publishers...")
        self.run_for_publishers()
