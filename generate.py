from lxml import etree
import os
import requests
import csv
import time
import datetime
import math
import exchangerates
from collections import defaultdict
import pandas as pd
import numpy as np
from pyexcelerate import Workbook
CSV_HEADERS = [
    'iati_identifier',
    'title',
    'reporting_org',
    'reporting_org_type',
    'aid_type',
    'finance_type',
    'provider_org',
    'provider_org_type',
    'receiver_org',
    'receiver_org_type',
    'transaction_type',
    'value_original', 'currency_original',
    'value_usd', 'exchange_rate_date',
    'exchange_rate', 'transaction_date',
    'country_code', 'multi_country',
    'sector_category', 'sector_code',
    'covid_19',
    'fiscal_year', 'fiscal_quarter']
GROUP_BY_HEADERS = [
   'iati_identifier',
   'title',
   'reporting_org',
   'reporting_org_type',
   'aid_type',
   'finance_type',
   'provider_org',
   'provider_org_type',
   'receiver_org',
   'receiver_org_type',
   'transaction_type',
   'country_code',
   'multi_country',
   'sector_category',
   'sector_code',
   'covid_19',
   'fiscal_year',
   'fiscal_quarter']

_DTYPES = [str, str, str, str,
                 str, str, str, str,
                 str, str, str, str,
                 str, np.float64, str, str,
                 str, str,
                 np.int32, str, str, np.int32,
                 np.int32, np.int32]

CSV_HEADER_DTYPES = dict(map(lambda csv_header: (csv_header[1], _DTYPES[csv_header[0]]), enumerate(CSV_HEADERS)))

MORPH_IO_API_KEY = os.environ['MORPH_IO_API_KEY']
EXCLUDED_PUBLISHERS=["aiddata"]
TRANSACTION_TYPES_RULES = {
    "1": {'provider': '1', 'receiver': 'reporter'},
    "2": {'provider': 'reporter', 'receiver': '4'},
    "3": {'provider': 'reporter', 'receiver': '4'},
    "4": {'provider': 'reporter', 'receiver': '4'},
    "5": {'provider': '4', 'receiver': 'reporter'},
    "6": {'provider': '4', 'receiver': 'reporter'},
    "7": {'provider': 'reporter', 'receiver': '4'},
    "8": {'provider': 'reporter', 'receiver': '4'},
    "11": {'provider': '1', 'receiver': 'reporter'},
    "12": {'provider': 'reporter', 'receiver': '4'},
    "13": {'provider': '1', 'receiver': 'reporter'}
}
REGIONS_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/Region.json"
COUNTRIES_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/Country.json"
SECTORS_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/Sector.json"
M49_CODELIST_URL = "https://codelists.codeforiati.org/api/json/en/RegionM49.json"
SECTOR_GROUPS_URL = "https://morph.io/codeforIATI/dac-sector-groups/data.json?key={}&query=select+%2A+from+%22swdata%22".format(
            MORPH_IO_API_KEY)
IATI_DUMP_DIR = os.path.join("iati-data-dump")


def get_date(_date):
    return datetime.datetime.strptime(_date, "%Y-%m-%d").date()


def get_fy_fq(_date):
    date = get_date(_date)
    return date.year, math.ceil(date.month/3)


def get_first(args, default=None):
    for arg in args:
        if arg not in [None, []]: return arg
    return default


def float_int_string(_item):
    try:
        return str(int(_item))
    except ValueError:
        return str(_item)


def get_if_exists(_from, _item):
    try:
        _item = str(int(_item))
    except ValueError:
        _item = str(_item)
    return _from.get(_item, "")


def get_covid_matches(transaction):
    transaction_descriptions = ", ".join(transaction.xpath("description/narrative/text()"))
    if ("COVID-19" in transaction_descriptions) or ("COVID 19" in transaction_descriptions) or ("EP-2020-000012-001" in transaction_descriptions): return True
    return False


# Exchange rates
def get_exchange_rates(get_rates=True):
    if get_rates:
        print("Getting exchange rates data")
        r_rates = requests.get("https://morph.io/markbrough/exchangerates-scraper/data.csv?key={}&query=select+%2A+from+%22rates%22".format(
            MORPH_IO_API_KEY), stream=True)
        with open("rates.csv", 'wb') as fd:
            for chunk in r_rates.iter_content(chunk_size=128):
                fd.write(chunk)
        print("Reading in exchange rates data")
    return exchangerates.CurrencyConverter(
    update=False, source="rates.csv")


def fix_narrative(ref, text):
    return text.strip()
    # Pass for now - consider including later
    return ORG_REFS_TEXTS.get(ref, text)


def get_narrative(container):
    narratives = container.xpath("narrative")
    if len(narratives) == 0: return ""
    if len(narratives) == 1:
        if narratives[0].text:
            return fix_narrative(container.get('ref'), narratives[0].text.strip())
        else: return ""
    def filter_lang(element):
        lang = element.get("{http://www.w3.org/XML/1998/namespace}lang")
        return lang in (None, 'en')
    filtered = list(filter(filter_lang, narratives))
    if len(filtered) == 0: return fix_narrative(container.get('ref'), narratives[0].text.strip())
    return fix_narrative(container.get('ref'), filtered[0].text.strip())


def get_org_name(ref, text=None):
    if (text == None):
        return ""
    return text
    # Pass for now - consider including laterr
    if (ref == None) or (ref.strip() == ""):
        return text
        return check_encoding(text)
    if ref in ORGANISATIONS:
        return ORGANISATIONS[ref]
    return text


def get_narrative_text(element):
    if element.find("narrative") is not None:
        return element.find("narrative").text
    #print("No narrative for {}".format(etree.tostring(element)))
    return None


def get_org(activity_data, transaction, provider=True):
    def _make_org_output(_text, _ref, _type):
        _display = ""
        if (_ref is not None) and (_ref != ""):
            _display += _ref
        if (_display != "") and (_text is not None):
            _display += " - "
        if _text is not None:
            _display += _text
        return {
            'text': get_first((_text, _ref)),
            'ref': get_first((_ref, _text)),
            'type': _type,
            'display': _display
        }

    transaction_type = transaction.find("transaction-type").get("code")
    provider_receiver = {True: 'provider', False: 'receiver'}[provider]
    if transaction.find('{}-org'.format(provider_receiver)) is not None:
        _el = transaction.find('{}-org'.format(provider_receiver))
        _text = get_org_name(
            ref=_el.get("ref"),
            text=get_narrative(_el)
        )
        _ref = _el.get("ref")
        _type = _el.get("type")
        if (_ref is not None) or (_text is not None):
            return _make_org_output(_text, _ref, _type)

    role = {
        True: TRANSACTION_TYPES_RULES[transaction_type]['provider'],
        False: TRANSACTION_TYPES_RULES[transaction_type]['receiver']}[provider]
    if ((role == "reporter")
        or (provider==True and transaction_type in ['3', '4'])
        or (provider==False and transaction_type in ['1', '11', '13'])):

        if activity_data.get('reporting_org') is None:
            _ro = transaction.getparent().find("reporting-org")
            _text = get_narrative(_ro)
            _type = _ro.get('type')
            _ref = _ro.get('ref')
            _display = "{} - {}".format(_ref, _text)
            activity_data['reporting_org'] = {
                'text': _text,
                'type': _type,
                'ref': _ref,
                'display': _display
            }
        return activity_data.get('reporting_org')

    if activity_data.get("participating_org_{}".format(role)) is None:
        activity_participating = transaction.getparent().findall("participating-org[@role='{}']".format(role))
        if len(activity_participating) == 1:
            _text = get_org_name(
                    activity_participating[0].get('ref'),
                    get_narrative_text(activity_participating[0])
                )
            _ref = activity_participating[0].get("ref")
            _type = activity_participating[0].get("type")

            activity_data["participating_org_{}".format(role)] = _make_org_output(_text, _ref, _type)
        elif len(activity_participating) > 1:
            _orgs = list(map(lambda _org: _make_org_output(
                _text=get_org_name(
                    ref=_org.get("ref"),
                    text=get_narrative(_org)
                ),
                _ref=_org.get('ref', ''),
                _type=_org.get('type', '')), activity_participating))

            _text = "; ".join([org.get('text', '') for org in _orgs])
            _ref = "; ".join([org.get('ref') for org in _orgs])
            _type = "; ".join([org.get('type') for org in _orgs])
            _display = "; ".join([org.get('display') for org in _orgs])

            activity_data["participating_org_{}".format(role)] = {
                'text': _text,
                'ref': _ref,
                'type': _type,
                'display': _display
            }

    if activity_data.get('participating_org_{}'.format(role)) is not None:
        return activity_data.get('participating_org_{}'.format(role))
    return {
        'text': "",
        'ref': "",
        'type': "",
        'display': ""
    }


def clean_sectors(items):
    items = list(filter(lambda item: item.get('percentage', 100) not in ["", "0", "0.0"], items))
    _total_pct = sum(list(map(lambda item: float(item.get('percentage', 100.0)), items)))
    _pct = 100.0
    return [{
        'percentage': float(item.get('percentage', 100))/(_total_pct/100),
        'code': item.get('code')
    } for item in items]


def clean_countries(tr_countries, tr_regions=[]):
    tr_countries = list(filter(lambda item: item.get('percentage', 100) not in ["", "0", "0.0"], tr_countries))
    _total_pct = sum(list(map(lambda item: float(item.get('percentage', 100.0)), tr_countries)))

    # We take regions only if
    #  a) they exist
    #  b) country percentages don't sum to around 100%
    #  c) country percentages aren't all 100%

    if (
        (len(tr_regions) > 0) and
        (round(_total_pct) != 100) and
        ((_total_pct == 0) or (round(_total_pct)!=(100*len(tr_countries))))
        ):
        tr_regions = list(filter(lambda item2: item2.get('percentage', 100) not in ["", "0", "0.0"], tr_regions))
        _total_pct_2 = sum(list(map(lambda item2: float(item2.get('percentage', 100.0)), tr_regions)))
        tr_countries += tr_regions
        _total_pct += _total_pct_2

    _pct = 100.0
    return [{
        'percentage': float(item.get('percentage', 100))/(_total_pct/100),
        'code': item.get('code')
    } for item in tr_countries]

def get_countries(activity, transaction):
    recipient_c = get_first((transaction.xpath("recipient-country"), activity.xpath("recipient-country")), [])
    recipient_r = get_first((transaction.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']"), activity.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']")), [])
    return clean_countries(recipient_c, recipient_r)

def get_sectors(activity, transaction):
    recipient_s = get_first((transaction.xpath("sector[not(@vocabulary) or @vocabulary='1']"), activity.xpath("sector[not(@vocabulary) or @vocabulary='1']")), [])
    tr_sectors = clean_sectors(recipient_s)
    if len(tr_sectors) > 0:
        return tr_sectors
    return [{'percentage': 100.0, 'code': ''}]

def get_sector_category(code, category_group):
    if code == None: return ""
    return category_group.get(code[0:3], "")


class FlatIATITransaction():
    def transaction_data(self, country, sector, sector_category):
        transaction = {
            'iati_identifier': self.iati_identifier
        }

        def get_data(attr):
            if attr == 'title': return get_activity_title()
            if attr == 'reporting_org': return get_reporting_org()
            if attr == 'aid_type': return get_aid_type()
            if attr == 'finance_type': return get_finance_type()
            if attr == 'currency_original': return get_currency()

        def get_activity_title():
            return get_narrative(self.activity.find("title"))

        def get_reporting_org():
            _ro = self.activity.find("reporting-org")
            _text = get_narrative(_ro)
            _type = _ro.get('type')
            _ref = _ro.get('ref')
            _display = "{} - {}".format(_ref, _text)
            return {
                'text': _text,
                'type': _type,
                'ref': _ref,
                'display': _display
            }

        def get_reporting_org_type():
            return self.activity.find('reporting-org').get('type')

        def get_aid_type():
            el = self.activity.find('default-aid-type')
            if el is not None: return el
            else: return {}

        def get_finance_type():
            el = self.activity.find('default-finance-type')
            if el is not None: return el
            else: return {}

        def get_currency():
            return self.activity.get('default-currency')

        activity_functions = ['title', 'reporting_org', 'reporting_org_type',
            'aid_type', 'finance_type', 'currency_original']

        # This section reads from data for the activity in order to speed things up.
        for key in activity_functions:
            if getattr(self, key) is not None: continue
            if self.iati_identifier in self.flattener.activity_data:
                if self.flattener.activity_data.get(self.iati_identifier).get(key) is not None:
                    setattr(self, key, self.flattener.activity_data.get(self.iati_identifier).get(key))
                else:
                    setattr(self, key, get_data(key))
                    self.flattener.activity_data[self.iati_identifier][key] = getattr(self, key)
            else:
                setattr(self, key, get_data(key))
                self.flattener.activity_data[self.iati_identifier] = {
                    key: getattr(self, key)
                }

        if self.value_date == None:
            raise Exception("No value date for {}: {}".format(self.iati_identifier, etree.tostring(self.transaction)))
        if self.currency_original == None:
            raise Exception("No currency for {}: {}".format(self.iati_identifier, etree.tostring(self.transaction)))
        if self.value_original == None:
            raise Exception("No value for {}: {}".format(self.iati_identifier, etree.tostring(self.transaction)))

        if self.currency_original == 'USS': self.currency_original = 'USD'
        closest_exchange_rate = self.flattener.exchange_rates.closest_rate(
            self.currency_original, self.value_date
        )
        self.exchange_rate = closest_exchange_rate.get('conversion_rate')
        self.value_usd = self.value_original / self.exchange_rate

        return ([
            self.iati_identifier,
            self.title,
            self.reporting_org.get('display'),
            self.reporting_org.get('type'),
            self.aid_type.get('code', ''),
            self.finance_type.get('code', ''),
            self.provider_org.get('display'),
            self.provider_org.get('type'),
            self.receiver_org.get('display'),
            self.receiver_org.get('type'),
            self.transaction_type,
            (self.value_original*(country['percentage']/100)*(sector['percentage']/100)),
            self.currency_original,
            (self.value_usd*(country['percentage']/100)*(sector['percentage']/100)),
            self.value_date.isoformat(),
            self.exchange_rate,
            self.transaction_date,
            country['code'],
            self.multi_country,
            sector_category,
            sector['code'],
            self.covid_19,
            self.fiscal_year,
            self.fiscal_quarter
        ])


    def output_transaction(self):
        for sector in self.sectors:
            sector_category = get_sector_category(sector.get('code'), self.flattener.category_group)
            for country in self.countries:
                if (country['code'] not in self.flattener.countries):
                    continue
                if country['code'] not in self.flattener.csv_files:
                    _file = open('output/csv/{}.csv'.format(country['code']), 'a')
                    self.flattener.csv_files[country['code']] = {'file': _file, 'csv': csv.writer(_file), 'rows': []}
                self.flattener.csv_files[country['code']]['rows'].append(self.transaction_data(country, sector, sector_category))


    def process_transaction(self):
        activity = self.activity
        transaction = self.transaction

        self.iati_identifier = activity.find('iati-identifier').text
        if self.iati_identifier not in self.flattener.activity_data:
            self.flattener.activity_data[self.iati_identifier] = {}
        activity_data = self.flattener.activity_data[self.iati_identifier]

        # Get country/region - try first from transaction and then from activity
        transaction_countries = transaction.xpath('recipient-country')
        transaction_regions = transaction.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']")
        if (len(transaction_countries)!=0) or (len(transaction_regions) != 0):
            self.countries = clean_countries(transaction_countries, transaction_regions)
        else:
            activity_data_countries = activity_data.get('recipient_countries', [])
            activity_data_regions = activity_data.get('recipient_regions', [])
            if (len(activity_data_countries) != 0) or (len(activity_data_regions) != 0):
                self.countries = clean_countries(activity_data_countries, activity_data_regions)
            else:
                activity_data_countries = activity.xpath('recipient-country')
                activity_data_regions = activity.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']")
                if (len(activity_data_countries) != 0) or (len(activity_data_regions) != 0):
                    self.countries = clean_countries(activity_data_countries, activity_data_regions)
                if len(activity_data_countries) != 0:
                    activity_data['recipient_countries'] = activity_data_countries
                if len(activity_data_regions) != 0:
                    activity_data['recipient_regions'] = activity_data_regions

        if not hasattr(self, countries):
            return

        # Get sectors - try first from transaction and then from activity
        transaction_sectors = transaction.xpath("sector[not(@vocabulary) or @vocabulary='1']")
        if (len(transaction_sectors)!=0):
            self.sectors = clean_sectors(transaction_sectors)
        else:
            activity_data_sectors = activity_data.get('sectors', [])
            if (len(activity_data_sectors) != 0):
                self.sectors = clean_sectors(activity_data_sectors)
            else:
                activity_data_sectors = activity.xpath("sector[not(@vocabulary) or @vocabulary='1']")
                if (len(activity_data_sectors) != 0):
                    self.sectors = clean_sectors(activity_data_sectors)
                    activity_data['sectors'] = activity_data_sectors
                else:
                    self.sectors = [{'percentage': 100.0, 'code': ''}]
                    activity_data['sectors'] = self.sectors

        self.multi_country = {True: 1, False: 0}[len(self.countries)>1]
        self.transaction_type = transaction.find('transaction-type').get('code')
        if self.transaction_type not in ['2', '3', '4']: return

        self.aid_type = transaction.find('aid-type')
        self.finance_type = transaction.find('finance-type')

        self.provider_org = get_org(activity_data, transaction)
        self.receiver_org = get_org(activity_data, transaction, False)

        self.value_original = float(transaction.find('value').text)
        self.currency_original = transaction.find('value').get('currency')
        #currency_original = get_first((transaction.find('value').get('currency'), activity.get('default-currency')))

        self.value_date = get_date(transaction.find('value').get('value-date'))
        self.transaction_date = transaction.find('transaction-date').get('iso-date')
        self.fiscal_year, self.fiscal_quarter = get_fy_fq(self.transaction_date)
        self.covid_19 = int(get_covid_matches(transaction))

        self.output_transaction()

    def set_headers(self):
        for header in CSV_HEADERS:
            setattr(self, header, None)
        self.value_date = None

    def __init__(self, flattener, activity, transaction):
        self.flattener = flattener
        self.activity = activity
        self.transaction = transaction
        self.set_headers()
        self.process_transaction()


class FlattenIATIData():
    def setup_codelists(self, refresh_rates):
        self.activity_data = {}
        country_req = requests.get(COUNTRIES_CODELIST_URL)
        region_req = requests.get(REGIONS_CODELIST_URL)
        sector_req = requests.get(SECTORS_CODELIST_URL)
        sector_groups_req = requests.get(SECTOR_GROUPS_URL)
        #FIXME remove
        m49_req = requests.get(M49_CODELIST_URL)

        self.countries = list(map(lambda country: country['code'], country_req.json()["data"]))
        self.regions = list(map(lambda region: region['code'], region_req.json()["data"]))
        # FIXME in future - possibly remove this
        self.countries += self.regions
        self.category_group = dict(map(lambda code: (code['category_code'], code['group_code']), sector_groups_req.json()))
        self.sector_groups = dict(map(lambda code: (code['group_code'], code['group_name']), sector_groups_req.json()))
        # FIXME remove
        iso2_iso3 = dict(map(lambda code: (code['codeforiati:iso-alpha-2-code'], code['codeforiati:iso-alpha-3-code']), m49_req.json()["data"]))

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
        self.column_codelist['sector_category'] = self.sector_groups
        self.column_codelist['country_code'] = self.country_names

        self.exchange_rates = get_exchange_rates(refresh_rates)

    def setup_countries(self):
        for country in self.countries:
            with open('output/csv/{}.csv'.format(country), 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(CSV_HEADERS)


    def process_transaction(self, activity, transaction):
        FlatIATITransaction(self, activity, transaction)

    def write_csv_files(self):
        for _file in self.csv_files.values():
            #print("Writing file {}".format(_file['file'].name))
            _file['csv'].writerows(_file['rows'])
            _file['file'].close()


    def process_package(self, publisher, package):
        print("Processing package {}".format(package))
        starting = time.time()
        self.csv_files = {}
        doc = etree.parse(os.path.join(IATI_DUMP_DIR, "data", "{}".format(publisher), "{}".format(package)))
        if doc.getroot().get("version") not in ['2.01', '2.02', '2.03']: return
        transactions = doc.xpath("//transaction")
        #print("There are {} transactions for {}".format(len(transactions), package))

        start = time.time()
        self.activity_data = {}
        for transaction in transactions:
            self.process_transaction(transaction.getparent(), transaction)
        end = time.time()
        print("TRANSACTIONS {} TOOK {}".format(package, end-start))

        self.write_csv_files()

        ending = time.time()
        print("PROCESSING {} TOOK {}".format(package, ending-starting))


    def run_for_publishers(self):
        print("BEGINNING PROCESS AT {}".format(datetime.datetime.utcnow()))
        beginning = time.time()
        #FIXME
        for publisher in ['fcdo']: #self.publishers:
            if publisher in EXCLUDED_PUBLISHERS: continue
            start = time.time()
            try:
                print("Processing {}".format(publisher))
                packages = os.listdir(os.path.join(IATI_DUMP_DIR, "data", "{}".format(publisher)))
                packages.sort()
                #FIXME
                #packages = ['fcdo-ng.xml']
                for package in packages:
                    #try:
                    if package.endswith(".xml"):
                        self.process_package(publisher, package)
                        #FIXME
                        #break
                    #except Exception:
                    #    print("Exception with package {}".format(package))
                    #    continue #raise Exception
            except NotADirectoryError:
                continue
            end = time.time()
            print("Processing {} took {}s".format(publisher, end-start))
        print("FINISHED PROCESS AT {}".format(datetime.datetime.utcnow()))
        finishing = time.time()
        print("PROCESSING TOOK {}".format(finishing-beginning))


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
            #dataframe[cl_key] = dataframe.apply(lambda row: "{} - {}".format(float_int_string(row[cl_key]), get_if_exists(_from=cl_items, _item=row[cl_key])), axis=1)
        return dataframe


    def write_dataframe_to_excel(self, dataframe, filename):
        """
        Function to write a pandas dataframe to Excel.
        This uses pyExcelerate, which is about 2x as fast as the built-in
        pandas library.
        """
        wb = Workbook()
        headers = dataframe.columns.tolist()
        data = dataframe.values.tolist()
        data.insert(0, headers)
        ws = wb.new_sheet("Data", data=data)
        wb.save(filename)


    def group_results(self, country):
        country_name, _ = country.split(".")
        df = pd.read_csv("output/csv/{}.csv".format(country_name), dtype=CSV_HEADER_DTYPES)
        if (not "reporting_org" in df.columns.values) or (len(df)==0):
            return
        out = df.fillna("").groupby(GROUP_BY_HEADERS)
        out = out["value_usd"].agg("sum").reset_index().fillna("")
        out = self.relabel_dataframe(out)
        self.write_dataframe_to_excel(out, "output/xlsx/{}.xlsx".format(country_name))

    def group_data(self):
        csv_files = os.listdir("output/csv/")
        csv_files.sort()
        print("BEGINNING PROCESS AT {}".format(datetime.datetime.utcnow()))
        for country in csv_files:
            start = time.time()
            if country.endswith(".csv"): self.group_results(country)
            end = time.time()
            print("Processing {} took {}s".format(country, end-start))
        print("FINISHED PROCESS AT {}".format(datetime.datetime.utcnow()))


    def __init__(self, refesh_rates=False):
        self.publishers = os.listdir(os.path.join(IATI_DUMP_DIR, "data"))
        self.publishers.sort()
        self.setup_codelists(refesh_rates=refresh_rates)
        self.setup_countries()
        self.run_for_publishers()
        self.group_data()


FlattenIATIData(refresh_rates=True)
