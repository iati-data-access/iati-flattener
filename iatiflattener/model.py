import json, datetime, csv, os

from iatiflattener.lib.utils import get_date, get_fy_fq, get_fy_fq_numeric
from iatiflattener.lib.iati_helpers import clean_countries, clean_sectors, get_narrative, get_org_name, get_org, get_sector_category
from iatiflattener.lib.iati_transaction_helpers import get_classification_from_transactions, get_sectors_from_transactions, get_countries_from_transactions
from iatiflattener.lib.variables import headers_with_langs
from exchangerates import UnknownCurrencyException

DPORTAL_URL = "https://d-portal.org/q.html?aid={}"


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if (isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date)):
            return obj.isoformat()
        elif (type(obj) is {}.values().__class__) or (type(obj) is {}.keys().__class__):
            return list(obj)
        elif (type(obj) is range):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


def jsonify(*args, **kwargs):
    return json.dumps(dict(*args, **kwargs), cls=JSONEncoder)


class ActivityCacheActivity():
    def get(self, property):
        return getattr(self, property)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __init__(self, iati_identifier):
        self.iati_identifier = iati_identifier
        fields = ['title', 'currency', 'sectors',
        'countries', 'regions', 'aid_type',
        'finance_type', 'flow_type', 'title',
        'reporting_org', 'participating_org_1',
        'participating_org_2', 'participating_org_3',
        'participating_org_4']
        for field in fields:
            setattr(self, field, None)


class ActivityCache():
    def get(self, iati_identifier):
        activity = self.data.get(iati_identifier)
        if activity is None:
            self.data[iati_identifier] = ActivityCacheActivity(iati_identifier)
        return self.data[iati_identifier]

    def __init__(self):
        self.data = {}


class CSVFilesWriter():
    def append(self, country, flat_transaction_budget):
        if country not in self.csv_files:
            _file = open(
                os.path.join(self.output_dir,
                    'csv',
                    '{}.csv'.format(country)),
                'a')
            self.csv_files[country] = {
                'file': _file,
                'csv': csv.writer(_file),
                'rows': []
            }
        if self.csv_headers:
            self.csv_files[country]['rows'].append([flat_transaction_budget[header] for header in self.csv_headers])
        else:
            self.csv_files[country]['rows'].append(flat_transaction_budget.values())

    def write(self):
        for _filename, _file in self.csv_files.items():
            _file['csv'].writerows(_file['rows'])
            _file['file'].close()

    def __init__(self, output_dir='output', headers=[]):
        self.csv_files = {}
        self.csv_headers = headers
        self.output_dir = output_dir


class FlatBudget():
    def make_flattened(self, country, sector, aid_type,
            finance_type, flow_type, budget):
        for k, v in budget.items():
            self.flat_budget[k] = v
        self.flat_budget['country_code'] = country.get('code')
        self.flat_budget['sector_code'] = sector.get('code')
        self.flat_budget['sector_category'] = get_sector_category(sector.get('code'), self.sector_categories)
        self.flat_budget['aid_type'] = aid_type.get('code')
        self.flat_budget['finance_type'] = finance_type.get('code')
        self.flat_budget['flow_type'] = flow_type.get('code')
        pct_adjustment = ((country['percentage']/100) *
            (sector['percentage']/100) * (aid_type['percentage']/100) *
            (finance_type['percentage']/100) * (flow_type['percentage']/100))
        self.flat_budget['value_original'] = (
            budget['value_original'] * pct_adjustment
        )
        self.flat_budget['value_usd'] = (
            budget['value_usd'] * pct_adjustment
        )
        self.flat_budget['value_eur'] = (
            budget['value_eur'] * pct_adjustment
        )
        self.flat_budget['value_local'] = dict([(country, (value_local * pct_adjustment)) for country, value_local in self.flat_budget['value_local'].items()])
        return dict([(k, v) for k, v in self.flat_budget.items() if k not in ['countries', 'sectors']])

    def flatten(self):
        for sector in self.budget.sectors.value:
            sector_category = get_sector_category(
                sector.get('code'),
                self.sector_categories)
            for country in self.budget.countries.value:
                for aid_type in self.budget.aid_types.value:
                    for finance_type in self.budget.finance_types.value:
                        for flow_type in self.budget.flow_types.value:
                            for budget in self.budget.budgets.value:
                                yield self.make_flattened(
                                    country, sector, aid_type, finance_type,
                                    flow_type, budget)

    def flatten_json(self):
        for sector in self.budget.sectors.value:
            sector_category = get_sector_category(
                sector.get('code'),
                self.sector_categories)
            for country in self.budget.countries.value:
                for aid_type in self.budget.aid_types.value:
                    for finance_type in self.budget.finance_types.value:
                        for flow_type in self.budget.flow_types.value:
                            for budget in self.budget.budgets.value:
                                yield jsonify(self.make_flattened(
                                    country, sector, aid_type, finance_type,
                                    flow_type, budget))

    def __init__(self, budget, sector_categories={}):
        self.budget = budget
        self.sector_categories = sector_categories
        self.budget_dict = budget.as_csv_dict()
        self.flat_budget = self.budget_dict


class FlatTransactionBudgetCSV():
    def get_local_currency(self, country, flat_transaction_budget):
        flat_transaction_budget['value_local'] = flat_transaction_budget['value_local'].get(country)
        return flat_transaction_budget

    def output(self):
        country = self.flat_transaction_budget['country_code']
        if country in self.countries:
            self.csv_writer.append(country=country,
                flat_transaction_budget=self.get_local_currency(country, self.flat_transaction_budget))

    def __init__(self, countries, csv_writer, flat_transaction_budget):
        self.countries = countries
        self.csv_writer = csv_writer
        self.flat_transaction_budget = flat_transaction_budget


class FlatTransaction():
    def make_flattened(self, sector, country):
        self.flat_transaction['country_code'] = country.get('code')
        self.flat_transaction['sector_code'] = sector.get('code')
        self.flat_transaction['sector_category'] = get_sector_category(sector.get('code'), self.sector_categories)
        sector_pct_adjustment = (country['percentage']/100) * (sector['percentage']/100)
        self.flat_transaction['value_original'] = (
            self.transaction.value_original.value * sector_pct_adjustment
        )
        self.flat_transaction['value_usd'] = (
            self.transaction.value_usd.value * sector_pct_adjustment
        )
        self.flat_transaction['value_eur'] = (
            self.transaction.value_eur.value * sector_pct_adjustment
        )
        self.flat_transaction['value_local'] = dict([(country, (value_local * sector_pct_adjustment)) for country, value_local in self.flat_transaction['value_local'].items()])
        return dict([(k, v) for k, v in self.flat_transaction.items() if k not in ['countries', 'sectors']])

    def flatten(self):
        for sector in self.transaction.sectors.value:
            for country in self.transaction.countries.value:
                yield self.make_flattened(sector, country)

    def flatten_json(self):
        for sector in self.transaction.sectors.value:
            for country in self.transaction.countries.value:
                yield jsonify(self.make_flattened(sector, country))

    def __init__(self, transaction, sector_categories={}):
        self.transaction = transaction
        self.sector_categories = sector_categories
        self.transaction_dict = transaction.as_csv_dict()
        self.flat_transaction = self.transaction_dict


class FinancialValues():
    def _exchange_rate_usd(self):
        closest_exchange_rate = self.exchange_rates.closest_rate(
            self.currency_original.value, self.value_date.value
        )
        exchange_rate = closest_exchange_rate.get('conversion_rate')
        exchange_rate_date = closest_exchange_rate.get('closest_date').isoformat()
        value_usd = self.value_original.value / exchange_rate
        return SimpleField(exchange_rate), SimpleField(value_usd), SimpleField(exchange_rate_date)

    def _exchange_rate_eur(self):
        closest_exchange_rate = self.exchange_rates.closest_rate(
            'EUR', self.value_date.value
        )
        exchange_rate = closest_exchange_rate.get('conversion_rate')
        value_eur = self.value_usd.value * exchange_rate
        return SimpleField(value_eur)

    def _values_local(self):
        out = {}
        for country in self.countries.value:
            currency_code = self.currencies.get(country.get('code'))
            try:
                closest_exchange_rate = self.exchange_rates.closest_rate(
                    currency_code, self.value_date.value
                )
                exchange_rate = closest_exchange_rate.get('conversion_rate')
                value = self.value_usd.value * exchange_rate
                out[country.get('code')] = value
            except UnknownCurrencyException:
                out[country.get('code')] = 0.00
        return SimpleField(out)


class Common(FinancialValues):
    def _get_first_attrib(self, field, attribute):
        if field is None:
            return None
        list_of_values = list(field.values())
        if len(list_of_values) > 0:
            return list_of_values[0].get(attribute)
        return None

    def _iati_identifier(self):
        return IATIIdentifier(self.activity)

    def _title(self):
        return Title(self.activity, self.activity_cache, self.langs)

    def _reporting_org(self):
        return ReportingOrg(self.activity, self.activity_cache, self.organisations_cache, self.langs)

    def _reporting_org_type(self):
        return SimpleField(list(self.reporting_org.value.values())[0].get('type'))

    def _fiscal_year_quarter(self):
        fy, fq = get_fy_fq(self.transaction_date.value)
        return SimpleField(fy), SimpleField(fq)

    def _fiscal_year_fiscal_quarter(self):
        return SimpleField("{} {}".format(self.fiscal_year.value, self.fiscal_quarter.value))

    def _countries(self, budget=False):
        if budget is False:
            return CountryRegion(self.activity, self.activity_cache, self.transaction)
        return CountryRegion(self.activity, self.activity_cache, False, self.activity_currency, self.exchange_rates)

    def _sectors(self, budget=False):
        if budget is False:
            return Sector(self.activity, self.activity_cache, self.transaction)
        return Sector(self.activity, self.activity_cache, False, self.activity_currency, self.exchange_rates)

    def _humanitarian(self, budget=True):
        if budget is False:
            return Humanitarian(self.activity, self.transaction)
        return Humanitarian(self.activity, False)

    def _default_field(self, field_name, budget=False):
        if budget is False:
            return DefaultActivityField(self.activity, self.activity_cache, self.transaction, field_name)
        return DefaultActivityField(self.activity, self.activity_cache, False, field_name, self.activity_currency, self.exchange_rates)

    def _dportal_url(self):
        return SimpleField(DPORTAL_URL.format(self.iati_identifier))

    def update_cache(self, field):
        self.activity_cache = field.activity_cache
        return field

    def as_dict(self):
        return dict([(field, getattr(getattr(self, field), 'value')) for field in self.fields])

    def field_with_lang(self, field, lang):
        if field in self.multilingual_fields:
            return "{}#{}".format(field, lang)
        return field

    def as_csv_dict(self):
        return dict([(self.field_with_lang(field, lang), getattr(getattr(self, field), 'csv_value')(lang)) for lang in self.langs for field in self.csv_fields])

    def jsonify(self):
        return jsonify(self.as_dict())

    def _multi_country(self):
        return SimpleField(1 if len(self.countries.value) > 1 else 0)

    def _provider_org_type(self):
        return SimpleField(self._get_first_attrib(self.provider_org.value, 'type'))

    def _receiver_org_type(self):
        return SimpleField(self._get_first_attrib(self.receiver_org.value, 'type'))


class BudgetPeriod():
    def __init__(self, dict):
        for k, v in dict.items():
            setattr(self, k, v)


class Budget(FinancialValues):
    def generate(self):
        budget_currency = self.budget_element.find('value').get('currency')
        if budget_currency is not None:
            self.currency_original = SimpleField(budget_currency)
        else:
            self.currency_original = SimpleField(self.default_currency)
        self.period_start = get_date(self.budget_element.find('period-start').get('iso-date'))
        self.period_end = get_date(self.budget_element.find('period-end').get('iso-date'))
        self.value_original = SimpleField(float(self.budget_element.find('value').text))
        self.value_date = SimpleField(get_date(self.budget_element.find('value').get('value-date')))

        self.exchange_rate, self.value_usd, self.exchange_rate_date = self._exchange_rate_usd()
        self.value_eur = self._exchange_rate_eur()
        self.value_local = self._values_local()
        return ((self.period_start, self.period_end),
            BudgetPeriod({
                'period_start': self.period_start,
                'period_end': self.period_end,
                'currency_original': self.currency_original,
                'value_original': self.value_original,
                'value_date': self.value_date,
                'original_revised': self.original_revised,
                'value_local': self.value_local,
                'value_eur': self.value_eur,
                'exchange_rate': self.exchange_rate,
                'value_usd': self.value_usd,
                'exchange_rate_date': self.exchange_rate_date
            })
        )

    def as_dict(self):
        return dict([(field, getattr(getattr(self, field), 'value')) for field in self.fields])

    def __init__(self, budget_element, default_currency, original_revised, countries, exchange_rates, currencies):
        self.budget_element = budget_element
        self.default_currency = default_currency
        self.original_revised = original_revised
        self.countries = countries
        self.exchange_rates = exchange_rates
        self.currencies = currencies
        self.value = self.generate()
        self.fields = ['period_start', 'default_currency', 'original_revised', 'value']


class ActivityBudget(Common):
    def _activity_currency(self):
        return self.activity.get('default-currency')

    def _organisation_field(self, provider_receiver):
        return Organisation(self.activity_cache, self.organisations_cache, self.activity, provider_receiver=='provider', self.langs)

    def _get_budget_periods(self, exchange_rates, budgets):
        out = []
        for budget in budgets:
            if (budget.value_original == 0): continue
            period_start_fy, period_start_fq = get_fy_fq_numeric(budget.period_start)
            period_end_fy, period_end_fq = get_fy_fq_numeric(budget.period_end)
            year_range = range(period_start_fy, period_end_fy+1)
            for year in year_range:
                if (year == period_start_fy) and (year==period_end_fy):
                    quarter_range = range(period_start_fq, period_end_fq+1)
                elif year == period_start_fy:
                    quarter_range = range(period_start_fq, 4+1)
                elif year == period_end_fy:
                    quarter_range = range(1, period_end_fq+1)
                else:
                    quarter_range = range(1, 4+1)
                for quarter in quarter_range:
                    value_local = dict([(country, (value_local/len(quarter_range)/len(year_range))) for country, value_local in budget.value_local.value.items()])
                    out.append({
                        'fiscal_year': year,
                        'fiscal_quarter': quarter,
                        'fiscal_year_quarter': "{} {}".format(year, quarter),
                        'value_usd': budget.value_usd.value/len(quarter_range)/len(year_range),
                        'value_eur': budget.value_eur.value/len(quarter_range)/len(year_range),
                        'value_local': value_local,
                        'value_original': budget.value_original.value/len(quarter_range)/len(year_range),
                        'value_date': budget.value_date.value,
                        'transaction_date': "{}-{}-01".format(year, (quarter-1)*3),
                        'exchange_rate': budget.exchange_rate.value,
                        'exchange_rate_date': budget.exchange_rate_date.value,
                        'currency_original': budget.currency_original.value,
                        'original_revised': budget.original_revised
                    })
        return out


    def _get_budgets(self):
        original_budget_els = self.activity.xpath("budget[not(@type) or @type='1']")
        revised_budget_els = self.activity.findall("budget[@type='2']")

        original_budgets = dict(map(lambda budget: Budget(budget, self.activity_currency,
            'original', self.countries, self.exchange_rates, self.currencies).value, original_budget_els))
        revised_budgets = dict(map(lambda budget: Budget(budget, self.activity_currency,
            'revised', self.countries, self.exchange_rates, self.currencies).value, revised_budget_els))

        revised_budget_start_dates = list(map(lambda budget: budget[0], revised_budgets))
        def filter_budgets(budget_item):
            for start_date in revised_budget_start_dates:
                if (budget_item[0][0] <= start_date) and (budget_item[0][0] >= start_date): return False
            return True

        budgets = list(dict(filter(filter_budgets, original_budgets.items())).values())
        budgets += list(revised_budgets.values())

        return self._get_budget_periods(self.exchange_rates, budgets)

    def generate(self):
        self.iati_identifier = self._iati_identifier()
        self.title = self.update_cache(self._title())
        self.reporting_org = self.update_cache(self._reporting_org())
        self.reporting_org_type = self._reporting_org_type()

        self.activity_currency = self._activity_currency()
        self.sectors = self.update_cache(self._sectors(budget=True))
        self.countries = self.update_cache(self._countries(budget=True))
        if self.countries.value == False:
            return False
        self.budgets = SimpleField(self._get_budgets())
        self.multi_country = self._multi_country()
        self.humanitarian = self._humanitarian(budget=True)

        self.aid_types = self.update_cache(self._default_field('aid-type', budget=True))
        self.finance_types = self.update_cache(self._default_field('finance-type', budget=True))
        self.flow_types = self.update_cache(self._default_field('flow-type', budget=True))

        self.provider_org = self._organisation_field('provider')
        self.provider_org_type = self._provider_org_type()
        self.receiver_org = self._organisation_field('receiver')
        self.receiver_org_type = self._receiver_org_type()
        self.transaction_type = SimpleField('budget')
        self.url = self._dportal_url()
        return True

    def __init__(self, activity, activity_cache, exchange_rates, currencies,
            organisations_cache=[], langs=['en']):
        self.activity = activity
        self.activity_cache = activity_cache.get(
            self._iati_identifier().value
        )
        self.currencies = currencies
        self.exchange_rates = exchange_rates
        self.organisations_cache = organisations_cache
        self.langs = langs
        self.csv_fields = ['iati_identifier', 'title', 'reporting_org',
        'reporting_org_type', 'budgets',
        'countries', 'sectors', 'multi_country', 'humanitarian', 'aid_types',
        'finance_types', 'flow_types', 'provider_org', 'provider_org_type',
        'receiver_org', 'receiver_org_type',
        'transaction_type', 'url']
        self.fields = ['iati_identifier', 'title', 'reporting_org',
        'reporting_org_type', 'budgets',
        'countries', 'sectors', 'multi_country', 'humanitarian', 'aid_types',
        'finance_types', 'flow_types', 'provider_org', 'provider_org_type',
        'receiver_org', 'receiver_org_type',
        'transaction_type',
        'url']
        self.fields_with_attributes = {
            'reporting_org': {
                '': 'display',
                '_type': 'type'
            }
        }
        self.multilingual_fields = ['title', 'reporting_org',
        'provider_org', 'receiver_org']


class Transaction(Common):
    def _organisation_field(self, provider_receiver):
        return Organisation(self.activity_cache, self.organisations_cache, self.transaction, provider_receiver=='provider', self.langs)

    def _transaction_type(self):
        return SimpleField(self.transaction.find('transaction-type').get('code'))

    def _value_original(self):
        return SimpleField(float(self.transaction.find('value').text))

    def _currency_original(self):
        return Currency(self.activity, self.activity_cache, self.transaction)

    def _value_date(self):
        return SimpleField(get_date(self.transaction.find('value').get('value-date')))

    def _transaction_date(self):
        return SimpleField(self.transaction.find('transaction-date').get('iso-date'))

    def generate(self):
        self.iati_identifier = self._iati_identifier()
        self.title = self.update_cache(self._title())
        self.reporting_org = self.update_cache(self._reporting_org())
        self.reporting_org_type = self._reporting_org_type()
        self.countries = self.update_cache(self._countries())
        if self.countries.value == False: return False
        self.sectors = self.update_cache(self._sectors())
        self.multi_country = self._multi_country()
        self.transaction_type = self._transaction_type()
        if self.limit_transaction_types and (self.transaction_type.value not in ['1', '2', '3', '4']):
            return False
        self.aid_type = self.update_cache(self._default_field('aid-type'))
        self.finance_type = self.update_cache(self._default_field('finance-type'))
        self.flow_type = self.update_cache(self._default_field('flow-type'))
        self.humanitarian = self._humanitarian()
        self.provider_org = self._organisation_field('provider')
        self.provider_org_type = self._provider_org_type()
        self.receiver_org = self._organisation_field('receiver')
        self.receiver_org_type = self._receiver_org_type()
        self.value_original = self._value_original()
        self.currency_original = self.update_cache(self._currency_original())
        self.value_date = self._value_date()
        self.transaction_date = self._transaction_date()
        self.fiscal_year, self.fiscal_quarter = self._fiscal_year_quarter()
        self.fiscal_year_quarter = self._fiscal_year_fiscal_quarter()
        self.exchange_rate, self.value_usd, self.exchange_rate_date = self._exchange_rate_usd()
        self.value_eur = self._exchange_rate_eur()
        self.value_local = self._values_local()
        self.url = self._dportal_url()
        return True

    def __init__(self, activity, transaction, activity_cache, exchange_rates,
            currencies, limit_transaction_types=True, organisations_cache=[],
            langs=['en']):
        self.transaction = transaction
        self.activity = activity
        self.activity_cache = activity_cache.get(
            self._iati_identifier().value
        )
        self.currencies = currencies
        self.exchange_rates = exchange_rates
        self.limit_transaction_types = limit_transaction_types
        self.langs = langs
        self.csv_fields = ['iati_identifier', 'title', 'reporting_org',
        'reporting_org_type',
        'countries', 'sectors', 'multi_country', 'humanitarian', 'aid_type',
        'finance_type', 'flow_type', 'provider_org', 'provider_org_type',
        'receiver_org', 'receiver_org_type',
        'transaction_type', 'value_original', 'currency_original',
        'value_date', 'transaction_date', 'fiscal_year',
        'fiscal_quarter', 'fiscal_year_quarter',
        'exchange_rate', 'exchange_rate_date', 'value_usd', 'value_eur',
        'value_local',
        'url']
        self.fields = ['iati_identifier', 'title', 'reporting_org',
        'reporting_org_type',
        'countries', 'sectors', 'multi_country', 'humanitarian', 'aid_type',
        'finance_type', 'flow_type', 'provider_org', 'provider_org_type',
        'receiver_org', 'receiver_org_type',
        'transaction_type', 'value_original', 'currency_original',
        'value_date', 'transaction_date', 'fiscal_year',
        'fiscal_quarter', 'fiscal_year_quarter', 'exchange_rate',
        'exchange_rate_date', 'value_usd', 'value_eur', 'value_local',
        'url']
        self.fields_with_attributes = {
            'reporting_org': {
                '': 'display',
                '_type': 'type'
            }
        }
        self.multilingual_fields = ['title', 'reporting_org',
        'provider_org', 'receiver_org']
        self.organisations_cache = organisations_cache


class Field():
    def csv_value(self, lang='en', attribute=None):
        return self.value

    def __str__(self):
        return self.value


class SimpleField(Field):
    def __init__(self, value):
        self.value = value


class IATIIdentifier(Field):
    def __init__(self, activity):
        self.activity = activity
        self.value = activity.find('iati-identifier').text


class Title(Field):
    def csv_value(self, lang):
        return self.value.get(lang)

    def generate(self):
        if self.activity_cache.title is not None:
            return self.activity_cache.title
        title = dict([(lang, get_narrative(self.activity.find("title"), lang)) for lang in self.langs])
        self.activity_cache.title = title
        return title

    def __init__(self, activity, activity_cache, langs):
        self.activity = activity
        self.activity_cache = activity_cache
        self.langs = langs
        self.value = self.generate()


class ReportingOrg(Field):
    def csv_value(self, lang):
        return self.value.get(lang).get('display')

    def get_reporting_org(self, lang):
        _ro = self.activity.find("reporting-org")
        _text = get_org_name(
            organisations=self.organisations,
            ref=_ro.get("ref"),
            text=get_narrative(_ro, lang)
        )
        _type = _ro.get('type')
        _ref = _ro.get('ref')
        _display = "{} [{}]".format(_text, _ref)
        return {
            'text': _text,
            'type': _type,
            'ref': _ref,
            'display': _display
        }

    def generate(self):
        if self.activity_cache.reporting_org is not None:
            return self.activity_cache.reporting_org
        reporting_org = dict([(lang, self.get_reporting_org(lang)) for lang in self.langs])
        self.activity_cache.reporting_org = reporting_org
        return reporting_org

    def __init__(self, activity, activity_cache, organisations, langs):
        self.activity = activity
        self.activity_cache = activity_cache
        self.organisations = organisations
        self.langs = langs
        self.value = self.generate()


class Organisation(Field):
    def csv_value(self, lang):
        return self.value.get(lang).get('display')

    def get_organisation(self, lang):
        return get_org(self.organisations, self.activity_cache, self.transaction, self.provider_receiver, lang)

    def generate(self):
        return dict([(lang, self.get_organisation(lang)) for lang in self.langs])

    def __init__(self, activity_cache, organisations, transaction, provider_receiver, langs):
        self.activity_cache = activity_cache
        self.organisations = organisations
        self.transaction = transaction
        self.provider_receiver = provider_receiver
        self.langs = langs
        self.value = self.generate()


class CountryRegion(Field):
    def country_transaction(self):
        return self.transaction.xpath('recipient-country')

    def region_transaction(self):
        return self.transaction.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']")

    def country_activity(self):
        return self.activity.xpath('recipient-country')

    def region_activity(self):
        return self.activity.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']")

    def country_region_transaction(self):
        countries = self.country_transaction()
        regions = self.region_transaction()
        if (countries or regions):
            return clean_countries(countries, regions)
        return []

    def country_region_from_all_transactions(self):
        return get_countries_from_transactions(
            self.activity,
            self.currency_original,
            self.exchange_rates
        )

    def country_region_activity(self):
        if (self.activity_cache.countries is not None) or (self.activity_cache.regions is not None):
            return clean_countries(self.activity_cache.countries,
                self.activity_cache.regions)
        countries = self.country_activity()
        regions = self.region_activity()
        if (countries or regions):
            if countries is not None:
                self.activity_cache.countries = countries
            if regions is not None:
                self.activity_cache.regions = regions
            return clean_countries(countries, regions)
        return []

    def generate_from_transaction(self):
        country_region_transaction = self.country_region_transaction()
        if country_region_transaction:
            return country_region_transaction
        country_region_activity = self.country_region_activity()
        if country_region_activity:
            return country_region_activity
        return False

    def generate_from_budget(self):
        country_region_activity = self.country_region_activity()
        if country_region_activity:
            return country_region_activity
        country_region_from_transactions = self.country_region_from_all_transactions()
        if len(country_region_from_transactions) >= 0:
            return country_region_from_transactions
        return False

    def __init__(self, activity, activity_cache, transaction, currency_original=None, exchange_rates=None):
        self.activity = activity
        self.activity_cache = activity_cache
        self.transaction = transaction
        if self.transaction == False:
            self.currency_original = currency_original
            self.exchange_rates = exchange_rates
            self.value = self.generate_from_budget()
        else:
            self.value = self.generate_from_transaction()


class Sector(Field):
    def _sector_transaction(self):
        return self.transaction.xpath("sector[not(@vocabulary) or @vocabulary='1']")

    def _sector_activity(self):
        return self.activity.xpath("sector[not(@vocabulary) or @vocabulary='1']")

    def sector_transaction(self):
        sectors = self._sector_transaction()
        if (len(sectors)!=0):
            return clean_sectors(sectors)
        return False

    def sectors_from_all_transactions(self):
        return get_sectors_from_transactions(
            self.activity,
            self.currency_original,
            self.exchange_rates
        )

    def sector_activity(self):
        if (self.activity_cache.sectors is not None):
            return clean_sectors(self.activity_cache.sectors)
        sectors = self._sector_activity()
        if sectors:
            self.activity_cache.sectors = sectors
            return clean_sectors(sectors)
        return False

    def generate_from_transaction(self):
        sector_transaction = self.sector_transaction()
        if sector_transaction:
            return sector_transaction
        sector_activity = self.sector_activity()
        if sector_activity:
            return sector_activity
        self.activity_cache.sectors = [{'percentage': 100.0, 'code': ''}]
        return self.activity_cache.sectors

    def generate_from_budget(self):
        sector_activity = self.sector_activity()
        if sector_activity:
            return sector_activity
        return self.sectors_from_all_transactions()

    def __str__(self):
        return self.value

    def __init__(self, activity, activity_cache, transaction, currency_original=None, exchange_rates=None):
        self.activity = activity
        self.activity_cache = activity_cache
        self.transaction = transaction
        if self.transaction == False:
            self.exchange_rates = exchange_rates
            self.currency_original = currency_original
            self.value = self.generate_from_budget()
        else:
            self.value = self.generate_from_transaction()


class Humanitarian(Field):
    def field_transaction(self):
        return self.transaction.get('humanitarian')

    def field_activity(self):
        return self.activity.get('humanitarian')

    def generate(self):
        if self.transaction is not False:
            field_transaction = self.field_transaction()
            if field_transaction is not None: return 1 if field_transaction in ['true', '1'] else 0
        field_activity = self.field_activity()
        if field_activity is not None: return 1 if field_activity in ['true', '1'] else 0
        return False

    def __init__(self, activity, transaction):
        self.activity = activity
        self.transaction = transaction
        self.value = 1 if self.generate() == True else 0


class Currency(Field):
    def field_transaction(self):
        return self.transaction.find('value').get('currency')

    def field_activity(self):
        return self.activity.get('default-currency')

    def generate(self):
        field_transaction = self.field_transaction()
        if field_transaction is not None: return field_transaction
        field_activity = self.field_activity()
        if field_activity is not None:
            setattr(self.activity_cache, 'currency', field_activity)
            return field_activity
        else:
            return None

    def __init__(self, activity, activity_cache, transaction):
        self.activity = activity
        self.activity_cache = activity_cache
        self.transaction = transaction
        self.value = self.generate()


class DefaultActivityField(Field):
    def field_transaction(self):
        return self.transaction.find(self.field_name)

    def field_activity(self):
        field_activity = self.activity.find('default-{}'.format(self.field_name))
        if field_activity is not None:
            setattr(self.activity_cache, self.field_name_underscores, field_activity.get('code'))
        return field_activity

    def field_from_all_transactions(self):
        return get_classification_from_transactions(
            self.activity,
            self.currency_original,
            self.exchange_rates,
            self.field_name)

    def generate_from_transaction(self):
        field_transaction = self.field_transaction()
        if field_transaction is not None: return field_transaction.get('code')
        if getattr(self.activity_cache, self.field_name_underscores) is not None:
            return getattr(self.activity_cache, self.field_name_underscores)
        field_activity = self.field_activity()
        if field_activity is not None:
            return field_activity.get('code')
        else:
            return None

    def generate_from_budget(self):
        field_activity = self.field_activity()
        if field_activity is not None:
            return [{
                'code': field_activity.get('code'),
                'percentage': 100.0
            }]
        return self.field_from_all_transactions()

    def __init__(self, activity, activity_cache, transaction, field_name, currency_original=None, exchange_rates=None):
        self.activity = activity
        self.activity_cache = activity_cache
        self.transaction = transaction
        self.field_name = field_name
        self.field_name_underscores = field_name.replace("-", "_")
        if self.transaction == False:
            self.exchange_rates = exchange_rates
            self.currency_original = currency_original
            self.value = self.generate_from_budget()
        else:
            self.value = self.generate_from_transaction()


