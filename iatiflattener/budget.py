from lxml import etree
import csv
import os

from iatiflattener.activity import ActivityDataSetter
from iatiflattener.lib.iati_helpers import (get_sector_category, clean_sectors,
    clean_countries, get_org)
from iatiflattener.lib.iati_budget_helpers import get_budgets
from iatiflattener.lib.iati_transaction_helpers import (get_sectors_from_transactions,
    get_countries_from_transactions, get_aid_type_from_transactions,
    get_finance_type_from_transactions)
from iatiflattener.lib.variables import CSV_HEADERS


class FlatIATIBudget():

    def budget_data(self, country, sector, sector_category, aid_type,
            finance_type, budget, as_dict=False):
        for k, v in budget.items():
            setattr(self, k, v)

        if self.value_date == None:
            raise Exception("No value date for {}: {}".format(
                self.iati_identifier, etree.tostring(self.budget)))
        if self.currency_original == None:
            raise Exception("No currency for {}: {}".format(
                self.iati_identifier, etree.tostring(self.budget)))
        if self.value_original == None:
            raise Exception("No value for {}: {}".format(
                self.iati_identifier, etree.tostring(self.budget)))

        value_usd = (
            self.value_usd*(
                country['percentage']/100
            )*(
                sector['percentage']/100
            )*(
                aid_type['percentage']/100
            )*(
                finance_type['percentage']/100
            )
        )
        value_original = (
            self.value_original*(
                country['percentage']/100
            )*(
                sector['percentage']/100
            )*(
                aid_type['percentage']/100
            )*(
                finance_type['percentage']/100
            )
        )

        out = [
            self.iati_identifier,
            self.title,
            self.reporting_org.get('display'),
            self.reporting_org.get('type'),
            aid_type.get('code', ''),
            finance_type.get('code', ''),
            self.provider_org.get('display'),
            self.provider_org.get('type'),
            self.receiver_org.get('display'),
            self.receiver_org.get('type'),
            'budget', # Transaction Type
            value_original,
            self.currency_original,
            value_usd,
            self.value_date.isoformat(),
            self.exchange_rate,
            "{}-{}-01".format(self.fiscal_year, (self.fiscal_quarter-1)*3), # Transaction Date,
            country['code'],
            self.multi_country,
            sector_category,
            sector['code'],
            0, #self.covid_19, # COVID-19
            self.fiscal_year,
            "Q{}".format(self.fiscal_quarter)
        ]

        if as_dict==False:
            return out

        return {
            'iati_identifier': self.iati_identifier,
            'title': self.title,
            'reporting_org': self.reporting_org,
            'aid_type': aid_type.get('code', ''),
            'finance_type': finance_type.get('code', ''),
            'provider_org': self.provider_org,
            'receiver_org': self.receiver_org,
            'transaction_type': 'budget', # Transaction Type
            'value_original': value_original,
            'currency_original': self.currency_original,
            'value_usd': value_usd,
            'value_date': self.value_date.isoformat(),
            'exchange_rate': self.exchange_rate,
            'transaction_date': "{}-{}-01".format(self.fiscal_year, (self.fiscal_quarter-1)*3), # Transaction Date,
            'country': country['code'],
            'multi_country': self.multi_country,
            'sector_category': sector_category,
            'sector': sector['code'],
            'covid_19': 0, #self.covid_19, # COVID-19
            'fiscal_year': self.fiscal_year,
            'fiscal_quarter': "Q{}".format(self.fiscal_quarter)
        }


    def flatten_budget(self, as_dict=False):
        for sector in self.sectors:
            sector_category = get_sector_category(
                sector.get('code'),
                self.flattener.category_group)
            for country in self.countries:
                if (country['code'] not in self.flattener.countries):
                    continue
                for aid_type in self.aid_type:
                    for finance_type in self.finance_type:
                        for budget in self.budgets:
                            yield self.budget_data(
                                country, sector, sector_category,
                                aid_type, finance_type, budget, as_dict)


    def output_budget(self):
        for sector in self.sectors:
            sector_category = get_sector_category(
                sector.get('code'),
                self.flattener.category_group)
            for country in self.countries:
                if (country['code'] not in self.flattener.countries):
                    continue
                if country['code'] not in self.flattener.csv_files_budgets:
                    _file = open(
                        os.path.join(self.flattener.output_dir,
                            'csv',
                            'budget-{}.csv'.format(country['code'])),
                        'a')
                    self.flattener.csv_files_budgets[country['code']] = {
                        'file': _file,
                        'csv': csv.writer(_file),
                        'rows': []
                    }
                for aid_type in self.aid_type:
                    for finance_type in self.finance_type:
                        for budget in self.budgets:
                            self.flattener.csv_files_budgets[country['code']]['rows'].append(
                                self.budget_data(
                                    country, sector, sector_category,
                                    aid_type, finance_type, budget))


    def process_activity(self):
        activity = self.activity
        self.iati_identifier = self.activity.find('iati-identifier').text
        if self.iati_identifier not in self.flattener.activity_data:
            self.flattener.activity_data[self.iati_identifier] = {}
        activity_data = self.flattener.activity_data[self.iati_identifier]
        ActivityDataSetter(self)

        self.budgets = get_budgets(activity, self.currency_original, self.flattener.exchange_rates)

        activity_data_sectors = activity_data.get('sectors', [])
        if (len(activity_data_sectors) != 0):
            self.sectors = clean_sectors(activity_data_sectors)
        else:
            activity_data_sectors = activity.xpath("sector[not(@vocabulary) or @vocabulary='1']")
            if (len(activity_data_sectors) != 0):
                self.sectors = clean_sectors(activity_data_sectors)
            else:
                # Look at commitment transactions with DAC sectors
                transaction_data_sectors = get_sectors_from_transactions(
                    activity,
                    self.currency_original,
                    self.flattener.exchange_rates
                )
                self.sectors = transaction_data_sectors

        activity_data_countries = activity_data.get('recipient_countries', [])
        activity_data_regions = activity_data.get('recipient_regions', [])
        if (len(activity_data_countries) != 0) or (len(activity_data_regions) != 0):
            self.countries = clean_countries(activity_data_countries, activity_data_regions)
        else:
            activity_data_countries = activity.xpath('recipient-country')
            activity_data_regions = activity.xpath("recipient-region[not(@vocabulary) or @vocabulary='1']")
            if (len(activity_data_countries) != 0) or (len(activity_data_regions) != 0):
                self.countries = clean_countries(activity_data_countries, activity_data_regions)
            else:
                transaction_data_countries = get_countries_from_transactions(
                    activity,
                    self.currency_original,
                    self.flattener.exchange_rates
                )
                if len(transaction_data_countries) != 0:
                    self.countries = transaction_data_countries

        if not hasattr(self, 'countries'):
            return

        self.multi_country = {True: 1, False: 0}[len(self.countries)>1]

        if self.aid_type == {}:
            self.aid_type = get_aid_type_from_transactions(
                activity,
                self.currency_original,
                self.flattener.exchange_rates)
        else:
            self.aid_type = [{
                'code': self.aid_type.get('code', ''),
                'percentage': 100.0
            }]

        if self.finance_type == {}:
            self.finance_type = get_finance_type_from_transactions(
                activity,
                self.currency_original,
                self.flattener.exchange_rates)
        else:
            self.finance_type = [{
                'code': self.finance_type.get('code', ''),
                'percentage': 100.0
            }]

        self.provider_org = get_org(self.flattener.organisations, activity_data, activity)
        self.receiver_org = get_org(self.flattener.organisations, activity_data, activity, False)
        self.output = True


    def set_headers(self):
        for header in CSV_HEADERS:
            setattr(self, header, None)
        self.value_date = None
        self.output = False


    def __init__(self, flattener, activity):
        self.flattener = flattener
        self.activity = activity
        self.set_headers()
        self.process_activity()
