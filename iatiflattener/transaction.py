from lxml import etree
import csv
import os

from iatiflattener.activity import ActivityDataSetter
from iatiflattener.lib.utils import get_date, get_fy_fq
from iatiflattener.lib.iati_helpers import get_sector_category, clean_countries, clean_sectors, get_org, get_covid_matches
from iatiflattener.lib.variables import CSV_HEADERS

class FlatIATITransaction():

    def transaction_data(self, country, sector, sector_category, as_dict=False):

        ActivityDataSetter(self)

        if self.value_date == None:
            raise Exception("No value date for {}: {}".format(
                self.iati_identifier, etree.tostring(self.transaction)))
        if self.currency_original == None:
            raise Exception("No currency for {}: {}".format(
                self.iati_identifier, etree.tostring(self.transaction)))
        if self.value_original == None:
            raise Exception("No value for {}: {}".format(
                self.iati_identifier, etree.tostring(self.transaction)))

        if self.currency_original == 'USS': self.currency_original = 'USD'
        closest_exchange_rate = self.flattener.exchange_rates.closest_rate(
            self.currency_original, self.value_date
        )
        self.exchange_rate = closest_exchange_rate.get('conversion_rate')
        self.value_usd = self.value_original / self.exchange_rate

        value_original = (
            self.value_original*(
                country['percentage']/100
            )*(
                sector['percentage']/100
            )
        )
        value_usd = (
            self.value_usd*(
                country['percentage']/100
            )*(
                sector['percentage']/100
            )
        )

        out = [
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
            value_original,
            self.currency_original,
            value_usd,
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
        ]

        if as_dict==False:
            return out

        return {
            'iati_identifier': self.iati_identifier,
            'title': self.title,
            'reporting_org': self.reporting_org,
            'aid_type': self.aid_type.get('code', ''),
            'finance_type': self.finance_type.get('code', ''),
            'provider_org': self.provider_org,
            'receiver_org': self.receiver_org,
            'transaction_type': self.transaction_type,
            'value_original': value_original,
            'currency_original': self.currency_original,
            'value_usd': value_usd,
            'value_date': self.value_date.isoformat(),
            'exchange_rate': self.exchange_rate,
            'transaction_date': self.transaction_date,
            'country': country['code'],
            'multi_country': self.multi_country,
            'sector_category': sector_category,
            'sector': sector['code'],
            'covid_19': self.covid_19,
            'fiscal_year': self.fiscal_year,
            'fiscal_quarter': self.fiscal_quarter
        }


    def flatten_transaction(self, as_dict=False):
        for sector in self.sectors:
            sector_category = get_sector_category(
                sector.get('code'),
                self.flattener.category_group)
            for country in self.countries:
                if (country['code'] not in self.flattener.countries):
                    continue
                yield self.transaction_data(country, sector, sector_category, as_dict)


    def output_transaction(self):
        for sector in self.sectors:
            sector_category = get_sector_category(
                sector.get('code'),
                self.flattener.category_group)
            for country in self.countries:
                if (country['code'] not in self.flattener.countries):
                    continue
                if country['code'] not in self.flattener.csv_files_transactions:
                    _file = open(
                        os.path.join(self.flattener.output_dir,
                            'csv',
                            '{}.csv'.format(country['code'])),
                        'a')
                    self.flattener.csv_files_transactions[country['code']] = {
                        'file': _file,
                        'csv': csv.writer(_file),
                        'rows': []
                    }
                self.flattener.csv_files_transactions[country['code']]['rows'].append(
                    self.transaction_data(country, sector, sector_category)
                )


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

        if not hasattr(self, 'countries'):
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
        if self.limit_transaction_types:
            if self.transaction_type not in ['1', '2', '3', '4']: return

        self.aid_type = transaction.find('aid-type')
        self.finance_type = transaction.find('finance-type')

        self.provider_org = get_org(self.flattener.organisations, activity_data, transaction)
        self.receiver_org = get_org(self.flattener.organisations, activity_data, transaction, False)

        self.value_original = float(transaction.find('value').text)
        self.currency_original = transaction.find('value').get('currency')

        self.value_date = get_date(transaction.find('value').get('value-date'))
        self.transaction_date = transaction.find('transaction-date').get('iso-date')
        self.fiscal_year, self.fiscal_quarter = get_fy_fq(self.transaction_date)
        self.covid_19 = int(get_covid_matches(transaction))
        self.output = True

    def set_headers(self):
        for header in CSV_HEADERS:
            setattr(self, header, None)
        self.value_date = None
        self.output = False

    def __init__(self, flattener, activity, transaction,
            limit_transaction_types=True):
        self.limit_transaction_types = limit_transaction_types
        self.flattener = flattener
        self.activity = activity
        self.transaction = transaction
        self.set_headers()
        self.process_transaction()
