import os
import datetime
import pytest
import exchangerates
from iatiflattener import model
from lxml import etree

exchange_rates = exchangerates.CurrencyConverter(update=False, source="iatiflattener/tests/fixtures/rates.csv")

assert "GBP" in exchange_rates.known_currencies()

countries_currencies = {'AE': 'AED', 'AO': 'AOA', 'AR': 'ARS', 'BD': 'BDT', 'LR': 'LRD', 'AT': 'EUR'}


def write_outputs(publisher, transaction, flat_transaction_json):
    with open('iatiflattener/tests/artefacts/{}-transaction.json'.format(publisher), 'w') as json_file:
        json_file.write(transaction.jsonify())
    with open('iatiflattener/tests/artefacts/{}-transaction-flat.json'.format(publisher), 'w') as json_file:
        json_file.write(flat_transaction_json)


class TestModel:

    @pytest.fixture()
    def node(self, publisher):
        doc = etree.parse('iatiflattener/tests/fixtures/{}-activity.xml'.format(publisher))
        yield doc.xpath('//transaction')[0]

    @pytest.fixture
    def transaction(self, node):
        activity_cache = model.ActivityCache()
        _transaction_node = node
        _activity_node = _transaction_node.getparent()
        _transaction = model.Transaction(_activity_node, _transaction_node, activity_cache,
                                         exchange_rates, countries_currencies)
        _transaction.generate()
        return _transaction

    @pytest.fixture
    def flat_transaction_first_item(self, transaction):
        _flat_transaction = model.FlatTransaction(transaction)
        return list(_flat_transaction.flatten())[0]

    @pytest.fixture
    def transaction_flattened_as_list(self, transaction):
        _flat_transaction = model.FlatTransaction(transaction)
        return list(_flat_transaction.flatten())

    @pytest.fixture
    def flat_transaction_json(self, transaction):
        _flat_transaction = model.FlatTransaction(transaction)
        return list(_flat_transaction.flatten_json())[0]

    @pytest.mark.parametrize("publisher", ["fcdo", "canada", "usaid", "usaid-humanitarian", "ifad"])
    def test_as_dict(self, publisher, transaction):
        with open('iatiflattener/tests/artefacts/{}-transaction.json'.format(publisher), 'r') as json_file:
            assert transaction.jsonify() == json_file.read()

    @pytest.mark.parametrize("publisher", ["fcdo", "canada", "usaid", "usaid-humanitarian", "ifad"])
    def test_flat_transaction(self, publisher, transaction, flat_transaction_json):
        with open('iatiflattener/tests/artefacts/{}-transaction-flat.json'.format(publisher), 'r') as json_file:
            assert flat_transaction_json == json_file.read()

    @pytest.mark.parametrize("publisher", ["fcdo", "canada", "usaid", "usaid-humanitarian", "ifad"])
    def test_flat_transaction_usd_values(self, transaction, flat_transaction_first_item):
        """
        Confirm that the flat transaction value is the USD value *
        country percentage * sector percentage
        """
        country = flat_transaction_first_item['country_code']
        transaction_dict = transaction.as_dict()
        country_pct = list(filter(lambda _country: _country['code'] == country, transaction_dict['countries']))[0]['percentage']
        sector = flat_transaction_first_item['sector_code']
        sector_pct = list(filter(lambda _sector: _sector['code'] == sector, transaction_dict['sectors']))[0]['percentage']
        assert flat_transaction_first_item['value_usd'] == transaction_dict['value_usd'] * (country_pct / 100) * (sector_pct / 100)

    @pytest.mark.parametrize("publisher", ["fcdo", "canada", "usaid", "usaid-humanitarian", "ifad"])
    def test_humanitarian(self, publisher, transaction, flat_transaction_first_item):
        """Checks to see if humanitarian transactions are set correctly"""
        if publisher == 'usaid-humanitarian':
            assert transaction.humanitarian.transaction.get('humanitarian', False) in ('1', True)
            assert flat_transaction_first_item['humanitarian'] == 1
        else:
            assert transaction.humanitarian.transaction.get('humanitarian', False) in ('0', False)
            assert flat_transaction_first_item['humanitarian'] == 0

    @pytest.mark.parametrize("publisher", ["fcdo", "canada", "usaid", "usaid-humanitarian", "ifad"])
    def _test_write_outputs(self, publisher, transaction, flat_transaction_json):
        """
        Remove the underscore to enable writing, if you update any of the outputs.
        """
        write_outputs(publisher, transaction, flat_transaction_json)

    @pytest.mark.parametrize("publisher", ["fcdo", "canada", "ifad", "gdihub", "finddiagnostics", "beis"])
    def test_transaction_value_usd(self, publisher, transaction):
        """Tests the `value_usd` field on transactions for the first transaction in each file"""

        expected_data = {'fcdo': 1232 / 0.726269155,  # GBP
                         'canada': 1664612 / 1.2617,  # CAD
                         'ifad': 9480000 / 0.702121,  # XDR
                         'gdihub': 56826.74 / 0.726269155,  # GBP
                         "finddiagnostics": 71189586,   # USD
                         "beis": 5172459.0 / 0.726269155}  # GBP

        assert transaction.value_usd.value == expected_data[publisher]

    @pytest.mark.parametrize("publisher", ["fcdo", "canada", "ifad", "gdihub", "finddiagnostics", "beis"])
    def test_transaction_value_original(self, publisher, transaction):
        """Tests the `value_original` field on transactions for the first transaction in each file"""

        expected_data = {'fcdo': 1232,  # GBP
                         'canada': 1664612,  # CAD
                         'ifad': 9480000,  # XDR
                         'gdihub': 56826.74,  # GBP
                         "finddiagnostics": 71189586,   # USD
                         "beis": 5172459.0}  # GBP

        assert transaction.value_original.value == expected_data[publisher]

    @pytest.mark.parametrize("publisher", ["fcdo", "ifad", "unops-at"])
    def test_transaction_value_local(self, publisher, transaction):
        """Tests the `value_original` field on transactions for the first transaction in each file"""

        expected_data = {'fcdo': {'LR': 1232 / 0.726269155 * 171.7972},  # GBP -> USD -> LRD
                         'ifad': {'LR': 9480000 / 0.702121 * 171.7972},  # XDR -> USD -> LRD
                         'unops-at': {'AT': 1321549 * 0.845022816}  # GBP
                         }

        assert transaction.value_local.value == expected_data[publisher]

    @pytest.mark.parametrize("publisher", ["fcdo", "finddiagnostics"])
    def test_flat_transaction_value_usd(self, publisher, transaction, transaction_flattened_as_list):
        """Tests the `value_usd` field on transactions for the few flattened transaction items"""

        expected_data = {'fcdo': {'LR': {'15110': 1232 / 0.726269155}},
                         "finddiagnostics": {'AO': {'32182': (71189586 / 100) * 0.18 * 0.72,
                                                    '12210': (71189586 / 100) * 0.18 * 0.28},
                                             'AE': {'32182': (71189586 / 100) * 0.01 * 0.72,
                                                    '12210': (71189586 / 100) * 0.01 * 0.28},
                                             'AR': {'32182': (71189586 / 100) * 0.01 * 0.72,
                                                    '12210': (71189586 / 100) * 0.01 * 0.28},
                                             'AT': {'32182': (71189586 / 100) * 0.0001 * 0.72,
                                                    '12210': (71189586 / 100) * 0.0001 * 0.28},
                                             }}

        for country_code in expected_data[publisher]:
            for sector_code in expected_data[publisher][country_code]:
                for transaction_entry in transaction_flattened_as_list:
                    if transaction_entry['country_code'] == country_code \
                       and transaction_entry['sector_code'] == sector_code:
                        assert (transaction_entry['value_usd'] ==
                                pytest.approx(expected_data[publisher][country_code][sector_code]))

    @pytest.mark.parametrize("publisher", ["fcdo", "finddiagnostics"])
    def test_flat_transaction_value_original(self, publisher, transaction,
                                             transaction_flattened_as_list):
        """Tests the `value_original` field on transactions for the few flattened transaction items"""

        expected_data = {'fcdo': {'LR': {'15110': 1232}},  # original currency here is GBP
                         "finddiagnostics": {'AO': {'32182': 71189586 * 0.0018 * 0.72,   # same as value_usd test
                                                    '12210': 71189586 * 0.0018 * 0.28},  # b/c original is USD
                                             'AE': {'32182': 71189586 * 0.0001 * 0.72,
                                                    '12210': 71189586 * 0.0001 * 0.28},
                                             'AR': {'32182': 71189586 * 0.0001 * 0.72,
                                                    '12210': 71189586 * 0.0001 * 0.28},
                                             'AT': {'32182': 71189586 * 0.000001 * 0.72,
                                                    '12210': 71189586 * 0.000001 * 0.28},
                                             }}

        for country_code in expected_data[publisher]:
            for sector_code in expected_data[publisher][country_code]:
                for transaction_entry in transaction_flattened_as_list:
                    if transaction_entry['country_code'] == country_code \
                       and transaction_entry['sector_code'] == sector_code:
                        assert (transaction_entry['value_original'] ==
                                pytest.approx(expected_data[publisher][country_code][sector_code]))

    @pytest.mark.parametrize("publisher", ["fcdo", "finddiagnostics"])
    def test_flat_transaction_value_local(self, publisher, transaction, transaction_flattened_as_list):
        """Tests the `value_local` field on transactions for the first transaction in each file"""

        # fcdo: value_original is in GBP, so convert to US dollars, then to Liberian dollars. there is only a single
        #       country or sector, so no splitting
        # finddiagnostics: value_original is in USD, so just split by country, then
        #                  sector, then multiply by the appropriate currency for
        #                  value_local (Angolan Kwanza, UAE Dirham, Argentine Peso, and
        #                  EUR respectively)
        expected_data = {'fcdo': {'LR': {'15110': (1232 / 0.726269155) * 171.7972}},
                         "finddiagnostics": {'AO': {'32182': 71189586 * 0.0018 * 0.72 * 642.44255,
                                                    '12210': 71189586 * 0.0018 * 0.28 * 642.44255},
                                             'AE': {'32182': 71189586 * 0.0001 * 0.72 * 3.6725,
                                                    '12210': 71189586 * 0.0001 * 0.28 * 3.6725},
                                             'AR': {'32182': 71189586 * 0.0001 * 0.72 * 97.64,
                                                    '12210': 71189586 * 0.0001 * 0.28 * 97.64},
                                             'AT': {'32182': 71189586 * 0.000001 * 0.72 * 0.845022816,
                                                    '12210': 71189586 * 0.000001 * 0.28 * 0.845022816},
                                             }}

        for country_code in expected_data[publisher]:
            for sector_code in expected_data[publisher][country_code]:
                for transaction_entry in transaction_flattened_as_list:
                    if transaction_entry['country_code'] == country_code \
                       and transaction_entry['sector_code'] == sector_code:
                        assert (transaction_entry['value_local'][country_code] ==
                                pytest.approx(expected_data[publisher][country_code][sector_code]))

