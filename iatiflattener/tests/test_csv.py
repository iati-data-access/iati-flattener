import os
import datetime
import pytest
import exchangerates
import iatiflattener
from iatiflattener import model
from iatiflattener import group_data
from lxml import etree
import csv
import shutil
import openpyxl

exchange_rates = exchangerates.CurrencyConverter(
    update=False, source="iatiflattener/tests/fixtures/rates.csv")

assert "GBP" in exchange_rates.known_currencies()

countries_currencies = {'BD': 'BDT'}


class TestModel():

    @pytest.fixture(scope='class')
    def flattener(self):
        shutil.rmtree('output_test/')
        yield iatiflattener.FlattenIATIData(
            refresh_rates=False,
            iatikitcache_dir='',
            output='output_test',
            publishers=None,
            langs=['en', 'fr'],
            run_publishers=False
        )


    @pytest.fixture(scope='class')
    def package(self, flattener):
        flattener.process_package(None, f"fcdo-activity.xml",
            'iatiflattener/tests/fixtures')


    @pytest.fixture()
    def csvfile(self, package):
        with open('output_test/csv/transaction-LR.csv', 'r') as _csv_file:
            yield csv.DictReader(_csv_file)


    @pytest.fixture(scope='class')
    def group(self, package):
        group_data.GroupFlatIATIData(
            langs=['en', 'fr'],
            output_folder='output_test'
            )


    @pytest.fixture
    def groupedfile_en(self, package, group):
        yield openpyxl.load_workbook(filename='output_test/xlsx/en/LR.xlsx')


    @pytest.fixture
    def groupedfile_fr(self, package, group):
        yield openpyxl.load_workbook(filename='output_test/xlsx/fr/LR.xlsx')


    def test_row(self, flattener, package, csvfile):
        row = next(csvfile)
        assert row == {
            'iati_identifier': 'GB-1-103662-101',
            'title#en': 'PROCOFSERVICES and P0220 for Civil Ser. Cap. Bldng. Liberia',
            'title#fr': 'PROCOFSERVICES and P0220 for Civil Ser. Cap. Bldng. Liberia',
            'reporting_org#en': 'UK - Foreign, Commonwealth and Development Office [GB-GOV-1]',
            'reporting_org#fr': 'Royaume-Uni – Ministère des Affaires étrangères, du Commonwealth et du Développement [GB-GOV-1]',
            'reporting_org_type': '10', 'aid_type': 'C01', 'finance_type': '110',
            'flow_type': '10',
            'provider_org#en': 'UK - Foreign, Commonwealth and Development Office [GB-GOV-1]',
            'provider_org#fr': 'Royaume-Uni – Ministère des Affaires étrangères, du Commonwealth et du Développement [GB-GOV-1]',
            'provider_org_type': '', 'receiver_org#en': 'Adam Smith International [GB-COH-2732176]',
            'receiver_org#fr': 'Adam Smith International [GB-COH-2732176]', 'receiver_org_type': '70',
            'transaction_type': '3', 'value_original': '1232.0', 'currency_original': 'GBP',
            'value_usd': '1796.2559999999992', 'exchange_rate_date': '2010-05-31',
            'exchange_rate': '0.685871056241427', 'value_eur': '1459.5400991305746',
            'value_local': '128432.30399999995',
            'transaction_date': '2010-05-27', 'country_code': 'LR', 'multi_country': '0',
            'sector_category': '150', 'sector_code': '15110', 'humanitarian': '0', 'fiscal_year': '2010',
            'fiscal_quarter': 'Q2',
            'fiscal_year_quarter': '2010 Q2', 'url': 'https://d-portal.org/q.html?aid=GB-1-103662-101'
        }


    def test_group_headers_en(self, flattener, package, groupedfile_en):
        headers_row = [cell.value for cell in next(groupedfile_en.worksheets[0].iter_rows(min_row=1, max_row=1))]
        assert headers_row == ['IATI Identifier', 'Title', 'Reporting Organisation', 'Reporting Organisation Type', 'Aid Type', 'Finance Type', 'Flow Type', 'Provider Organisation', 'Provider Organisation Type', 'Receiver Organisation', 'Receiver Organisation Type', 'Transaction Type', 'Recipient Country or Region', 'Multi Country', 'Sector Category', 'Sector', 'Humanitarian', 'Calendar Year', 'Calendar Quarter', 'Calendar Year and Quarter', 'URL', 'Value (USD)', 'Value (EUR)', 'Value (Local currrency)']

    def test_first_row_en(self, flattener, package, groupedfile_en):
        first_row = [cell.value for cell in next(groupedfile_en.worksheets[0].iter_rows(min_row=2, max_row=2))]
        assert first_row ==  ['GB-1-103662-101', 'PROCOFSERVICES and P0220 for Civil Ser. Cap. Bldng. Liberia', 'UK - Foreign, Commonwealth and Development Office [GB-GOV-1]', '10 - Government', 'C01 - Project-type interventions', '110 - Standard grant', '10 - ODA', 'UK - Foreign, Commonwealth and Development Office [GB-GOV-1]', 'No data', 'Adam Smith International [GB-COH-2732176]', '70 - Private Sector', '3 - Disbursement', 'LR - Liberia', 0, '150 - Government & Civil Society', '15110 - Public sector policy and administrative management', 0, 2010, 'Q2', '2010 Q2', 'https://d-portal.org/q.html?aid=GB-1-103662-101', 1796.256, 1459.54009913057, 128432.304]

    def test_group_headers_fr(self, flattener, package, groupedfile_fr):
        headers_row = [cell.value for cell in next(groupedfile_fr.worksheets[0].iter_rows(min_row=1, max_row=1))]
        assert headers_row == ['Identifiant de l’IITA', 'Titre', 'Organisme déclarant', 'Type d’organisme déclarant', 'Type d’aide', 'Type de financement', 'Type de flux', 'Organisme prestataire', 'Type d’organisme prestataire', 'Organisme bénéficiaire', 'Type d’organisme bénéficiaire', 'Type de transaction', 'Pays ou région bénéficiaire', 'Multipays', 'Catégorie de secteur', 'Secteur', 'Humanitaire', 'Année civile', 'Trimestre civil', 'Année et trimestre civils', 'URL', 'Valeur (USD)', 'Valeur (EUR)', 'Valeur (Monnaie locale)']

    def test_first_row_fr(self, flattener, package, groupedfile_fr):
        first_row = [cell.value for cell in next(groupedfile_fr.worksheets[0].iter_rows(min_row=2, max_row=2))]
        assert first_row == ['GB-1-103662-101', 'PROCOFSERVICES and P0220 for Civil Ser. Cap. Bldng. Liberia', 'Royaume-Uni – Ministère des Affaires étrangères, du Commonwealth et du ' 'Développement [GB-GOV-1]', '10 - Gouvernement', 'C01 - Interventions de type projet', '110 - Dons ordinaires', '10 - APD', 'Royaume-Uni – Ministère des Affaires étrangères, du Commonwealth et du ' 'Développement [GB-GOV-1]', 'Aucune donnée', 'Adam Smith International [GB-COH-2732176]', '70 - Secteur privé', '3 - Décaissement', 'LR - Libéria (le)', 0, '150 - Gouvernement & Société Civile', '15110 - Politiques publiques et gestion administrative', 0, 2010, 'Q2', '2010 Q2', 'https://d-portal.org/q.html?aid=GB-1-103662-101', 1796.256, 1459.54009913057, 128432.304]
