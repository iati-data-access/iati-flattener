import pytest
from iatiflattener import FlattenIATIData


class TestFlattenIATIData():
    @pytest.mark.parametrize("lang, code, name", [
        ("en", 'XM-DAC-41119', "United Nations Population Fund"),
        ("fr", 'XM-DAC-41119', "United Nations Population Fund"), # uses EN fallback
        ("en", '44000', "The World Bank"),
        ("fr", '44000', "Banque mondiale")
    ])
    def test_setup_codelists(self, lang, code, name):
        fid = FlattenIATIData
        fid.langs = ['en', 'fr']
        fid.get_exchange_rates = lambda a: a
        fid.setup_codelists(fid, False)
        assert fid.organisations[lang][code] == name
