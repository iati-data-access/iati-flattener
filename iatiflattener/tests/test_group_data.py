import pytest
from iatiflattener.group_data import GroupFlatIATIData


class TestGroupData():
    @pytest.mark.parametrize("codelist_name, language, code, item_name", [
        ("ActivityStatus", "en", '1', "Pipeline/identification"),
        ("ActivityStatus", "fr", '1', "Planification"),
        ("ReportingOrganisation", "en", 'XM-DAC-41119', "United Nations Population Fund"),
        ("ReportingOrganisation", "fr", 'XM-DAC-41119', None)
    ])
    def test_get_codelist_with_fallback(self, codelist_name, language, code, item_name):
        gfd = GroupFlatIATIData
        req = gfd.get_codelist_with_fallback(gfd, lang=language, codelist_name=codelist_name)
        assert req.status_code == 200
        data = req.json()
        item = next(filter(lambda codelistitem: codelistitem['code'] == code, data['data']) )
        assert item['name'] == item_name
