import os
import datetime
import pytest
from iatiflattener import model
from lxml import etree
import json


def write_outputs(publisher, activity, flat_activity_json):
    with open('iatiflattener/tests/artefacts/{}-activity.json'.format(publisher), 'w') as json_file:
        json_file.write(activity.jsonify())


@pytest.mark.parametrize("publisher", ["fcdo", "canada", "usaid", "worldbank"])
class TestModel():

    @pytest.fixture()
    def node(self, publisher):
        doc = etree.parse('iatiflattener/tests/fixtures/{}-activity.xml'.format(publisher))
        yield doc.xpath('//iati-activity')[0]

    @pytest.fixture
    def activity(self, node):
        activity_cache = model.ActivityCache()
        _activity = model.Activity(node,
            activity_cache, {}, ['en', 'fr'])
        _activity.generate()
        return _activity

    @pytest.fixture
    def flat_activity(self, activity):
        return activity.as_csv_dict()

    def test_as_dict(self, publisher, activity):
        with open('iatiflattener/tests/artefacts/{}-activity.json'.format(publisher), 'r') as json_file:
            assert activity.jsonify() == json_file.read()

    def test_titles(self, publisher, flat_activity):
        """
        Confirm that titles are being parsed correctly
        """
        assert 'title#en' in flat_activity
        assert 'title#fr' in flat_activity
        if publisher == 'canada':
            assert flat_activity['title#en'] == 'Partnerships for Municipal Innovation in Local Economic Development'
            assert flat_activity['title#fr'] == 'Partenariats pour l’innovation municipale au service du développement économique local'

    def test_dates(self, publisher, flat_activity):
        if publisher == 'canada':
            assert flat_activity['start_date'] == '2016-01-20'
            assert flat_activity['end_date'] == '2021-09-30'

    def test_locations(self, publisher, flat_activity):
        if publisher == 'worldbank':
            assert flat_activity['location'] == '250441'

    def test_hrp(self, publisher, flat_activity):
        if publisher == 'worldbank':
            assert flat_activity['HRP'] == 'HCOVD20'

    def test_glide(self, publisher, flat_activity):
        if publisher == 'worldbank':
            assert flat_activity['GLIDE'] == 'EP-2020-000012-001'

    def _test_write_outputs(self, publisher, activity, flat_activity):
        """
        Remove the underscore to enable writing, if you update any of the outputs.
        """
        write_outputs(publisher, activity, flat_activity)

