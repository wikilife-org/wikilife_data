# coding=utf-8

from wikilife_data.tests.base_test import BaseTest


class LocationDAOTests(BaseTest):

    _dao = None

    def test_search_location_by_name(self):
        
        loc_name = "piltriquitrón"
        
        dao = self.get_dao_builder().build_location_dao()
        result = dao.search_location_by_name(loc_name)
        
        print result
        assert result != None
        assert len(result) == 1
