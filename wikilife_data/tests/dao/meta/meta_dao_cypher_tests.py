# coding=utf-8

from bulbs.neo4jserver.graph import Graph
from wikilife_data.tests.base_test import BaseTest
from wikilife_data.dao.meta.meta_dao import MetaDAO

class MetaDAOCypherTests(BaseTest):

    _dao = None

    def setUp(self):
        g = Graph()
        self._dao = MetaDAO(self.get_logger(), g)
        #self._dao._graph.clear()
        #self._create_test_graph()

    def tearDown(self):
        #self._dao._graph.clear()
        self._dao = None

    def test_get_children_by_starting_letter(self):
        parent_id = 602
        letter = "V"
        limit = 3
        children = self._dao.get_children_by_starting_letter(parent_id, letter, limit)

        assert children != None
