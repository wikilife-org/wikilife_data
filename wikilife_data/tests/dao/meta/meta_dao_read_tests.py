# coding=utf-8

from bulbs.neo4jserver.graph import Graph
from wikilife_data.tests.base_test import BaseTest
from wikilife_data.dao.meta.meta_dao import MetaDAO

class MetaDAOReadTests(BaseTest):

    _dao = None

    def setUp(self):
        g = Graph()
        self._dao = MetaDAO(self.get_logger(), g)
        #self._dao._graph.clear()
        #self._create_test_graph()

    def tearDown(self):
        #self._dao._graph.clear()
        self._dao = None

    def test_get_node_by_name(self):
        name = "a1"
        node = self._dao.get_node_by_name(name)

        assert node != None
        assert node.name == name

    def test_get_node_by_id(self):
        node_id = self._dao.get_node_by_name("a1")._id
        node = self._dao.get_node_by_id(node_id)

        assert node != None
        assert node._id == node_id

    def test_get_node_by_orig_id(self):
        orig_id = 101
        node = self._dao.get_node_by_orig_id(orig_id)

        assert node != None
        assert node.orig_id == orig_id

    def test_get_children(self):
        node = self._dao.get_node_by_name("a1")
        children = self._dao.get_children(node)

        assert children != None
        print children

    def test_get_children_by_id(self):
        node_id = self._dao.get_node_by_name("a1")._id
        ids = self._dao.get_children_by_id(node_id)

        assert ids != None
        print ids

    def test_get_parent_nodes(self):
        node = self._dao.get_node_by_name("d1")
        parent_nodes = self._dao.get_parent_nodes(node)

        assert parent_nodes != None
        assert len(parent_nodes) == 3
        print parent_nodes

    def test_get_ancestors_ids(self):
        node = self._dao.get_node_by_name("c111")
        parent_nodes = self._dao.get_ancestors_ids(node._id)

        assert parent_nodes != None
        assert len(parent_nodes) == 7
        print parent_nodes

    def test_get_top_nodes(self):
        node_id = self._dao.get_node_by_name("c111")._id
        top_nodes = self._dao.get_top_nodes(node_id)

        assert top_nodes != None
        assert len(top_nodes) == 3
        print top_nodes


    """ Helpers """

    def _create_node(self, name, orig_id, parent_node=None):
        node = self._dao.create_node(name, orig_id)

        if parent_node:
            if isinstance(parent_node, list):
                for pn in parent_node:
                    self._dao.create_rel_is(node, pn)
                    print "%s(%s) --> %s(%s)" %(node.name, node._id, pn.name, pn._id)
            else:
                self._dao.create_rel_is(node, parent_node)
                print "%s(%s) --> %s(%s)" %(node.name, node._id, parent_node.name, parent_node._id)

        else:
            print "%s(%s)" %(node.name, node._id)
            
        return node 
    
    def _create_test_graph(self):
        a1 = self._create_node("a1", 101)
        a11 = self._create_node("a11", 1011, a1)
        a111 = self._create_node("a111", 10111, a11)
        a112 = self._create_node("a112", 10112, a11)
        a113 = self._create_node("a113", 10113, a11)
        a12 = self._create_node("a12", 1012, a1)
        a121 = self._create_node("a121", 10121, a12)
        a13 = self._create_node("a13", 1013, a1)

        b1 = self._create_node("b1", 201)
        b11 = self._create_node("b11", 2011, b1)
        b111 = self._create_node("b111", 20111, b11)
        b12 = self._create_node("b12", 2012, b1)
        
        c1 = self._create_node("c1", 301)
        c11 = self._create_node("c11", 3011, c1)
        c111 = self._create_node("c111", 30111, [c11, a13, b111])
        
        d1 = self._create_node("d1", 401, [a1, b1, c1])
