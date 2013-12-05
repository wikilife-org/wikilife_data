# coding=utf-8

from bulbs.neo4jserver.graph import Graph
from wikilife_data.tests.base_test import BaseTest
from wikilife_data.dao.meta.meta_dao import MetaDAO
from wikilife_data.model.meta import Is, Has

class MetaDAOWriteTests(BaseTest):

    _dao = None

    def setUp(self):
        g = Graph()
        self._dao = MetaDAO(self.get_logger(), g)
        #self._dao._graph.clear()

    def tearDown(self):
        #self._dao._graph.clear()
        self._dao = None

    def test_create_node(self):
        name = "t1"
        orig_id = 1
        node = self._dao.create_node(name, orig_id)

        assert node != None
        assert node.name == name
        assert node.orig_id == orig_id

    def test_update_node(self):
        name = "a"
        name_upd = "a_upd"
        orig_id = 123
        node = self._dao.create_node(name, orig_id)
        self._dao.update_node(node, name=name_upd)
        node_upd = self._dao._graph.vertices.get(node._id)

        assert node_upd != None
        assert node_upd._id == node._id
        assert node_upd.name == name_upd
        assert node_upd.orig_id == orig_id
        
        try:
            self._dao._graph.node.index.lookup("name", name).next()
            assert False
        except:
            assert True

    def test_delete_node(self):
        node = self._dao.create_node("t2")
        node_id = node._id
        self._dao.delete_node(node)
        node_del = self._dao._graph.vertices.get(node_id)
        
        assert node_del == None

    def test_delete_node_by_id(self):
        node = self._dao.create_node("t2")
        node_id = node._id
        self._dao.delete_node_by_id(node_id)
        node_del = self._dao._graph.vertices.get(node_id)
        
        assert node_del == None

    def test_create_rel_is(self):
        node = self._dao.create_node("a", 0)
        parent_node = self._dao.create_node("b", 1)
        rel = self._dao.create_rel_is(node, parent_node)

        assert rel != None
        assert node.outV(Is.label).next()==parent_node
    
    """
    def test_delete_rel_by_id(self):
        node = self._dao.create_node("a", 0)
        parent_node = self._dao.create_node("b", 1)
        rel_is = self._dao.create_rel_is(node, parent_node)
        self._dao.delete_rel_by_id(rel_is._id)

        assert self._dao._graph.edges.get(rel_is._id) == None 

        node = self._dao.create_node("c", 2)
        component_node = self._dao.create_node("d", 3)
        rel_has = self._dao.create_rel_has(node, component_node)
        self._dao.delete_rel_by_id(rel_has._id)

        assert self._dao._graph.edges.get(rel_has._id) == None 
    """

    def test_delete_rel(self):
        node = self._dao.create_node("a", 0)
        parent_node = self._dao.create_node("b", 1)
        rel_is = self._dao.create_rel_is(node, parent_node)
        self._dao.delete_rel(node, parent_node)

        assert self._dao._graph.edges.get(rel_is._id) == None
        
        node = self._dao.create_node("c", 2)
        component_node = self._dao.create_node("d", 3)
        rel_has = self._dao.create_rel_has(node, component_node)
        self._dao.delete_rel(node, component_node)

        assert self._dao._graph.edges.get(rel_has._id) == None
