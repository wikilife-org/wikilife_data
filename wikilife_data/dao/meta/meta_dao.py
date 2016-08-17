# coding=utf-8

from wikilife_data.dao.base_graph_dao import BaseGraphDAO
from wikilife_data.model.meta import Is, Has, MetaNode, MeasuredBy, \
    NumericMetricNode, TextMetricNode, MeasuredByDefault


class MetaDAOException(Exception):
    pass


class MetaDAO(BaseGraphDAO):

    def _initialize(self):
        self._graph.add_proxy("meta_node", MetaNode)
        self._graph.add_proxy("numeric_metric_node", NumericMetricNode)
        self._graph.add_proxy("text_metric_node", TextMetricNode)
        self._graph.add_proxy("isa", Is)
        self._graph.add_proxy("has", Has)
        self._graph.add_proxy("measured_by", MeasuredBy)
        self._graph.add_proxy("measured_by_default", MeasuredByDefault)

    def get_node_by_id(self, node_id):
        """
        node_id: Integer
        """
        node = self._graph.vertices.get(node_id)
        return node

    def get_nodes_by_ids(self, node_ids):
        """
        :param node_ids: Meta Node ids.
        :type node: list

        :rtype: list
        """
        params = {}
        params["node_ids"] = node_ids

        nodes = self._graph.cypher.query("START n=node({node_ids}) RETURN n;", params)
        return list(nodes) if nodes else []

    def get_node_metrics(self, node_id):
        """
        node_id: Integer
        """
        params = {}
        params["node_id"] = node_id

        metrics = self._graph.cypher.query("START n=node({node_id}) MATCH n-[:measured_by]->m RETURN m;", params)
        return list(metrics) if metrics else []
    
    def get_node_metrics_ids(self, node_id):
        """
        node_id: Integer
        """
        metrics_ids = []

        for metric in self.get_node_metrics(node_id):
            metrics_ids.append(metric._id)

        return metrics_ids

    '''
    def get_metrics(self, meta_node):
        """
        Returns List of Metric nodes.

        :param meta_node: Meta Node.
        :type node: wikilife_data.model.meta.MetaNode

        :rtype: list
        """
        params = {}
        params["node_id"] = meta_node._id

        metric_nodes = self._graph.cypher.query("START n=node({node_id}) MATCH n-[:measured_by]->m RETURN m ORDER BY m.name;", params)
        return list(metric_nodes) if metric_nodes else []
    '''

    def get_default_metrics(self, node_id):
        """
        Returns Default Metric node.

        :param node_id: Meta Node ID.
        :type node: int

        :rtype: list
        """
        params = {}
        params["node_id"] = node_id

        metric_nodes = self._graph.cypher.query("START n=node({node_id}) MATCH n-[:measured_by_default]->m RETURN m ORDER BY m.name;", params)
        return list(metric_nodes) if metric_nodes else []

    def get_nodes_measured_by(self, metric_id, limit=100):
        """
        Returns Meta nodes measured by a Metric.

        :param metric_id: Metric Node ID.
        :type node: int

        :rtype: list
        """
        params = {}
        params["metric_id"] = metric_id
        params["l"] = limit

        meta_nodes = self._graph.cypher.query("START m=node({metric_id}) MATCH n-[:measured_by]->m RETURN n ORDER BY n.name LIMIT {l};", params)
        return list(meta_nodes) if meta_nodes else []

    def get_node_by_orig_id(self, orig_id):
        """
        :param orig_id: Node original tree id.
        :type node: int

        :rtype: MetaNode
        """
        params = {}
        params["orig_id"] = orig_id

        nodes = self._graph.cypher.query("START n=node:MetaNode(orig_id={orig_id}) RETURN n;", params)

        try:
            node = nodes.next()
        except:
            node = None

        return node

    def get_metric_by_orig_id(self, orig_id):
        """
        :param orig_id: Value Node original tree id.
        :type node: int

        :rtype: NumericMetricNode or TextMetricNode
        """
        params = {}
        params["orig_id"] = orig_id
        
        nodes = self._graph.cypher.query("START n=node(*) WHERE HAS(n.orig_id) AND n.orig_id={orig_id} RETURN n;", params)

        try:
            node = nodes.next()
        except:
            node = None

        return node
    
    def find_exact_nodes(self, node_name, skip, limit):
        """
        :param name: Node name.
        :type node: str

        :rtype: tupple
        """
        params = {}
        params["name_regex"] = "(?i).%s." %node_name
        params["s"] = skip
        params["l"] = limit

        items_total = self._graph.cypher.table("START n=node:MetaNode('name:*') WHERE n.name=~{name_regex} OR n.other_names=~{name_regex} RETURN count(*) as total;", params)[1][0][0]
        items  = self._graph.cypher.query     ("START n=node:MetaNode('name:*') WHERE n.name=~{name_regex} OR n.other_names=~{name_regex} RETURN n ORDER BY n.name SKIP {s} LIMIT {l};", params)

        if items:
            items = list(items)
        else:
            items = []

        return list(items), items_total
    
    def find_nodes(self, node_name, skip, limit):
        """
        :param name: Node name.
        :type node: str

        :rtype: tupple
        """
        params = {}
        params["name_regex"] = "(?i).*%s.*" %node_name
        params["s"] = skip
        params["l"] = limit

        items_total = self._graph.cypher.table("START n=node:MetaNode('name:*') WHERE n.name=~{name_regex} OR n.other_names=~{name_regex} RETURN count(*) as total;", params)[1][0][0]
        items  = self._graph.cypher.query     ("START n=node:MetaNode('name:*') WHERE n.name=~{name_regex} OR n.other_names=~{name_regex} RETURN n ORDER BY n.name SKIP {s} LIMIT {l};", params)

        if items:
            items = list(items)
        else:
            items = []

        return list(items), items_total

    def get_children(self, node_id, limit=100):
        params = {}
        params["node_id"] = node_id
        params["l"] = limit

        items  = self._graph.cypher.query("START n=node({node_id}) MATCH c-[:is]->n RETURN c ORDER BY c.name LIMIT {l};", params) 
        return list(items) if items else []

    def get_children_pag(self, node_id, skip, limit):
        params = {}
        params["node_id"] = node_id
        params["s"] = skip
        params["l"] = limit

        items_total = self._graph.cypher.table("START n=node({node_id}) MATCH c-[:is]->n RETURN count(c) as total;", params)[1][0][0]
        items = self._graph.cypher.query("START n=node({node_id}) MATCH c-[:is]->n RETURN c ORDER BY c.name SKIP {s} LIMIT {l};", params) 
        items = list(items) if items else []

        return items, items_total

    '''
    def get_children(self, node):
        """
        node: MetaNode
        """
        #return list(node.inV())
        return self.get_children_by_id(node._id)

    def get_children_by_id(self, node_id):
        #return self.get_children(self.get_node_by_id(node_id))
        nodes = self._graph.cypher.query("START n=node(%s) MATCH c-[:is]->n RETURN c;" %node_id)
        return list(nodes)
    '''

    def get_parent_nodes(self, node):
        """
        Returns parent nodes. List<MetaNode>.
        Originally get_parent_node 

        :param node: Graph node.
        :type node: wikilife_data.model.meta.MetaNode

        :rtype: list
        """
        return self.get_parent_nodes_by_id(node._id)

    def get_parent_nodes_by_id(self, node_id):
        params = {}
        params["node_id"] = node_id

        nodes = self._graph.cypher.query("START n=node({node_id}) MATCH n-[:is]->p RETURN p ORDER BY p.name;", params)
        return list(nodes) if nodes else [] 

    def get_component_nodes(self, node_id):
        params = {}
        params["node_id"] = node_id

        nodes = self._graph.cypher.query("START n=node({node_id}) MATCH n-[:has]->p RETURN p ORDER BY p.name;", params)
        return list(nodes) if nodes else [] 

    def get_descendants_count(self, node_id):
        params = {}
        params["node_id"] = node_id
        items_total = self._graph.cypher.table("START n=node({node_id}) MATCH m-[:is*..100]->n RETURN count(*) as total;", params)[1][0][0]
        return items_total

    def get_parent_ids_by_id(self, node_id):
        """
        node = self.get_node_by_id(node_id)
        parent_ids = []

        for parent in node.outV(Is.label):
            parent_ids.append(parent._id)
        
        """
        params = {}
        params["node_id"] = node_id

        parent_ids = []
        r = self._graph.cypher.query("START n=node({node_id}) MATCH n-[:is]->p RETURN p;", params)
        if r:
            for parent in r:
                parent_ids.append(parent._id)

        return parent_ids

    def get_ancestors_ids(self, node_id):
        params = {}
        params["node_id"] = node_id

        parent_ids = []
        r = self._graph.cypher.query("START n=node({node_id}) MATCH n-[:is*..10]->p RETURN p;", params)
        for parent in r:
            parent_ids.append(parent._id)

        return parent_ids

    def get_ancestors(self, node_id):
        params = {}
        params["node_id"] = node_id

        nodes = self._graph.cypher.query("START n=node({node_id}) MATCH n-[:is*..10]->p RETURN p;", params)
        return list(nodes) if nodes else [] 

    def get_ancestors_ids_measured_by(self, node_id, metric_id):
        params = {}
        params["node_id"] = node_id
        params["metric_id"] = metric_id

        ids = []
        r = self._graph.cypher.query("START n=node({node_id}), m=node({metric_id}) MATCH n-[:is*..10]->p-[:measured_by]->m RETURN p;", params)
        if r:
            for ancestor in r:
                ids.append(ancestor._id)

        return ids

    def get_top_nodes(self):
        nodes = self._graph.cypher.query("START n=node:MetaNode('name:*') MATCH n-[r?:is]->p WHERE r is null RETURN n;")
        return list(nodes)

    """
    def get_children_by_starting_letter(self, parent_id, letter, limit=1000):
        query = 'START n=node:MetaNode("name:%s*"), p=node(%s) MATCH n-[:is]->p RETURN n LIMIT %s;' %(letter, parent_id, limit)
        r = self._graph.cypher.query(query)
    """

    def create_meta_node(self, name, orig_id, other_names):
        node = self._graph.meta_node.create(orig_id=orig_id, name=name, other_names=other_names)
        return node

    def create_numeric_metric_node(self, name, orig_id, min, max, unit, default, precision=0):
        node = self._graph.numeric_metric_node.create(name=name, orig_id=orig_id, min=min, max=max, unit=unit, default=default, precision=precision)
        return node

    def create_text_metric_node(self, name, orig_id, options, default=0):
        node = self._graph.text_metric_node.create(name=name, orig_id=orig_id, options=options, default=default)
        return node

    def create_rel_is(self, node, parent_node):
        rel_is = self._graph.isa.create(node, parent_node)
        return rel_is

    def create_rel_has(self, node, component_node, metric_id=None, metric_value=None):
        rel_props = {
            "metric_id": metric_id, 
            "metric_value": metric_value
        }
        rel_has = self._graph.has.create(node, component_node, rel_props)
        return rel_has

    def create_rel_measured_by(self, meta_node, metric_node):
        rel_measured_by = self._graph.measured_by.create(meta_node, metric_node)
        return rel_measured_by

    def create_rel_measured_by_default(self, meta_node, metric_node):
        rel_measured_by_default = self._graph.measured_by_default.create(meta_node, metric_node)
        return rel_measured_by_default

    def update_node(self, node, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(node, k, v)

        node.save()

    def delete_node(self, node_id):
        params = {}
        params["node_id"] = node_id
        self._graph.cypher.execute("START n=node({node_id}) MATCH n-[r]-() DELETE n, r;", params)

    def delete_rel(self, node, related_node):
        rel = self._get_rel(node, related_node)
        self._delete_rel_by_id(rel._id)

    def delete_rel_is(self, node_id, related_node_id):
        params = {}
        params["node_id"] = node_id
        params["related_node_id"] = related_node_id
        self._graph.cypher.execute("START n=node({node_id}), rn=node({related_node_id}) MATCH n-[r?:is]-rn DELETE r;", params)

    def _delete_rel_by_id(self, rel_id):
        self._graph.edges.delete(rel_id)

    def _get_rel(self, node, related_node):
        for e in node.outE():
            if e.inV()._id == related_node._id:
                return e

        raise MetaDAOException("No relationship between %s and %s" %(node._id, related_node._id))
    
    '''
    Removed
    def create_root_node(self, namespace):
    def delete_node_logical(self, node):
    
    Replaced by create_node, create_rel_*
    def create_meta_node(self, namespace, loggable=False, extra_fields=None, extra_fields_fields=None):
    def _create_node(self, namespace, node_type):
    
    Replaced by metrics in create/update_node
    def create_property_node(self, namespace, default_property=False):
    def create_range_value_node(self, namespace, min_value, max_value, range_step, default=None, value_unit="", default_value=False):
    def create_text_value_node(self, namespace, options, default=None, default_value=False):
    def create_datetime_value_node(self, namespace, default_value=False):
    def _create_value_node(self, namespace):
    '''
