# coding=utf-8

from bulbs.model import Node, Relationship
from bulbs.property import Integer, Float, String, DateTime 
from bulbs.utils import current_datetime

META_NODE = "MetaNode"
NUMERIC_METRIC_NODE = "NumericMetricNode"
TEXT_METRIC_NODE = "TextMetricNode"


class Is(Relationship):
    label = "is"
    created = DateTime(default=current_datetime, nullable=False)


class Has(Relationship):
    label = "has"
    created = DateTime(default=current_datetime, nullable=False)
    metric_id = Integer()
    metric_value = String()


class MeasuredBy(Relationship):
    label = "measured_by"
    created = DateTime(default=current_datetime, nullable=False)

class MeasuredByDefault(Relationship):
    label = "measured_by_default"
    created = DateTime(default=current_datetime, nullable=False)

class BaseNode(Node):
    element_type = "BaseNode"
    orig_id = Integer(nullable=False)
    name = String(nullable=False)

    def __str__(self):
        return "%s{_id: %s, orig_id: %s, name: '%s'}" %(self.element_type, self._id, self.orig_id, self.name)

    def __repr__(self):
        return self.__str__()


class MetaNode(BaseNode):
    element_type = META_NODE
    other_names = String(nullable=False)


class NumericMetricNode(BaseNode):
    """
    orig_id is tree vn_id or 0
    """
    element_type = NUMERIC_METRIC_NODE
    min = Float(nullable=False)
    max = Float(nullable=False)
    default = Float(nullable=False)
    unit = String(nullable=False)
    precision = Integer(default=0)


class TextMetricNode(BaseNode):
    """
    orig_id is tree vn_id or 0
    """
    element_type = TEXT_METRIC_NODE
    options = String(nullable=False) #comma delimited string
    default = Integer(default=0)
