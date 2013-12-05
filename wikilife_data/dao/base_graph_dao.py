# coding=utf-8


class BaseGraphDAO(object):

    _logger = None
    _graph = None

    def __init__(self, logger, graph):
        self._logger = logger
        self._graph = graph
        self._initialize()

    def _initialize(self):
        pass
