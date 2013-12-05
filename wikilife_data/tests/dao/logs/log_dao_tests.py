# coding=utf-8

from wikilife_data.tests.base_test import BaseTest
from wikilife_utils.date_utils import DateUtils
import time

TEST_USER_ID = "TEST"


class LogDAOTests(BaseTest):

    _logs = None
    _dao = None

    def setUp(self):
        self._logs = self.get_db_conn().get_conn_logs().logs
        self._dao = self.get_dao_builder().build_log_dao()

    def tearDown(self):
        self._logs.remove({"test": True})
        #FIXME removing test logs when db running slow cause tests to fail
        self.wait_seconds(3, "teardown db remove") 
        self._logs = None
        self._dao = None

    def test_add_log(self):
        log = self._get_test_log()
        log_id = self._dao.add_log(log)
        found_log_cursor = self._logs.find({"id": log_id})

        assert found_log_cursor.count() == 1
        found_log = found_log_cursor.next()
        assert log_id > 0
        assert found_log != None
        assert found_log["test"]
        assert self._dao.add_log(log) > log_id

    def test_get_log_by_id(self):
        log = self._get_test_log()
        log_id = self._dao.add_log(log)

        found_log = self._dao.get_log_by_id(log_id)
        assert found_log != None
        assert found_log["test"]

    def test_get_user_logs_by_node_id(self):
        user_id = TEST_USER_ID
        node_id = 100
        nodes = [{"nodeId": node_id, "value": 5}]
        log = self._get_test_log(user_id, nodes)
        log_id = self._dao.add_log(log)

        found_logs_cursor = self._dao.get_user_logs_by_node_id(user_id, node_id)

        assert found_logs_cursor.count() > 0
        found_log = found_logs_cursor.next()
        assert found_log != None
        assert found_log["test"]
        assert found_log["id"] == log_id
        assert found_log["nodes"][0]["nodeId"] == node_id

    def test_get_last_user_log_by_node_id(self):
        user_id = TEST_USER_ID
        node_id = 200
        last_log_node_value = 3
        
        self._dao.add_log(self._get_test_log(user_id, [{"nodeId" : node_id, "value" : 1}], DateUtils.create_datetime(2012, 6, 15, 1), DateUtils.create_datetime(2012, 6, 15, 1)))
        self._dao.add_log(self._get_test_log(user_id, [{"nodeId" : node_id, "value" : 2}], DateUtils.create_datetime(2012, 6, 15, 2), DateUtils.create_datetime(2012, 6, 15, 2)))
        last_log_id = self._dao.add_log(self._get_test_log(user_id, [{"nodeId" : node_id, "value" : last_log_node_value}], DateUtils.create_datetime(2012, 6, 16, 0), DateUtils.create_datetime(2012, 6, 16, 0)))
        self._dao.add_log(self._get_test_log(user_id, [{"nodeId" : node_id, "value" : 4}], DateUtils.create_datetime(2012, 6, 15, 3), DateUtils.create_datetime(2012, 6, 15, 3)))

        found_log = self._dao.get_last_user_log_by_node_id(user_id, node_id)
        
        assert found_log != None
        assert found_log["test"]
        assert found_log["id"] == last_log_id
        assert found_log["nodes"][0]["nodeId"] == node_id
        assert found_log["nodes"][0]["value"] == last_log_node_value

    """
    def test_get_logs_by_create_datetime_utc_range_asc(self):
        pass
    """


    """ helpers """

    def _get_test_log(self, user_id=TEST_USER_ID, nodes=[], start=None, end=None):
        return {
            "id": 0,
            "origId": 0,
            "oper": "i",
            "createUTC": DateUtils.get_datetime_utc(),
            "source": "log_dao.tests",
            "category": "test",
            "userId": user_id,
            "start": start,
            "end": end,
            "text": "test log id=%s" %id,
            "nodes": nodes,
            "test": True
        }

    def wait_seconds(self, seconds, msg=""):
        print "sleeping %s sec %s..." % (seconds, msg)
        time.sleep(seconds)
