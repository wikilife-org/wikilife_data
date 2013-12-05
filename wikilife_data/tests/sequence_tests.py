# coding=utf-8

from wikilife_data.tests.base_test import BaseTest
from wikilife_data.sequence import Sequence

class SequenceTests(BaseTest):

    sequence = None
    db = None

    def setUp(self):
        #self.db = self.get_conn().test_sequence
        self.db = self.get_conn().wikilife
        self.db.seq.drop()
        self.sequence = Sequence(self.db, "my_coll")

    def tearDown(self):
        self.db.seq.drop()

    def test_get_name(self):
        assert self.sequence.get_name() == "my_coll"

    def test_current_value(self):
        assert self.sequence.current_value() == 0

    def test_next_value(self):
        assert self.sequence.current_value() == 0
        assert self.sequence.next_value() == 1
        assert self.sequence.next_value() == 2
        assert self.sequence.next_value() == 3

    def test_reset_to(self):
        sequence = self.sequence
        
        sequence.next_value()
        sequence.next_value()
        sequence.next_value()
        
        sequence.reset_to(1)
        assert sequence.current_value() == 1
        sequence.next_value()
        assert sequence.current_value() == 2
        
        sequence.next_value()
        sequence.next_value()
        sequence.reset_to(0)
        assert sequence.current_value() == 0
        sequence.next_value()
        assert sequence.current_value() == 1
        
        sequence.reset_to(5)
        assert sequence.current_value() == 5
        sequence.next_value()
        assert sequence.current_value() == 6
        
        sequence.reset_to(1000)
        assert sequence.current_value() == 1000
        sequence.next_value()
        assert sequence.current_value() == 1001
        
    def test_reset_to_zero(self):
        sequence = self.sequence
        
        sequence.next_value()
        sequence.next_value()
        sequence.next_value()

        sequence.reset_to_zero()
        assert sequence.current_value() == 0

        sequence.next_value()
        sequence.next_value()
        sequence.next_value()

        sequence.reset_to_zero()
        assert sequence.current_value() == 0
        
        sequence.reset_to(10)
        sequence.reset_to_zero()
        assert sequence.current_value() == 0

        sequence.next_value()
        sequence.next_value()
        sequence.next_value()

        sequence.reset_to_zero()
        assert sequence.current_value() == 0
        
    