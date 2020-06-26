import unittest
import sys
import numpy as np
from collections import defaultdict
sys.path.append('../src/')
from stream import Stream
from sPENminer import sPENminer
from oPENminer import oPENminer
from math import log2 as log
from math import log10

class TestMethod(unittest.TestCase):
    def test_init_1(self):
        '''
        Test init.
        '''
        stream = Stream('../test/disconnected_then_connected')
        method = sPENminer(stream, 3, 3)
        assert(method)
        method.stream.f.close()

    def test_continuous_age_1(self):
        '''
        Test continuous age is maintained.
        '''
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '4', '1', '3', '4', '1', 10)]
        for i, update in enumerate(updates):
            method.process_update(update)
            if i == 0:
                assert(method.time == 1)
            else:
                assert(method.time == 10)

    def test_process_update_1(self):
        '''
        Test that process_update handles disconnected sequences correctly.
        '''
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1'), ('1', '3', '4', '1', '3', '4', '1')]
        for update in updates:
            method.process_update(update)
        assert(len(method.num_occs.keys()) == 2)

    def test_process_update_2(self):
        '''
        Test that process_update handles connected sequences correctly.
        '''
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1'), ('1', '3', '1', '1', '3', '1', '1')]
        for update in updates:
            method.process_update(update)
        assert(len(method.num_occs.keys()) == 3)

    def test_process_update_3(self):
        '''
        Test that process_update handles window size properly.
        '''
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1'),
                   ('1', '3', '4', '1', '3', '4', '1'),
                   ('1', '5', '6', '1', '5', '6', '1'),
                   ('1', '7', '8', '1', '7', '8', '1')]

        for t, update in enumerate(updates):
            method.process_update(update)
        assert(len(method.num_occs.keys()) == 4)

    def test_process_update_4(self):
        '''
        Test that process_update handles disconnected sequences that become connected.
        '''
        method = sPENminer(None, window_size=4, max_size=4)
        updates = [('1', '1', '2', '1', '1', '2', '1'), ('1', '3', '4', '1', '3', '4', '1'), ('1', '3', '1', '1', '3', '1', '1')]

        for t, update in enumerate(updates):
            method.process_update(update)
        assert(len(method.num_occs.keys()) == 6)
        assert('1_1_2_1|1_3_1_1' in method.num_occs.keys())

    def test_process_update_5(self):
        '''
        Test that process_update handles age correctly for disconnected sequences that become connected.
        '''
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1'), ('1', '3', '4', '1', '3', '4', '1'), ('1', '3', '1', '1', '3', '1', '1'), ('1', '5', '6', '1', '5', '6', '1')]

        for t, update in enumerate(updates):
            method.process_update(update)
        assert(len(method.num_occs.keys()) == 7)

if __name__ == "__main__":
    unittest.main()
