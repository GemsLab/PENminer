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
import random
from scipy.stats import entropy as scipy_entropy

class TestP(unittest.TestCase):
    def W(self, occs, interval_width):
        return (occs[-1] - occs[0] + 1) / (interval_width + 1)

    def F(self, occs):
        return log10(len(occs) + 1)

    def H(self, occs):
        p = np.diff(occs)
        p = p / sum(p)
        return scipy_entropy(p, base=2)
        
    def S(self, occs):
        if len(set(occs)) <= 2: # first or second occ
            return 1
        occs = sorted(set(occs))
        return self.H(occs) / log(len(occs) - 1) + 1

    def P(self, occs, interval_width, alpha=1, beta=1, gamma=1):
        return (self.W(occs, interval_width) ** alpha) * (self.F(occs) ** beta) * (self.S(occs) ** gamma)

    def random_update(self, timestamp, max_node=100):
        u = str(random.randint(0, max_node))
        v = str(random.randint(0, max_node))
        u = ('1', u, v, '1', u, v, '1', timestamp)
        return u

    def random_stream(self, num_updates=100, max_node=100, max_time_between_occ=10):
        updates = list()
        timestamp = 1
        for i in range(num_updates):
            timestamp += random.randint(0, max_time_between_occ)
            updates.append(self.random_update(timestamp, max_node=max_node))
        ts = updates[0][-1]
        te = updates[-1][-1]
        interval_width = te - ts
        return updates, interval_width

    def test_P_1(self):
        method = sPENminer(None, window_size=3, max_size=1)
        method_offline = oPENminer(None, window_size=3, max_size=1)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1)]

        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
        method_offline.compute_persistence()
        _P = self.P([1], interval_width=0)
        assert(_P == method_offline.Ps['1_1_2_1'])
        assert(_P == method.old_Ps.get('1_1_2_1') == method.query('1_1_2_1'))

    def test_P_2(self):
        method = sPENminer(None, window_size=3, max_size=1)
        method_offline = oPENminer(None, window_size=3, max_size=1)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '1', '2', '1', '1', '2', '1', 2)]

        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
        method_offline.compute_persistence()
        _P = self.P([1, 2], interval_width=1)
        assert(_P == method_offline.Ps['1_1_2_1'])
        assert(_P == method.old_Ps.get('1_1_2_1') == method.query('1_1_2_1'))

    def test_P_3(self):
        method = sPENminer(None, window_size=3, max_size=1)
        method_offline = oPENminer(None, window_size=3, max_size=1)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '1', '2', '1', '1', '2', '1', 2), ('1', '1', '2', '1', '1', '2', '1', 3)]

        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
        method_offline.compute_persistence()
        _P = self.P([1, 2, 3], interval_width=2)
        assert(_P == method_offline.Ps['1_1_2_1'])
        assert(_P == method.old_Ps.get('1_1_2_1') == method.query('1_1_2_1'))

    def test_P_4(self):
        '''
        Test that persistence is correctly computed when the update occurs not every second.
        '''
        method = sPENminer(None, window_size=3, max_size=1)
        method_offline = oPENminer(None, window_size=3, max_size=1)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1),
                   ('1', '1', '2', '1', '1', '2', '1', 2),
                   ('1', '3', '4', '1', '3', '4', '1', 3),
                   ('1', '1', '2', '1', '1', '2', '1', 5)]

        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
        method_offline.compute_persistence()
        _P = self.P([1, 2, 5], interval_width=5-1)
        assert(_P == method_offline.Ps['1_1_2_1'])
        assert(_P == method.old_Ps.get('1_1_2_1'))

    def test_P_5(self):
        method = sPENminer(None, window_size=3, max_size=2)
        method_offline = oPENminer(None, window_size=3, max_size=1)
        random.seed(0)
        updates, interval_width = self.random_stream(max_node=10)
        pattern_to_occs = defaultdict(list)
        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
            pattern = method.extractor.singletons[-1][-1]
            pattern_to_occs[pattern].append(update[-1])
            assert(abs(method.old_Ps.get(pattern) - method.query(pattern)) < 0.000000001)
        method_offline.compute_persistence()
        for pattern, occs in pattern_to_occs.items():
            _P = self.P(occs, interval_width)
            assert(abs(method_offline.Ps[pattern] - method.query(pattern)) < 0.00000001)

    def test_P_6(self):
        method = sPENminer(None, window_size=3, max_size=2)
        method_offline = oPENminer(None, window_size=3, max_size=1)
        random.seed(1)
        updates, interval_width = self.random_stream(max_node=100)
        pattern_to_occs = defaultdict(list)
        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
            pattern = method.extractor.singletons[-1][-1]
            pattern_to_occs[pattern].append(update[-1])
            assert(abs(method.old_Ps.get(pattern) - method.query(pattern)) < 0.000000001)
        method_offline.compute_persistence()
        for pattern, occs in pattern_to_occs.items():
            _P = self.P(occs, interval_width)
            assert(_P == method_offline.Ps[pattern])
            assert(abs(method_offline.Ps[pattern] - method.query(pattern)) < 0.00000001)

    def test_P_7(self):
        method = sPENminer(None, window_size=3, max_size=3)
        method_offline = oPENminer(None, window_size=3, max_size=3)
        random.seed(2)
        updates, interval_width = self.random_stream(max_node=20)
        pattern_to_occs = defaultdict(list)
        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
            pattern = method.extractor.singletons[-1][-1]
            pattern_to_occs[pattern].append(update[-1])
            assert(abs(method.old_Ps.get(pattern) - method.query(pattern)) < 0.000000001)
        method_offline.compute_persistence()
        for pattern, occs in pattern_to_occs.items():
            _P = self.P(occs, interval_width)
            assert(_P == method_offline.Ps[pattern])
            assert(abs(method_offline.Ps[pattern] - method.query(pattern)) < 0.00000001)

    def test_P_8(self):
        '''
        Test with gamma > 1.
        '''
        gamma = 3
        method = sPENminer(None, window_size=3, max_size=3, gamma=gamma)
        method_offline = oPENminer(None, window_size=3, max_size=3, gamma=gamma)
        random.seed(3)
        updates, interval_width = self.random_stream(max_node=20, num_updates=1000)
        pattern_to_occs = defaultdict(list)
        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
            pattern = method.extractor.singletons[-1][-1]
            pattern_to_occs[pattern].append(update[-1])
            assert(abs(method.old_Ps.get(pattern) - method.query(pattern)) < 0.000000001)
        method_offline.compute_persistence()
        for pattern, occs in pattern_to_occs.items():
            _P = self.P(occs, interval_width, gamma=gamma)
            assert(_P == method_offline.Ps[pattern])
            assert(abs(method_offline.Ps[pattern] - method.query(pattern)) < 0.00000001)

    def test_P_9(self):
        '''
        Test with alpha > 1.
        '''
        alpha = 3
        method = sPENminer(None, window_size=3, max_size=3, alpha=alpha)
        method_offline = oPENminer(None, window_size=3, max_size=3, alpha=alpha)
        random.seed(3)
        updates, interval_width = self.random_stream(max_node=20, num_updates=1000)
        pattern_to_occs = defaultdict(list)
        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
            pattern = method.extractor.singletons[-1][-1]
            pattern_to_occs[pattern].append(update[-1])
            assert(abs(method.old_Ps.get(pattern) - method.query(pattern)) < 0.000000001)
        method_offline.compute_persistence()
        for pattern, occs in pattern_to_occs.items():
            _P = self.P(occs, interval_width, alpha=alpha)
            assert(_P == method_offline.Ps[pattern])
            assert(abs(method_offline.Ps[pattern] - method.query(pattern)) < 0.00000001)

    def test_P_10(self):
        '''
        Test with 0 < alpha < 1.
        '''
        for num_updates in [1000, 2000]:
            alpha = 0.25
            method = sPENminer(None, window_size=3, max_size=3, alpha=alpha)
            method_offline = oPENminer(None, window_size=3, max_size=3, alpha=alpha)
            random.seed(3)
            updates, interval_width = self.random_stream(max_node=20, num_updates=num_updates)
            pattern_to_occs = defaultdict(list)
            for update in updates:
                method.process_update(update)
                method_offline.process_update(update)
                pattern = method.extractor.singletons[-1][-1]
                pattern_to_occs[pattern].append(update[-1])
                assert(abs(method.old_Ps.get(pattern) - method.query(pattern)) < 0.000000001)
            method_offline.compute_persistence()
            for pattern, occs in pattern_to_occs.items():
                _P = self.P(occs, interval_width, alpha=alpha)
                assert(_P == method_offline.Ps[pattern])
                assert(abs(method_offline.Ps[pattern] - method.query(pattern)) < 0.00000001)

    def test_P_11(self):
        '''
        Test with beta > 1.
        '''
        for num_updates in [1000, 2000]:
            beta = 3
            method = sPENminer(None, window_size=3, max_size=3, beta=beta)
            method_offline = oPENminer(None, window_size=3, max_size=3, beta=beta)
            random.seed(3)
            updates, interval_width = self.random_stream(max_node=20, num_updates=num_updates)
            pattern_to_occs = defaultdict(list)
            for update in updates:
                method.process_update(update)
                method_offline.process_update(update)
                pattern = method.extractor.singletons[-1][-1]
                pattern_to_occs[pattern].append(update[-1])
                assert(abs(method.old_Ps.get(pattern) - method.query(pattern)) < 0.000000001)
            method_offline.compute_persistence()
            for pattern, occs in pattern_to_occs.items():
                _P = self.P(occs, interval_width, beta=beta)
                assert(_P == method_offline.Ps[pattern])
                assert(abs(method_offline.Ps[pattern] - method.query(pattern)) < 0.00000001)

    def test_P_12(self):
        '''
        Test with 0 < beta < 1.
        '''
        beta = 0.25
        method = sPENminer(None, window_size=3, max_size=3, beta=beta)
        method_offline = oPENminer(None, window_size=3, max_size=3, beta=beta)
        random.seed(3)
        updates, interval_width = self.random_stream(max_node=20, num_updates=1000)
        pattern_to_occs = defaultdict(list)
        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
            pattern = method.extractor.singletons[-1][-1]
            pattern_to_occs[pattern].append(update[-1])
            assert(abs(method.old_Ps.get(pattern) - method.query(pattern)) < 0.000000001)
        method_offline.compute_persistence()
        for pattern, occs in pattern_to_occs.items():
            _P = self.P(occs, interval_width, beta=beta)
            assert(_P == method_offline.Ps[pattern])
            assert(abs(method_offline.Ps[pattern] - method.query(pattern)) < 0.00000001)

    def test_P_13(self):
        '''
        Test with 0 < beta < 1.
        '''
        beta = 0.025
        method = sPENminer(None, window_size=3, max_size=3, beta=beta)
        method_offline = oPENminer(None, window_size=3, max_size=3, beta=beta)
        random.seed(3)
        updates, interval_width = self.random_stream(max_node=10, num_updates=2000)
        pattern_to_occs = defaultdict(list)
        for update in updates:
            method.process_update(update)
            method_offline.process_update(update)
            pattern = method.extractor.singletons[-1][-1]
            pattern_to_occs[pattern].append(update[-1])
            assert(abs(method.old_Ps.get(pattern) - method.query(pattern)) < 0.000000001)
        method_offline.compute_persistence()
        for pattern, occs in pattern_to_occs.items():
            _P = self.P(occs, interval_width, beta=beta)
            assert(_P == method_offline.Ps[pattern])
            assert(abs(method_offline.Ps[pattern] - method.query(pattern)) < 0.00000001)


if __name__ == "__main__":
    unittest.main()
