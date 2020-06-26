import unittest
import sys
import numpy as np
from collections import defaultdict
sys.path.append('../src/')
from stream import Stream
from sPENminer import sPENminer
from oPENminer import oPENminer

class TestExtractor(unittest.TestCase):
    def test_create_singleton_1(self):
        '''
        Test that create_singleton() correctly creates a singleton snippet.
        '''
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '4', '1', '3', '4', '1', 2)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.time = int(update[-1])
            if method.ts == 0:
                method.ts = method.time
            method.extractor.create_singleton(update)
            if i == 0:
                assert(len(method.extractor.singletons) == 1)
                assert(method.extractor.singletons[-1][0] == 1)
            if i == 1:
                assert(len(method.extractor.singletons) == 2)
                assert(method.extractor.singletons[-1][0] == 2)

    def test_compatability_links_1(self):
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '4', '1', '3', '4', '1', 2), ('1', '5', '2', '1', '5', '2', '1', 3)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.extractor.create_singleton(update)
            method.extractor.create_size_2_snippets(update)
            if i == 0:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 1)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 1:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 2:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 3)
                assert(method.extractor.compatability_links[-1] == [2])

    def test_compatability_links_2(self):
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '4', '1', '3', '4', '1', 2), ('1', '5', '2', '1', '5', '2', '1', 3), ('1', '3', '2', '1', '3', '2', '1', 4), ('1', '3', '2', '1', '3', '2', '1', 5)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.extractor.create_singleton(update)
            method.extractor.create_size_2_snippets(update)
            method.time = update[-1]
            if i == 0:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 1)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 1:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 2:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 3)
                assert(method.extractor.compatability_links[-1] == [2])
            if i == 3:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 4)
                assert(set(method.extractor.compatability_links[-1]) == {1, 2, 3})
            if i == 4:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 4)
                assert(set(method.extractor.compatability_links[-1]) == {1, 2, 3})

    def test_create_size_2_snippets_1(self):
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '4', '1', '3', '4', '1', 2)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.extractor.create_singleton(update)
            method.extractor.create_size_2_snippets(update)
            if i == 0:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 1)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 1:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [])

    def test_create_size_2_snippets_2(self):
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '1', '1', '3', '1', '1', 2)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.extractor.create_singleton(update)
            method.extractor.create_size_2_snippets(update)
            if i == 0:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 1)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 1:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [1])

    def test_create_size_2_snippets_3(self):
        method = sPENminer(None, window_size=2, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '1', '1', '3', '1', '1', 2), ('1', '3', '4', '1', '3', '4', '1', 4)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.time = update[-1]
            method.extractor.create_singleton(update)
            method.extractor.create_size_2_snippets(update)
            if i == 0:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 1)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 1:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [1])
            if i == 2:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [1])

    def test_create_size_2_snippets_4(self):
        method = sPENminer(None, window_size=2, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '1', '1', '3', '1', '1', 2), ('1', '5', '4', '1', '5', '4', '1', 4)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.time = update[-1]
            method.extractor.create_singleton(update)
            method.extractor.create_size_2_snippets(update)
            if i == 0:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 1)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 1:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [1])
            if i == 2:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [])

    def test_create_size_2_snippets_5(self):
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '1', '1', '3', '1', '1', 2), ('1', '5', '4', '1', '5', '4', '1', 3)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.time = update[-1]
            method.extractor.create_singleton(update)
            method.extractor.create_size_2_snippets(update)
            if i == 0:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 1)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 1:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [1])
            if i == 2:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 3)
                assert(method.extractor.compatability_links[-1] == [])
                assert(method.extractor.compatability_links[-2] == [1])

    def test_create_size_2_snippets_6(self):
        method = sPENminer(None, window_size=3, max_size=3)
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '1', '1', '3', '1', '1', 2), ('1', '5', '4', '1', '5', '4', '1', 3), ('1', '3', '1', '1', '3', '1', '1', 5)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.time = update[-1]
            method.extractor.create_singleton(update)
            method.extractor.create_size_2_snippets(update)
            if i == 0:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 1)
                assert(method.extractor.compatability_links[-1] == [])
            if i == 1:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 2)
                assert(method.extractor.compatability_links[-1] == [1])
            if i == 2:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 3)
                assert(method.extractor.compatability_links[-1] == [])
                assert(method.extractor.compatability_links[-2] == [1])
            if i == 3:
                assert(len(method.extractor.singletons) == len(method.extractor.compatability_links) == 3)
                assert(method.extractor.compatability_links[-1] == [2])
                assert(method.extractor.compatability_links[-2] == [])
                assert(method.extractor.compatability_links[-3] == [1])

    def test_create_size_3_snippets_1(self):
        method = sPENminer(None, window_size=3, max_size=3, view='id')
        updates = [('1', '1', '2', '1', '1', '2', '1', 1), ('1', '3', '1', '1', '3', '1', '1', 2), ('1', '5', '1', '1', '5', '1', '1', 3)]
        new = set()
        method.old_freq_of_current = dict() # needed for book keeping
        method.freq_of_current = dict() # needed for book keeping
        for i, update in enumerate(updates):
            method.extractor.create_singleton(update)
            method.extractor.create_size_2_snippets(update)
            method.extractor.create_size_3_snippets()
            if i == 0:
                assert(len(list(method.first_occs.keys())) == 1)
            if i == 1:
                assert(len(list(method.first_occs.keys())) == 3)
            if i == 2:
                assert(set(method.extractor.compatability_links[-1]) == {1, 2})
                assert(set(method.extractor.compatability_links[-2]) == {1})
                assert(set(method.extractor.compatability_links[-3]) == set())
                assert('1_1_2_1|1_3_1_1|1_5_1_1' in method.first_occs)
                assert(len(list(method.first_occs.keys())) == 7)

    def test_extract_sequences_1(self):
        method = oPENminer(None, window_size=3, max_size=4, view='id')
        updates = [('1', '1', '1', '1', '1', '1', '1')] * 100

        for t, update in enumerate(updates):
            t += 1
            method.time += 1
            method.extractor.extract_sequences(update)
        assert(len(method.extractor.singletons) == 4)

        assert(method.snippet_to_freq['1_1_1_1'] == 100)
        assert(method.snippet_to_freq['1_1_1_1|1_1_1_1'] == 294)
        assert(method.snippet_to_freq['1_1_1_1|1_1_1_1|1_1_1_1'] == 292)

    def test_extract_sequences_2(self):
        method = oPENminer(None, window_size=3, max_size=3, view='id')
        u1 = ('1', '1', '2', '1', '1', '2', '1')
        u2 = ('1', '3', '4', '1', '3', '4', '1')
        u3 = ('1', '1', '4', '1', '1', '4', '1')
        updates = [u1, u2, u3] * 100

        for t, update in enumerate(updates):
            t += 1
            method.time += 1
            method.extractor.extract_sequences(update)
        assert(len(method.extractor.singletons) == 4)

        assert(method.snippet_to_freq['1_1_2_1'] == 100)
        assert(method.snippet_to_freq['1_3_4_1'] == 100)
        assert(method.snippet_to_freq['1_1_4_1'] == 100)

        assert(method.snippet_to_freq['1_1_2_1|1_3_4_1'] == 0)
        assert(method.snippet_to_freq['1_1_2_1|1_1_4_1'] == 100)

        assert(method.snippet_to_freq['1_1_2_1|1_3_4_1|1_1_4_1'] == 100)

    def test_extract_sequences_3(self):
        method = oPENminer(None, window_size=3, max_size=3, view='id')
        u1 = ('1', '1', '2', '1', '1', '2', '1')
        u2 = ('1', '3', '1', '1', '3', '1', '1')
        u3 = ('1', '1', '4', '1', '1', '4', '1')
        updates = [u1, u2, u3] * 100

        for t, update in enumerate(updates):
            t += 1
            method.time += 1
            method.extractor.extract_sequences(update)
        assert(len(method.extractor.singletons) == 4)

        assert(method.snippet_to_freq['1_1_2_1'] == 100)
        assert(method.snippet_to_freq['1_3_1_1'] == 100)
        assert(method.snippet_to_freq['1_1_4_1'] == 100)

        assert(method.snippet_to_freq['1_1_2_1|1_3_1_1'] == 100)
        assert(method.snippet_to_freq['1_1_2_1|1_1_4_1'] == 100)
        assert(method.snippet_to_freq['1_3_1_1|1_1_4_1'] == 100)

        assert(method.snippet_to_freq['1_1_2_1|1_3_1_1|1_1_4_1'] == 100)
        assert(method.snippet_to_freq['1_3_1_1|1_1_4_1|1_1_2_1'] == 99)

    def test_extract_sequences_4(self):
        method = oPENminer(None, window_size=3, max_size=3, view='id')
        u1 = ('1', '1', '2', '1', '1', '2', '1')
        u2 = ('1', '3', '1', '1', '3', '1', '1')
        u3 = ('1', '3', '4', '1', '3', '4', '1')
        updates = [u1, u2, u3] * 100

        for t, update in enumerate(updates):
            t += 1
            method.time += 1
            method.extractor.extract_sequences(update)
        assert(len(method.extractor.singletons) == 4)

        assert(method.snippet_to_freq['1_1_2_1'] == 100)
        assert(method.snippet_to_freq['1_3_1_1'] == 100)
        assert(method.snippet_to_freq['1_3_4_1'] == 100)

        assert(method.snippet_to_freq['1_1_2_1|1_3_1_1'] == 100)
        assert(method.snippet_to_freq['1_3_1_1|1_3_4_1'] == 100)
        assert(method.snippet_to_freq['1_1_2_1|1_3_4_1'] == 0)

        assert(method.snippet_to_freq['1_1_2_1|1_3_1_1|1_3_4_1'] == 100)

    def test_extract_sequences_5(self):
        '''
        Max size = 1.
        '''
        method = oPENminer(None, window_size=0, max_size=1, view='id')
        u1 = ('1', '1', '2', '1', '1', '2', '1')
        u2 = ('1', '3', '1', '1', '3', '1', '1')
        u3 = ('1', '3', '4', '1', '3', '4', '1')
        updates = [u1, u2, u3] * 100

        for t, update in enumerate(updates):
            t += 1
            method.time += 1
            method.extractor.extract_sequences(update)
        assert(len(method.extractor.singletons) == 0)

        assert(method.snippet_to_freq['1_1_2_1'] == 100)
        assert(method.snippet_to_freq['1_3_1_1'] == 100)
        assert(method.snippet_to_freq['1_3_4_1'] == 100)

        assert(method.snippet_to_freq['1_1_2_1|1_3_1_1'] == 0)
        assert(method.snippet_to_freq['1_3_1_1|1_3_4_1'] == 0)
        assert(method.snippet_to_freq['1_1_2_1|1_3_4_1'] == 0)

        assert(method.snippet_to_freq['1_1_2_1|1_3_1_1|1_3_4_1'] == 0)

    def test_extract_sequences_6(self):
        '''
        Max size = 2.
        '''
        method = oPENminer(None, window_size=3, max_size=2, view='id')
        u1 = ('1', '1', '2', '1', '1', '2', '1')
        u2 = ('1', '3', '1', '1', '3', '1', '1')
        u3 = ('1', '3', '4', '1', '3', '4', '1')
        updates = [u1, u2, u3] * 100

        for t, update in enumerate(updates):
            t += 1
            method.time += 1
            method.extractor.extract_sequences(update)
        assert(len(method.extractor.singletons) == 4)

        assert(method.snippet_to_freq['1_1_2_1'] == 100)
        assert(method.snippet_to_freq['1_3_1_1'] == 100)
        assert(method.snippet_to_freq['1_3_4_1'] == 100)

        assert(method.snippet_to_freq['1_1_2_1|1_3_1_1'] == 100)
        assert(method.snippet_to_freq['1_3_1_1|1_3_4_1'] == 100)
        assert(method.snippet_to_freq['1_1_2_1|1_3_4_1'] == 0)

        assert(method.snippet_to_freq['1_1_2_1|1_3_1_1|1_3_4_1'] == 0)

    def test_build_singleton_1(self):
        '''
        Test ID.
        '''
        method = sPENminer(None, window_size=3, max_size=2, view='id')
        u1 = ('1', '1', '2', '1', '1', '2', '1')
        u2 = ('1', '3', '1', '1', '3', '1', '1')
        u3 = ('1', '3', '4', '1', '3', '4', '1')
        updates = [u1, u2, u3]

        for t, update in enumerate(updates):
            t += 1
            method.time += 1
            if t == 1:
                assert(method.extractor.build_singleton(update, t) == (t, update, {'1', '2'}, None, '1_1_2_1'))
            if t == 2:
                assert(method.extractor.build_singleton(update, t) == (t, update, {'3', '1'}, None, '1_3_1_1'))
            if t == 3:
                assert(method.extractor.build_singleton(update, t) == (t, update, {'3', '4'}, None, '1_3_4_1'))

    def test_build_singleton_2(self):
        '''
        Test order.
        '''
        method = sPENminer(None, window_size=3, max_size=2, view='order')
        u1 = ('1', '1', '2', '1', '1', '2', '1')
        u2 = ('1', '3', '1', '1', '3', '1', '1')
        u3 = ('1', '3', '4', '1', '3', '4', '1')
        updates = [u1, u2, u3]

        for t, update in enumerate(updates):
            t += 1
            method.time += 1
            if t == 1:
                assert(method.extractor.build_singleton(update, t) == (t, update, {'1', '2'}, {'1': 0, '2': 1}, '1_0_1_1'))
            if t == 2:
                assert(method.extractor.build_singleton(update, t) == (t, update, {'3', '1'}, {'3': 0, '1': 1}, '1_0_1_1'))
            if t == 3:
                assert(method.extractor.build_singleton(update, t) == (t, update, {'3', '4'}, {'3': 0, '4': 1}, '1_0_1_1'))

    def test_build_snippet_from_1(self):
        '''
        Test order.
        '''
        view = 'order'
        method = sPENminer(None, window_size=3, max_size=3, view=view)
        u1 = ('1', '1', '2', '1', '1', '2', '1', 1)
        u2 = ('1', '3', '1', '1', '3', '1', '1', 2)
        u3 = ('1', '3', '4', '1', '3', '4', '1', 3)
        updates = [u1, u2, u3]

        for t, update in enumerate(updates):
            t += 1
            method.time = update[-1]
            method.process_update(update)

        assert(method.num_occs['1_0_1_1|1_2_0_1|1_2_3_1'] == 1)

    def test_build_snippet_from_2(self):
        '''
        Test ID.
        '''
        view = 'id'
        method = sPENminer(None, window_size=3, max_size=3, view=view)
        u1 = ('1', '1', '3', '1', '1', '3', '1', 1)
        u2 = ('1', '4', '2', '1', '4', '2', '1', 2)
        u3 = ('1', '2', '1', '1', '2', '1', '1', 3)
        updates = [u1, u2, u3]

        for t, update in enumerate(updates):
            t += 1
            method.time = update[-1]
            method.process_update(update)

        assert(method.num_occs['1_1_3_1|1_4_2_1'] == 0)
        assert(method.num_occs['1_1_3_1|1_4_2_1|1_2_1_1'] == 1)

    def test_build_snippet_from_3(self):
        '''
        Test order.
        '''
        view = 'order'
        method = sPENminer(None, window_size=3, max_size=3, view=view)
        u1 = ('1', '1', '3', '1', '1', '3', '1', 1)
        u2 = ('1', '4', '2', '1', '4', '2', '1', 2)
        u3 = ('1', '2', '1', '1', '2', '1', '1', 3)
        updates = [u1, u2, u3]

        for t, update in enumerate(updates):
            t += 1
            method.time = update[-1]
            method.process_update(update)

        assert(method.num_occs['1_0_1_1|1_2_3_1'] == 0)
        assert(method.num_occs['1_0_1_1|1_2_0_1'] == 1)
        assert(method.num_occs['1_0_1_1|1_2_3_1|1_3_0_1'] == 1)


if __name__ == "__main__":
    unittest.main()