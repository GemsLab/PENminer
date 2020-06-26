from collections import defaultdict
from stream import Stream
from extractor import Extractor
from math import log2 as log
from math import log10
from time import time
import numpy as np
from scipy.stats import entropy as scipy_entropy

class oPENminer:
    '''
    A class to perform the method in an offline (non-streaming) fashion.
    '''
    def __init__(self,
                 stream,
                 window_size,
                 max_size,
                 view='id',
                 alpha=1,
                 beta=1,
                 gamma=1,
                 save_output=True,
                 save_occs=False):
        '''
        :stream: a Stream object to mine
        :window_size: the size of a window to use—determines the max size of snippets
        '''
        self.stream = stream
        self.window_size = window_size
        self.max_size = max_size
        self.view = view
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.snippet_to_occs = defaultdict(list)
        self.snippet_to_freq = defaultdict(int)

        self.time = 0 # time starts at 1
        self.ts = -1

        self.Ps = dict()

        self.extractor = Extractor(self)
        self.save_output = save_output
        self.save_occs = save_occs

    def process_update(self, update):
        '''
        Process an update from the stream.

        update_type, u, v, w, l_u, l_v, r = update
        '''
        self.time = int(update[-1])
        if self.ts == -1:
            self.ts = self.time
        self.extractor.extract_sequences(update)

    def book_keeping(self, snippet):
        '''
        :snippet: a snippet to keep the books on.
        '''
        self.snippet_to_occs[snippet].append(self.time)
        if self.save_output:
            self.snippet_to_freq[snippet] += 1

    def W(self, occs, interval_width):
        '''
        Compute the coverage of a snippet.

        :occs: a list of times where the snippet occurred (duplicates if more than once).
        :interval_width: the width of the interval to compute coverage over.
        '''
        tf = occs[0]
        tl = occs[-1]
        return (tl - tf + 1) / (interval_width + 1)

    def F(self, occs):
        return log10(len(occs) + 1)

    def S(self, occs):
        # discard duplicates
        occs = sorted(set(occs))
        if len(occs) <= 2:
            return 1
        return self.H(occs) / log(len(occs) - 1) + 1

    def H(self, occs):
        '''
        Compute the "entropy" of a snippet, assigning weights as the width of gaps.

        :occs: a list of times where the snippet occurred (duplicates if more than once).
        '''
        p = np.diff(occs)
        p = p / sum(p)
        return scipy_entropy(p, base=2)

    def P(self, snippet, occs, interval_width):
        '''
        Compute the persistence of a snippet.

        :snippet: the snippet.
        :occs: a list of times where the snippet occurred (duplicates if more than once).
        :interval_width: the width of the interval to compute persistence over.
        '''
        W = self.W(occs, interval_width)
        F = self.F(occs)
        S = self.S(occs)

        return (W ** self.alpha) * (F ** self.beta) * (S ** self.gamma)

    def compute_persistence(self, occs_path=None):
        '''
        Computes persistence after parsing the stream.
        '''
        ts = self.ts
        te = self.time
        # the width of the interval to measure persistence over
        interval_width = te - ts

        if self.save_occs:
            f = open(occs_path, 'w')

        for snippet, occs in self.snippet_to_occs.items():
            # the snippet's occurrences
            occs = sorted(occs)
            if self.save_occs:
                f.write('{},{}\n'.format(snippet, ','.join(list(str(it) for it in occs))))
            ''' compute P value '''
            P = self.P(snippet, occs, interval_width)
            self.Ps[snippet] = P

        if self.save_occs:
            f.close()

    def mine(self, verbose=True):
        '''
        Mine the stream.
        '''
        t0 = time()
        N = 0
        for update in self.stream.flow():
            self.process_update(update)
            N += 1
            if N > 0 and N % 10000 == 0 and verbose:
                print('{} edge updates processed.'.format(N))
        t1 = time()

        if verbose:
            print('{} edges / sec'.format(N / (t1 - t0)))
            print('Computing persistence.')
        file_prefix = '{}_window_size_{}_max_size_{}_exps_{}_{}_{}'.format(self.stream.name, self.window_size, self.max_size, self.alpha, self.beta, self.gamma)
        base_path = '../output/{}/{}/{}'.format('offline', self.view, file_prefix)
        if self.save_occs:
            occs_path = '{}_occs.txt'.format(base_path)
        else:
            occs_path = None
        self.compute_persistence(occs_path=occs_path)

        if self.save_output:
            if verbose:
                for P, item in sorted(self.Ps.items(), reverse=True, key=lambda it: it[1])[:10]:
                    print(P, item)
                print('Saving output to {}'.format(base_path))
            with open('{}_out.txt'.format(base_path), 'w') as f:
                for snippet, persistence in sorted(self.Ps.items(), reverse=True, key=lambda it: it[1]):
                    f.write('{},{}\n'.format(snippet, persistence))
            with open('{}_freq_out.txt'.format(base_path), 'w') as f:
                for snippet, persistence in sorted(self.snippet_to_freq.items(), reverse=True, key=lambda it: it[1]):
                    f.write('{},{}\n'.format(snippet, persistence))
