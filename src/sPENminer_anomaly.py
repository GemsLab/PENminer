from collections import defaultdict
from stream import Stream
from sPENminer import sPENminer
from extractor import Extractor
from math import log2 as log
from math import log10
from time import time
import numpy as np
import sys
import random
import rrcf

class sPENminerAnomaly(sPENminer):
    '''
    A class to perform the method.
    '''
    def __init__(self,
                 stream,
                 window_size,
                 max_size,
                 view,
                 alpha=1,
                 beta=1,
                 gamma=1,
                 data_stream=False,
                 freq=False,
                 num_trees=50,
                 max_depth=256,
                 seed=None):
        '''
        :stream: a Stream object to mine
        :window_size: the size of a window to use—determines the max size of snippets
        '''
        print('Running anomaly version.')
        super().__init__(stream,
                         window_size,
                         max_size,
                         view=view,
                         alpha=alpha,
                         beta=beta,
                         gamma=gamma,
                         save_output=False)

        # tree parameters for rrcf
        print('Random seed {}.'.format(seed))
        self.seed = seed
        # tree parameters for rrcf
        np.random.seed(seed)
        self.index = 0
        self.num_trees = num_trees
        self.tree_size = max_depth
        self.data_stream = data_stream
        self.freq = freq
        # create a forest of empty trees for rrcf
        self.forest = []
        for _ in range(self.num_trees):
            tree = rrcf.RCTree()
            self.forest.append(tree)

        self.anomaly_scores = list()
        self.score_times = list()
        self.score_snippets = list()

        if self.data_stream: # NOTE: hardcoded for DARPA IP and Chicago (the two datasets used in the paper).
            print('Running data stream version of P')
            self.occ_intervals = defaultdict(set)
            # paper uses 60 measurement periods
            if stream.name.startswith('darpa'):
                bin_width = 87725 / 60
            elif stream.name.startswith('chicago'):
                gt_name = '../data/{}.txt'.format(self.stream.name)
                lines = open(gt_name, 'r').readlines()
                ts = int(lines[0].strip().split(',')[-1])
                te = int(lines[-1].strip().split(',')[-1])
                width = te - ts
                print('Width = te - ts = {} - {} = {}'.format(te, ts, width))
                bin_width = 7773420 / 60
            else:
                print('Baseline not implemented for datasets other than DARPA IP and Chicago Bike because the baseline falsely assumes that the stream length is known a priori.')
                sys.exit(1)
            self.bins = list()
            for i in range(1, 60):
                self.bins.append(0 + (bin_width * i))

    def score_snippet(self, point, snippet):
        avg_codisp = 0
        for tree in self.forest:
            if snippet in tree.leaves:
                try:
                    tree.forget_point(snippet)
                except RecursionError as e:
                    print(snippet in tree.leaves)
            if len(tree.leaves) > self.tree_size:
                to_delete = np.random.choice(list(tree.leaves.keys()))
                try:
                    tree.forget_point(to_delete)
                except RecursionError as e:
                    print(snippet in tree.leaves)
            # Insert the new point into the tree
            tree.insert_point(point, index=snippet)#self.index)
            # Compute codisp on the new point and take the average among all trees
            avg_codisp += tree.codisp(snippet) / self.num_trees
        self.index += 1
        return avg_codisp


    def process_update(self, update):
        '''
        Process an update from the stream.

        update_type, u, v, w, l_u, l_v, r = update
        '''
        self.new = set()
        # update the current time to what the update is
        self.time = int(update[-1])
        if self.ts == 0:
            self.ts = self.time
        self.old_freq_of_current = dict()
        self.freq_of_current = dict()
        self.extractor.extract_sequences(update)

        # update persistence scores
        seen = set()
        for snippet in self.new:
            if snippet in seen:
                continue
            seen.add(snippet)
            if not self.freq:
                if not self.data_stream:
                    P = self.P(snippet)
                else:
                    self.num_occs[snippet] += 1
                    self.last_occs[snippet] = self.time
                    # compute the number of distinct intervals where the snippet has occurred
                    self.occ_intervals[snippet].add(int(np.digitize(self.last_occs[snippet], self.bins)))
                    P = len(self.occ_intervals[snippet])
                F = log10(self.num_occs[snippet] + 1)
                point = [P, F]
                score = self.score_snippet(point, snippet)
            else:
                self.num_occs[snippet] += 1
                score = self.num_occs[snippet] / (len(self.anomaly_scores) + 1)
            self.anomaly_scores.append(score)
            self.score_times.append(self.time)
            self.score_snippets.append(snippet)

    def mine(self, verbose=True):
        '''
        Mine the stream.
        '''
        t0 = time()
        N = 0
        for update in self.stream.flow():
            self.process_update(update)
            N += 1
            if N > 0 and N % 1000 == 0 and verbose:
                print(N, np.max(self.anomaly_scores), len(self.forest[0].leaves))
        t1 = time()
        if verbose:
            print('{} edges / sec'.format(N / (t1 - t0)))

        file_prefix = '{}_window_size_{}_max_size_{}_exps_{}_{}_{}_num_trees_{}_max_depth_{}_seed_{}'.format(self.stream.name, self.window_size, self.max_size, self.alpha, self.beta, self.gamma, self.num_trees, self.tree_size, self.seed)
        if self.data_stream:
            file_prefix += '_data_stream'
        if self.freq:
            file_prefix += '_freq'
        base_path = '../output/{}/{}/{}'.format('online', self.view, file_prefix)
        np.save('{}_anomaly_scores'.format(base_path), np.asarray(self.anomaly_scores))
        np.save('{}_score_times'.format(base_path), np.asarray(self.score_times))
        np.save('{}_score_snippets'.format(base_path), np.asarray(self.score_snippets))
