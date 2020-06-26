from collections import defaultdict
from stream import Stream
from extractor import Extractor
from math import log2 as log
from math import log10
from time import time

class sPENminer:
    '''
    A class to perform the method.
    '''
    def __init__(self,
                 stream,
                 window_size,
                 max_size,
                 view='id',
                 alpha=1,
                 beta=1,
                 gamma=1,
                 save_output=True):
        '''
        :stream: a Stream object to mine
        :window_size: how much time to allow between the first and last occurrence of snippets
        :max_size: the maximum number of edges to allow in a snippet
        '''
        self.stream = stream
        self.window_size = window_size
        self.max_size = max_size
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma

        self.time = 0 # time starts at 0, but will be updated _upon_ the arrival of the first update
        self.ts = -1
        self.new = set()

        # maintain stats for snippets
        self.first_occs = dict()
        self.last_occs = dict()
        self.num_occs = defaultdict(int)
        self.num_gaps = defaultdict(int)
        self.old_Ps = dict()

        self.view = view
        self.extractor = Extractor(self)
        self.save_output = save_output

    def query(self, x):
        '''
        Query the current persistence score of snippet x.

        :x: the snippet whose persistence to return.
        :return: the persistence of x, after recomputing it.
        '''
        return self.P_no_new_occ(x)

    def P_no_new_occ(self, x, te=None):
        if te == None:
            ts = self.ts
            te = self.time
        # first occurrence
        tf = self.first_occs[x]
        # when the last occurrence was
        tl = self.last_occs[x]
        # the width of the interval to measure persistence over
        interval_width = te - ts
        # the snippet's width
        W = self.W(tl, tf, interval_width)
        # get the old persistence score
        old_P = self.old_Ps[x]
        # old interval width (assuming it was computed when it occurred)
        old_interval_width = tl - ts
        # old width
        old_W = self.W(tl, tf, old_interval_width)
        # reverse engineer the persistence
        remove_W = old_W ** self.alpha
        new_W = W ** self.alpha
        return old_P / remove_W * new_W

    def H(self, x, ts, te, tf, tl, old_tl, old_freq_of_current):
        ''' compute from old P value '''
        old_P = self.old_Ps[x]
        old_interval_width = old_tl - ts
        # what the old coverage was
        old_W = self.W(old_tl, tf, old_interval_width)
        # what the old frequency was
        old_F = old_freq_of_current#self.old_freq_of_current[x]
        # what the old H was
        old_num_gaps = self.num_gaps[x]
        remove_W = old_W ** self.alpha
        remove_F = log10(old_F + 1) ** self.beta
        old_H = old_P / (remove_W * remove_F) # divide off old W and F
        old_H = old_H ** (1 / self.gamma) # remove exponent
        old_H = (old_H - 1) * log(old_num_gaps) # remove normalizer and +1
        # what the old normalizing Z _was_
        old_Z = old_tl - tf
        gap = tl - old_tl
        new_Z = old_Z + gap

        num_gaps = old_num_gaps + 1

        try:
            entropy = old_H + (old_Z / new_Z) * log(new_Z) - log(old_Z) + ((1 / old_Z) - (1 / new_Z)) * (log(old_Z) - old_H) * old_Z - (gap / new_Z) * log(gap / new_Z)
            entropy /= log(num_gaps)
        except ValueError as e:
            print('time: {} x: {}, old_H = {}, old_Z = {}, new_Z = {}, gap = {}, gap/new_Z = {}'.format(self.time, x, old_H, old_Z, new_Z, gap, gap / new_Z))
            print(e)
            entropy = 0

        self.num_gaps[x] += 1
        return entropy

    def W(self, tl, tf, interval_width):
        return (tl - tf + 1) / (interval_width + 1)

    def S(self, x, ts, te, tf, tl, old_tl, old_freq_of_current):
        if tf == tl or tf == old_tl: # first or second occ
            if tf == old_tl:
                self.num_gaps[x] = 1
            return 1
        return self.H(x, ts, te, tf, tl, old_tl, old_freq_of_current) + 1

    def F(self, last_occs, old_freq_of_current):
        freq = old_freq_of_current + last_occs
        return log10(freq + 1)

    def P(self, x, ts=None, te=None):
        '''
        Return the persistence score for a snippet.

        :x: the snippet to compute the persistence score for
        :ts: the starting point of the interval to measure persistence in
        :te: the ending point of the interval to measure persistence in
        '''
        if te == None:
            ts = self.ts
            te = self.time
        # what the last occurrence _was_
        old_tl = self.last_occs[x]
        # what the last occurrence _now_ is
        tl = self.time
        # number of occurrences right now
        last_occs = self.freq_of_current[x]
        # if the old last and new old are the same, then just update freq
        old_freq_of_current = self.old_freq_of_current[x]
        if tl == old_tl and old_freq_of_current > 0:
            P = (self.old_Ps[x] / log10(old_freq_of_current + 1) ** self.beta) * (log10(old_freq_of_current + last_occs + 1) ** self.beta)
        else:
            # first occurrence
            tf = self.first_occs[x]
            # the width of the interval to measure persistence over
            interval_width = te - ts
            W = (tl - tf + 1) / (interval_width + 1)#self.W(tl, tf, interval_width)
            F = log10(old_freq_of_current + last_occs + 1)#self.F(old_freq_of_current, last_occs)
            S = self.S(x, ts, te, tf, tl, old_tl, old_freq_of_current)
            P = (W ** self.alpha) * (F ** self.beta) * (S ** self.gamma)

        self.old_Ps[x] = P
        self.last_occs[x] = tl
        self.num_occs[x] += last_occs

        return P

    def process_update(self, update):
        '''
        Process an update from the stream.

        update_type, u, v, w, l_u, l_v, r, t = update
        '''
        self.new = set()
        # update the current time to what the update is
        self.time = int(update[-1])
        if self.ts == -1:
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
            P = self.P(snippet)

    def book_keeping(self, snippet):
        '''
        :snippet: a snippet to keep the books on.
        '''
        self.new.add(snippet)
        # check if the there is any estimated first occurrence
        if snippet not in self.first_occs:
            self.first_occs[snippet] = self.time
            self.last_occs[snippet] = self.time

        # temporarilly store what the frequency was
        if snippet not in self.old_freq_of_current:
            self.old_freq_of_current[snippet] = self.num_occs[snippet]
            self.freq_of_current[snippet] = 0
        self.freq_of_current[snippet] += 1

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

        if self.save_output:
            file_prefix = '{}_window_size_{}_max_size_{}_exps_{}_{}_{}'.format(self.stream.name, self.window_size, self.max_size, self.alpha, self.beta, self.gamma)
            base_path = '../output/{}/{}/{}'.format('online', self.view, file_prefix)
            snippets = list(self.num_occs.keys())
            if verbose:
                for snippet in sorted(snippets, reverse=True, key=lambda it: self.query(it))[:10]:
                    print(snippet, self.query(snippet))
                print('Saving output to {}'.format(base_path))
            with open('{}_out.txt'.format(base_path), 'w') as f:
                for snippet in sorted(snippets, reverse=True, key=lambda it: self.query(it)):
                    f.write('{},{}\n'.format(snippet, self.query(snippet)))
            with open('{}_freq_out.txt'.format(base_path), 'w') as f:
                for snippet in sorted(snippets, reverse=True, key=lambda it: self.num_occs[it]):
                    f.write('{},{}\n'.format(snippet, self.num_occs[snippet]))
