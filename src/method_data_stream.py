from collections import defaultdict
from stream import Stream
from extractor import Extractor
from time import time
import numpy as np

class MethodDataStream:
    '''
    A class to perform the method from "Finding Persistent Items in Data Streams"
    '''
    def __init__(self,
                 stream,
                 window_size,
                 max_size,
                 view='id',
                 save_output=True):
        '''
        :stream: a Stream object to mine
        :window_size: the size of a window to use—determines the max size of snippets
        '''
        self.stream = stream
        self.window_size = window_size
        self.max_size = max_size
        self.view = view
        self.snippet_to_occs = defaultdict(list)

        self.time = 0 # time starts at 1
        self.ts = 0

        self.Ps = dict()

        self.extractor = Extractor(self)
        self.save_output = save_output

    def process_update(self, update):
        '''
        Process an update from the stream.

        update_type, u, v, w, l_u, l_v, r = update
        '''
        self.time = int(update[-1])
        if self.ts == 0:
            self.ts = self.time
        self.extractor.extract_sequences(update)

    def book_keeping(self, snippet):
        '''
        :snippet: a snippet to keep the books on.
        '''
        self.snippet_to_occs[snippet].append(self.time)

    def compute_persistence(self):
        '''
        Computes persistence after parsing the stream.
        '''
        ts = self.ts
        te = self.time
        # the width of the interval to measure persistence over
        interval_width = te - ts

        # paper uses 60 measurement periods
        bin_width = interval_width / 60
        bins = list()
        for i in range(60):
            bins.append(ts + (bin_width * i))

        for snippet, occs in self.snippet_to_occs.items():
            # the snippet's occurrences
            occs = sorted(occs)
            ''' compute P value '''
            P = len(set(np.digitize(occs, bins)))
            self.Ps[snippet] = P

    def mine(self, verbose=True):
        '''
        Mine the stream.
        '''
        print("Running method from \"Finding Persistent Items in Data Streams.\"")
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
        file_prefix = '{}_window_size_{}_max_size_{}'.format(self.stream.name, self.window_size, self.max_size)
        base_path = '../output/{}/{}/{}'.format('offline', self.view, file_prefix)
        self.compute_persistence()

        if self.save_output:
            if verbose:
                for P, item in sorted(self.Ps.items(), reverse=True, key=lambda it: it[1])[:10]:
                    print(P, item)
                print('Saving output to {}'.format(base_path))
            with open('{}_data_stream_out.txt'.format(base_path), 'w') as f:
                for snippet, persistence in sorted(self.Ps.items(), reverse=True, key=lambda it: it[1]):
                    f.write('{},{}\n'.format(snippet, persistence))
