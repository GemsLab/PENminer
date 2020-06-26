import os

class Stream:
    '''
    An edge stream representation that seeks to make the operations needed
    for this method as efficient as possible.
    '''
    def __init__(self, name, ext='txt', delimiter=','):
        '''
        :name: the name of the stream to load.
        :ext: the file extension for the graph files.
        :delimiter: the delimiter for the graph files.

        Assumes:
        1) ../data/{name}.{ext} edgelist.
        2) File format: delimiter separated items in order: (operation, u, v, w, u_label, v_label, edge_label)

        e.g., ../data/boston_bike.txt
        '''
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        edgelist = os.path.join(ROOT_DIR, '../data/{}.{}'.format(name, ext))
        self.name = name

        self.delimiter = delimiter
        self.f = open(edgelist, 'r')

    def flow(self):
        '''
        Output the stream of updates.
        '''
        for line in self.f:
            yield line.strip().split(self.delimiter)
