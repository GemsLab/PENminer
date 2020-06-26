from itertools import combinations

class Extractor:
    '''
    A class that extracts sequences from a window and does book keeping on them.

    In general, it works like this:
    1. Maintain a list of singleton sequences of updates [oldest, ..., newest].
    2. When a new singleton arrives, append it to the end.
    3. Check the front of the array to see how many old ones are stale (i.e., have left the window) and chop them off the list: [#stale:].
    4. Maintain a compatability graph among singletons based off whether they share nodes and can form larger snippets.
    5. Compute all connected subgraphs of this compatability graph (i.e., all valid snippets) and book-keep on all those that contain the new update.
    '''
    def __init__(self, method):
        '''
        :book_keeping: a function the performs the logic of book keeping
        '''
        self.book_keeping = method.book_keeping
        self.singletons = list()
        self.compatability_links = list()
        self.window_size = method.window_size
        self.max_size = method.max_size
        self.view = method.view
        # for compatability graph
        self.method = method

        self.size_2_subsets_map = dict()

    def build_singleton(self, update, age):
        '''
        Constructs a tuple representation of a singleton activity snippet (i.e., containing just one update)

        :update: the update of the snippet
        :age: the snippet's age

        :return: a tuple representation of a singleton activity snippet containing the update
        '''
        nodes = {update[1], update[2]}
        if self.view == 'id':
            snippet = f'{update[0]}_{update[1]}_{update[2]}_{update[6]}'
            names = None
        elif self.view == 'label':
            snippet = f'{update[0]}_{update[4]}_{update[5]}_{update[6]}'
            names = None
        elif self.view == 'order':
            if update[4] == update[5]:
                snippet = f'{update[0]}_{0}_{0}_{update[6]}'
                names = {update[4]: 0}
            else:
                snippet = f'{update[0]}_{0}_{1}_{update[6]}'
                names = {update[4]: 0, update[5]: 1}
        return (age, update, nodes, names, snippet)

    def build_snippet_from(self, singleton, new_update, view, snippet=None, names=None):
        new_snippet = singleton[-1] if not snippet else snippet
        if view == 'id':
            new_snippet += f'|{new_update[0]}_{new_update[1]}_{new_update[2]}_{new_update[6]}'
        elif view == 'label':
            new_snippet += f'|{new_update[0]}_{new_update[4]}_{new_update[5]}_{new_update[6]}'
        elif view == 'order':
            if not names:
                names = dict(singleton[-2])
            if new_update[4] not in names and new_update[5] not in names:
                if new_update[4] == new_update[5]:
                    names[new_update[4]] = len(names)
                else:
                    names[new_update[4]] = len(names)
                    names[new_update[5]] = len(names)
            elif new_update[4] not in names:
                names[new_update[4]] = len(names)
            elif new_update[5] not in names:
                names[new_update[5]] = len(names.values())
            new_snippet += f'|{new_update[0]}_{names[new_update[4]]}_{names[new_update[5]]}_{new_update[6]}'
        return new_snippet

    def extract_sequences(self, update):
        # add a sequence with only this update
        self.create_singleton(update)
        if self.max_size > 1:
            if self.max_size == 2:
                num_stale = 0
                for sequence in self.singletons[:-1]: # check all singletons except the new one
                    # check if sequence is stale
                    if self.method.time - sequence[0] > self.window_size:
                        # sequence is stale
                        num_stale += 1
                    else: # sequence is valid
                        # if the new update is connected to this sequence, then we'll need to create a new sequence with this update
                        # condition 1: the sequence is a singleton (we're only iterating over them) and condition 2: the two singletons are compatable
                        if update[1] in sequence[2] or update[2] in sequence[2]:
                            # build a snippet for the new sequence of updates
                            snippet = self.build_snippet_from(sequence, update, self.view)
                            # update the sequence's snippet frequencies
                            self.book_keeping(snippet)
                self.singletons = self.singletons[num_stale:]
            else:
                # add pairs
                self.create_size_2_snippets(update)
                # add bigger than pairs
                self.create_size_3_snippets()

    def create_singleton(self, update):
        '''
        Creates a size-1 activity snippet (i.e., containing just one update), and does book keeping on it.

        :update: the update of the snippet
        '''
        # create a new sequence that contains only this update
        new_singleton = self.build_singleton(update, age=self.method.time)
        if self.max_size > 1:
            self.singletons.append(new_singleton)
            if self.max_size > 2:
                self.compatability_links.append(list())
        # update the sequence's snippet frequencies
        self.book_keeping(new_singleton[-1])

    def create_size_2_snippets(self, update):
        '''
        Creates all new size-2 activity snippets, and does book keeping on them.

        :update: the update of the snippet
        '''
        num_stale = 0
        idx_of_new = len(self.singletons) - 1
        for i, sequence in enumerate(self.singletons[:-1]): # check all singletons except the new one
            # check if sequence is stale
            if self.method.time - sequence[0] > self.window_size:
                # sequence is stale
                num_stale += 1
            else: # sequence is valid
                # if the new update is connected to this sequence, then we'll need to create a new sequence with this update
                # condition 1: the sequence is a singleton (we're only iterating over them) and condition 2: the two singletons are compatable
                if update[1] in sequence[2] or update[2] in sequence[2]:
                    self.compatability_links[-1].append(idx_of_new - i)
                    # build a snippet for the new sequence of updates
                    snippet = self.build_snippet_from(sequence, update, self.view)
                    # update the sequence's snippet frequencies
                    self.book_keeping(snippet)
        self.singletons = self.singletons[num_stale:]
        self.compatability_links = self.compatability_links[num_stale:]

    def all_size_2_subsets(self, s):
        s = tuple(s)
        if s in self.size_2_subsets_map:
            return self.size_2_subsets_map[s]
        res = tuple(combinations(s, 2))
        self.size_2_subsets_map[s] = res
        return res

    def create_size_3_snippets(self):
        '''
        Creates all new size-3 activity snippets, and does book keeping on them.
        '''
        def two_one_hop(seen):
            for other_two in self.all_size_2_subsets(self.compatability_links[-1]):
                first_offset = -(other_two[0] + 1)
                second_offset = -(other_two[1] + 1)
                if first_offset <= second_offset:
                    idxs = (first_offset, second_offset, -1)
                else:
                    idxs = (second_offset, first_offset, -1)
                seen.add(idxs)
                # sort first by age, then by snippet if tied
                singleton_1 = self.singletons[-1]
                singleton_2 = self.singletons[first_offset]
                singleton_3 = self.singletons[second_offset]
                age_1 = singleton_1[0]
                age_2 = singleton_2[0]
                age_3 = singleton_3[0]

                one_before_two = age_1 < age_2 or (age_1 == age_2 and singleton_1[-1] < singleton_2[-1])
                one_before_three = age_1 < age_3 or (age_1 == age_3 and singleton_1[-1] < singleton_3[-1])
                two_before_three = age_2 < age_3 or (age_2 == age_3 and singleton_2[-1] < singleton_3[-1])

                if one_before_two and one_before_three:
                    if two_before_three:
                        triple = (singleton_1, singleton_2, singleton_3)
                    else:
                        triple = (singleton_1, singleton_3, singleton_2)
                elif not one_before_two and two_before_three:
                    if one_before_three:
                        triple = (singleton_2, singleton_1, singleton_3)
                    else:
                        triple = (singleton_2, singleton_3, singleton_1)
                else:
                    if one_before_two:
                        triple = (singleton_3, singleton_1, singleton_2)
                    else:
                        triple = (singleton_3, singleton_2, singleton_1)
                # triple = tuple(sorted([self.singletons[-1], self.singletons[first_offset], self.singletons[second_offset]], key=lambda it: (it[0], it[-1])))
                if self.view == 'id':
                    new_snippet = f'{triple[0][-1]}|{triple[1][-1]}|{triple[2][-1]}'
                else:
                    new_singleton = triple[0]
                    if self.view == 'order':
                        names = dict(new_singleton[-2])
                    else:
                        names = None
                    new_snippet = new_singleton[-1]
                    for sequence in triple[1:]:
                        new_snippet = self.build_snippet_from(None, sequence[1], self.view, snippet=new_snippet, names=names)
                # update the sequence's snippet frequencies
                self.book_keeping(new_snippet)

        def two_hop(seen):
            # for each singleton that the current is compatable with
            for link in self.compatability_links[-1]:
                # negative index to singleton one hop away
                one_hop_link = -(link + 1)
                # for reachable singleton in two hops
                for two_hop in self.compatability_links[one_hop_link]:
                    # negative index to singleton two hops away
                    two_hop_link = -(link + 1 + two_hop)
                    if -two_hop_link <= len(self.singletons):
                        if one_hop_link <= two_hop_link:
                            idxs = (one_hop_link, two_hop_link, -1)
                        else:
                            idxs = (two_hop_link, one_hop_link, -1)
                        if idxs in seen:
                            continue
                        seen.add(idxs)
                        # sort first by age, then by snippet if tied
                        singleton_1 = self.singletons[-1]
                        singleton_2 = self.singletons[one_hop_link]
                        singleton_3 = self.singletons[two_hop_link]
                        age_1 = singleton_1[0]
                        age_2 = singleton_2[0]
                        age_3 = singleton_3[0]

                        one_before_two = age_1 < age_2 or (age_1 == age_2 and singleton_1[-1] < singleton_2[-1])
                        one_before_three = age_1 < age_3 or (age_1 == age_3 and singleton_1[-1] < singleton_3[-1])
                        two_before_three = age_2 < age_3 or (age_2 == age_3 and singleton_2[-1] < singleton_3[-1])

                        if one_before_two and one_before_three:
                            if two_before_three:
                                triple = (singleton_1, singleton_2, singleton_3)
                            else:
                                triple = (singleton_1, singleton_3, singleton_2)
                        elif not one_before_two and two_before_three:
                            if one_before_three:
                                triple = (singleton_2, singleton_1, singleton_3)
                            else:
                                triple = (singleton_2, singleton_3, singleton_1)
                        else:
                            if one_before_two:
                                triple = (singleton_3, singleton_1, singleton_2)
                            else:
                                triple = (singleton_3, singleton_2, singleton_1)
                        if self.view == 'id':
                            new_snippet = f'{triple[0][-1]}|{triple[1][-1]}|{triple[2][-1]}'
                        else:
                            new_singleton = triple[0]
                            if self.view == 'order':
                                names = dict(new_singleton[-2])
                            else:
                                names = None
                            new_snippet = new_singleton[-1]
                            for sequence in triple[1:]:
                                new_snippet = self.build_snippet_from(None, sequence[1], self.view, snippet=new_snippet, names=names)
                        self.book_keeping(new_snippet)

        seen = set()
        two_one_hop(seen)
        two_hop(seen)
