from collections import deque


class UniqueDeque(object):
    """ Deque extension with unique elements only, backed by a set. 
    
    Not all data model attributes (dunders) have not been properly implemented since I don't really need them. """

    def __init__(self):
        self.deque_ = deque()
        self.set_ = set()

    def append(self, item):
        if item not in self.set_:
            return self.deque_.append(item)

    def appendleft(self, item):
        if item not in self.set_:
            return self.deque_.appendleft(item)

    def clear(self):
        self.set_.clear()
        return self.deque_.clear()

    def pop(self):
        item = self.deque_.pop()
        self.set_.remove(item)
        return item

    def popleft(self):
        item = self.deque_.popleft()
        self.set_.remove(item)
        return item

    def remove(self, item):
        self.set_.remove(item)
        return self.deque_.remove(item)

    def __len__(self):
        return len(self.deque_)
