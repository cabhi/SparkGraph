import abc
import json


class SparkEvents(object):
    """docstring for SparkEvents"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def getStartTime(self):
        """Returns the ingredient list."""
        raise NotImplementedError

    @abc.abstractmethod
    def getEndTime(self):
        """Returns the ingredient list."""
        raise NotImplementedError

    def setRank(self,rank):
        self.rank = rank

    def getRank(self):
        return self.rank

    def __init__(self, arg):
        self.eventJson = arg
        self.id = None

    def getId(self):
        return self.id

    def __eq__(self, other):

        return type(self).__name__ == type(other).__name__ and self.id == other.id

    def parentName(self):
        allParents = []
        for base in self.__class__.__bases__ :
            allParents.append(base.__name__)
        return  allParents

    def printClassdump(self):
        print json.dumps(self.eventJson, indent=4, sort_keys=True)
