import sys

from cassandra.cluster import Cluster
from transactions.DatabaseStateTransaction import DatabaseStateTransaction

# Consistency level: ONE = 1, QUORUM = 4
DEFAULT_CONSISTENCY_LEVEL = int(sys.argv[1]) if len(sys.argv) > 1 else 1

class FinalOutputer:

    """ Print out final db states
    """
    def output(self):
        # Connect to cassandra server
        cluster = Cluster()
        session = cluster.connect('cs4224')
        session.default_consistency_level = DEFAULT_CONSISTENCY_LEVEL

        transaction = DatabaseStateTransaction(session)
        transaction.execute({})

if __name__ == "__main__":
    print "Printing final db states"
    outputer = FinalOutputer()
    outputer.output()
