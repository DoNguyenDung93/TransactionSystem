from cassandra.cluster import Cluster
from transactions.DatabaseStateTransaction import DatabaseStateTransaction

class FinalOutputer:

    """ Print out final db states
    """
    def output(self):
        # Connect to cassandra server
        cluster = Cluster()
        session = cluster.connect('cs4224')
        session.execute.im_self.default_timeout = 1000000000000000

        transaction = DatabaseStateTransaction(session)
        transaction.execute({})

if __name__ == "__main__":
    print "Printing final db states"
    outputer = FinalOutputer()
    outputer.output()
