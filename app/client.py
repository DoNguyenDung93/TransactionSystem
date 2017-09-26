from cassandra.cluster import Cluster
from transactions.DummyTransaction import DummyTransaction

def run_transactions():
    cluster = Cluster()
    session = cluster.connect('cs4224')
    dummy_transaction = DummyTransaction(session)
    dummy_transaction.execute({ 'w_id': 2 })

run_transactions()