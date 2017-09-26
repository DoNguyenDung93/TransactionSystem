from cassandra.cluster import Cluster
from transactions.DummyTransaction import DummyTransaction
from transactions.StockLevelTransaction import StockLevelTransaction

def run_transactions():
    cluster = Cluster()
    session = cluster.connect('cs4224')
    dummy_transaction = StockLevelTransaction(session)
    dummy_transaction.execute({
        'w_id': 1,
        'd_id': 2,
        't': 400,
        'l': 3
    })

run_transactions()