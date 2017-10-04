import sys

from StatsCollector import StatsCollector
from Parser import Parser
from cassandra.cluster import Cluster
from transactions.DummyTransaction import DummyTransaction
from transactions.NewOrderTransaction import NewOrderTransaction
from transactions.PaymentTransaction import PaymentTransaction
from transactions.DeliveryTransaction import DeliveryTransaction
from transactions.OrderStatusTransaction import OrderStatusTransaction
from transactions.PopularItemTransaction import PopularItemTransaction
from transactions.StockLevelTransaction import StockLevelTransaction
from transactions.TopBalanceTransaction import TopBalanceTransaction

class Client:

    def __init__(self):
        # Inits parser and stats_collector
        self.stats_collector = StatsCollector()
        self.parser = Parser()


    """ Executes a transaction given cassandra session, transaction type and
        transaction params.
    """
    def execute_transaction(self, session, transaction_type, transaction_params):
        transaction = DummyTransaction(session)

        if transaction_type == Parser.NEW_ORDER:
            pass
            # transaction = NewOrderTransaction(session)

        elif transaction_type == Parser.PAYMENT:
            pass
            # transaction = PaymentTransaction(session)

        elif transaction_type == Parser.DELIVERY:
            pass
            # transaction = DeliveryTransaction(session)

        elif transaction_type == Parser.ORDER_STATUS:
            # pass
            transaction = OrderStatusTransaction(session)

        elif transaction_type == Parser.STOCK_LEVEL:
            pass
            # transaction = StockLevelTransaction(session)

        elif transaction_type == Parser.POPULAR_ITEM:
            pass
            # transaction = PopularItemTransaction(session)

        elif transaction_type == Parser.TOP_BALANCE:
            pass
            # transaction = TopBalanceTransaction(session)

        elif transaction_type == Parser.ORDER_LINE:
            pass

        transaction.execute(transaction_params)


    """ Initalize necessary objects, read and execute transaction.
    """
    def execute(self):
        # Connect to cassandra server
        cluster = Cluster()
        session = cluster.connect('cs4224')

        # Count transaction
        self.stats_collector.transactions.count()

        # line = 'D,1,10'
        line = 'O,1,1,1971'

        # Parsing transaction
        transaction_type = self.parser.get_transaction_type(line)
        extra_line_cnt = self.parser.get_transaction_extra_line_count(transaction_type, line)
        extra_lines = []
        for i in range(extra_line_cnt):
            extra_line = sys.stdin.readline().strip()
            extra_lines.append(extra_line)
        transaction_params = self.parser.parse(line, transaction_type, extra_lines)

        # Execute transaction and measure time
        self.stats_collector.transaction_timer.start()
        self.execute_transaction(session, transaction_type, transaction_params)
        self.stats_collector.transaction_timer.finish()

        self.output()

    """ Print out statistics collected during execution
    """
    def output(self):
        transaction_count = self.stats_collector.transactions.get_count()
        transaction_time = self.stats_collector.transaction_timer.get_total_time()
        print "Number of transactions executed: %s" % transaction_count
        print "Total execution time: %s" % transaction_time
        print "Execution throughput: %s (xact/s)" % (transaction_count * 1.0 / transaction_time)

if __name__ == "__main__":
    print "Executing client"
    client = Client()
    client.execute()
