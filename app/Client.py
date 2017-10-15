import sys
import traceback

from StatsCollector import StatsCollector
from Parser import Parser
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy, RetryPolicy
from transactions.DummyTransaction import DummyTransaction
from transactions.NewOrderTransaction import NewOrderTransaction
from transactions.PaymentTransaction import PaymentTransaction
from transactions.DeliveryTransaction import DeliveryTransaction
from transactions.OrderStatusTransaction import OrderStatusTransaction
from transactions.PopularItemTransaction import PopularItemTransaction
from transactions.StockLevelTransaction import StockLevelTransaction
from transactions.TopBalanceTransaction import TopBalanceTransaction

# Consistency level: ONE = 1, QUORUM = 4
DEFAULT_CONSISTENCY_LEVEL = int(sys.argv[1]) if len(sys.argv) > 1 else 1

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
            transaction = NewOrderTransaction(session)

        elif transaction_type == Parser.PAYMENT:
            transaction = PaymentTransaction(session)

        elif transaction_type == Parser.DELIVERY:
            transaction = DeliveryTransaction(session)

        elif transaction_type == Parser.ORDER_STATUS:
            transaction = OrderStatusTransaction(session)

        elif transaction_type == Parser.STOCK_LEVEL:
            transaction = StockLevelTransaction(session)

        elif transaction_type == Parser.POPULAR_ITEM:
            transaction = PopularItemTransaction(session)

        elif transaction_type == Parser.TOP_BALANCE:
            transaction = TopBalanceTransaction(session)

        elif transaction_type == Parser.ORDER_LINE:
            pass

        try:
            transaction.execute(transaction_params)
        except Exception:
            print "Transaction could not be executed"#, traceback.format_exc()


    """ Initalize necessary objects, read and execute transaction.
    """
    def execute(self):
        # Connect to cassandra server
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
            default_retry_policy=RetryPolicy())
        session = cluster.connect('cs4224')
        session.default_consistency_level = DEFAULT_CONSISTENCY_LEVEL
        session.execute.im_self.default_timeout = 1000000000000000

        # Reading transactions line by line, parsing and execute
        while True:
            # Break if interrupted by user
            try:
                line = sys.stdin.readline().strip()
            except KeyboardInterrupt:
                break

            # Break on empty line
            if not line:
                break

            # Count transaction
            self.stats_collector.transactions.count()

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
        sys.stderr.write("Number of transactions executed: %s\n" % transaction_count)
        sys.stderr.write("Total execution time: %s\n" % transaction_time)
        sys.stderr.write("Execution throughput: %s (xact/s)\n" % (transaction_count * 1.0 / transaction_time))

if __name__ == "__main__":
    print "Executing client"
    client = Client()
    client.execute()
