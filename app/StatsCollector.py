import time

class StatsCollector:

    # StatsCollector type-1: Counter
    class Counter:
        def __init__(self):
            self.counter = 0

        def count(self):
            self.counter += 1

        def get_count(self):
            return self.counter

    # StatsCollector type-2: Timer
    class Timer:
        def __init__(self):
            self.total = 0

        def start(self):
            self.start_time = time.clock()

        def finish(self):
            self.total += time.clock() - self.start_time
            self.last = time.clock() - self.start_time
 
        def get_total_time(self):
            return self.total

        def get_last(self):
            return self.last
 
    def __init__(self):
        self.transactions = StatsCollector.Counter()
        self.transaction_timer = StatsCollector.Timer()



