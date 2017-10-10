import argparse
import csv
import itertools

from Utility import Utility
from StatsCollector import StatsCollector
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent
from cassandra.query import BatchStatement

class DataLoader:

    BATCH_SIZE = 30
    DATABASE_NAME = "cs4224"
    DEFAULT_DIRECTORY = "."
    DEFAULT_ROW_READ = 1000000000
    TOKEN_SEPARATOR = ","
    JOIN_CH = "-"

    def __init__(self, dir_path = DEFAULT_DIRECTORY, row_count = DEFAULT_ROW_READ, only_additional_tables = False):
        self.only_additional_tables = only_additional_tables
        self.DIR_PATH = dir_path
        self.ROW_COUNT = row_count
        self.WAREHOUSE_FILE_PATH  = self.DIR_PATH + "/warehouse.csv"
        self.ORDER_FILE_PATH      = self.DIR_PATH + "/order.csv"
        self.DISTRICT_FILE_PATH   = self.DIR_PATH + "/district.csv"
        self.CUSTOMER_FILE_PATH   = self.DIR_PATH + "/customer.csv"
        self.ITEM_FILE_PATH       = self.DIR_PATH + "/item.csv"
        self.ORDER_LINE_FILE_PATH = self.DIR_PATH + "/order-line.csv"
        self.STOCK_FILE_PATH      = self.DIR_PATH + "/stock.csv"

    def timemeasure(original_function):
        def new_function(*args, **kwargs):
            timer = StatsCollector.Timer()
            timer.start()
            original_function(*args, **kwargs)
            timer.finish()
            print "%s finished in %s s" % (original_function.__name__, timer.get_last())
        return new_function


    """
        Executes some queries using batch statements
    """
    def execute_in_batch(self, session, reader, query, extract_data, get_params):
        batch = BatchStatement()
        query_count = 0
        for line in itertools.islice(reader, self.ROW_COUNT):
            # Store data to be used by other methods
            extract_data(self, line)
            batch.add(query, get_params(self, line))
            query_count = query_count + 1
            if query_count % self.BATCH_SIZE == 0:
                session.execute(batch)
                batch = BatchStatement()
        # Execute last batch
        session.execute(batch)


    """
        Load warehouse data
    """
    @timemeasure
    def load_warehouse_data(self, csv_file, session):
        query = session.prepare("INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state,"
                                "w_zip, w_ytd) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        query_and_params = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            params = (int(line[0]), line[1], line[2], line[3], line[4], line[5], line[6], float(line[8]))
            query_and_params.append((query, params))

            # Stores data needed for other methods
            w_id, w_name = line[0], line[1]
            self.map_w_name[w_id] = w_name

        execute_concurrent(session, query_and_params, raise_on_first_error=True)


    """
        Load district data
    """
    @timemeasure
    def load_district_data(self, csv_file, session):
        query = session.prepare("INSERT INTO district (d_w_id, d_id, d_name,"
                                "d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd)"
                                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        query_and_params = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            params = (int(line[0]), int(line[1]), line[2], line[3], line[4], line[5], line[6], line[7], float(line[9]))
            query_and_params.append((query, params))

            # Stores data needed for other methods
            d_w_id, d_id, d_name = line[0], line[1], line[2]
            self.map_d_name[self.JOIN_CH.join((d_w_id, d_id))] = d_name

        execute_concurrent(session, query_and_params, raise_on_first_error=True)


    """
        Load district_next_smallest_order_id data
    """
    @timemeasure
    def load_district_next_smallest_order_id_data(self, csv_file, session):
        query = session.prepare("INSERT INTO district_next_smallest_order_id (d_w_id, d_id, d_next_smallest_o_id) "
                                "VALUES (?, ?, ?)")
        reader = csv.reader(csv_file)
        query_and_params = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            d_w_id, d_id = line[0], line[1]
            key = self.JOIN_CH.join((d_w_id, d_id))
            d_next_smallest_o_id = (self.map_last_delivery[key] if key in self.map_last_delivery else 0) + 1

            query_and_params.append((query, (int(d_w_id), int(d_id), d_next_smallest_o_id)))

        execute_concurrent(session, query_and_params, raise_on_first_error=True)


    """
        Load customer data
    """
    @timemeasure
    def load_customer_data(self, csv_file, session):
        query = session.prepare("INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, "
                                "c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, "
                                "c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, "
                                "c_payment_cnt, c_delivery_cnt, c_data, w_name, d_name) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        def get_params_customer_data_line(self, line):
            datetime = Utility.convert_to_datetime_object(line[12])
            # Reading w_name, d_name from maps created in previous procedures
            c_w_id, c_d_id = line[0], line[1]
            c_w_name, c_d_name = self.map_w_name[c_w_id], self.map_d_name[self.JOIN_CH.join((c_w_id, c_d_id))]
            return (int(line[0]), int(line[1]), int(line[2]), line[3], line[4], line[5],
                      line[6], line[7], line[8], line[9], line[10], line[11], datetime,
                      line[13], float(line[14]), float(line[15]), float(line[16]), float(line[17]),
                      int(line[18]), int(line[19]), line[20], c_w_name, c_d_name)

        def extract_data(self, line):
            c_w_id, c_d_id, c_id, c_first, c_middle, c_last =                       \
                line[0], line[1], line[2], line[3], line[4], line[5]
            key = self.JOIN_CH.join((c_w_id, c_d_id, c_id))
            self.map_c_name[key] = (c_first, c_middle, c_last)

        self.execute_in_batch(session, reader, query, extract_data, get_params_customer_data_line)


    """
        Loads order data
    """
    @timemeasure
    def load_order_data(self, csv_file, session):
        query = session.prepare("INSERT INTO order_ (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, "
                                "o_ol_cnt, o_all_local, o_entry_d, c_first, c_middle, c_last) "
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        def get_params_order_data_line(self, line):
            o_entry_d = Utility.convert_to_datetime_object(line[7])
            o_w_id, o_d_id, o_c_id = line[0], line[1], line[3]
            c_first, c_middle, c_last = self.map_c_name[self.JOIN_CH.join((o_w_id, o_d_id, o_c_id))]
            return (int(line[0]), int(line[1]), int(line[2]), int(line[3]),
                     int(line[4]), float(line[5]), float(line[6]), o_entry_d, c_first, c_middle, c_last)

        def extract_data(self, line):
            o_w_id, o_d_id, o_carrier_id = line[0], line[1], int(line[4])
            if o_carrier_id > 0:
                key = self.JOIN_CH.join((o_w_id, o_d_id))
                last_delivery = self.map_last_delivery[key] if key in self.map_last_delivery else 0
                self.map_last_delivery[key] = max(last_delivery, o_carrier_id)

        self.execute_in_batch(session, reader, query, extract_data, get_params_order_data_line)


    """
        Loads item data
    """
    @timemeasure
    def load_item_data(self, csv_file, session):
        query = session.prepare("INSERT INTO item (i_id, i_name, i_price, i_im_id, i_data)"
                                "VALUES(?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        def get_params_item_data_line(self, line):
            return (int(line[0]), line[1], float(line[2]), int(line[3]), line[4])

        def extract_data(self, line):
            i_id, i_name, i_price = line[0], line[1], line[2]
            self.map_i_name[i_id] = i_name
            self.map_i_price[i_id] = i_price

        self.execute_in_batch(session, reader, query, extract_data, get_params_item_data_line)


    """
        Loads order-line data
    """
    @timemeasure
    def load_order_line_data(self, csv_file, session):
        query = session.prepare("INSERT INTO order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, "
                                "ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, "
                                "ol_dist_info, i_name) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        def get_params_order_line_data_line(self, line):
            delivery_time = line[5]
            delivery_time = Utility.get_time_now() if delivery_time == "-1" else Utility.convert_to_datetime_object(line[5])
            ol_i_id = line[4]
            i_name = self.map_i_name[ol_i_id]
            return (int(line[0]), int(line[1]), int(line[2]), int(line[3]), int(line[4]),
                    delivery_time, float(line[6]), int(line[7]), float(line[8]), line[9], i_name)

        def extract_data(self, line):
            pass

        self.execute_in_batch(session, reader, query, extract_data, get_params_order_line_data_line)


    """
        Loads stock data
    """
    @timemeasure
    def load_stock_data(self, csv_file, session):
        query = session.prepare("INSERT INTO stock (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, "
                                "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
                                "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data, i_price) "
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        def get_params_stock_data_line(self, line):
            s_i_id = line[1]
            i_price = self.map_i_price[s_i_id]
            return (int(line[0]), int(line[1]), float(line[2]), float(line[3]), int(line[4]), int(line[5]),
                    line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15],
                    line[16], float(i_price))

        def extract_data(self, line):
            pass

        self.execute_in_batch(session, reader, query, extract_data, get_params_stock_data_line)


    """
        Loads warehouse tax data
    """
    @timemeasure
    def load_warehouse_tax_data(self, csv_file, session):
        query = session.prepare("INSERT INTO warehouse_tax (w_id, w_tax) VALUES(?, ?)")
        reader = csv.reader(csv_file)
        query_and_params = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            query_and_params.append((query, (int(line[0]), float(line[7]))))

        execute_concurrent(session, query_and_params, raise_on_first_error=True)


    """
        Loads district next order id data
    """
    @timemeasure
    def load_district_next_order_id_data(self, csv_file, session):
        query = session.prepare("INSERT INTO district_next_order_id (d_w_id, d_id, d_tax, d_next_o_id) "
                                "VALUES(?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        query_and_params = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            query_and_params.append((query, (int(line[0]), int(line[1]), float(line[8]), int(line[10]))))

        execute_concurrent(session, query_and_params, raise_on_first_error=True)


    """
        Reads and passes data to corresponding handling method.
    """
    def execute(self):
        # Initialize map for denormalizing tables
        self.map_d_name, self.map_w_name, self.map_c_name = {}, {}, {}
        self.map_last_delivery, self.map_i_name, self.map_i_price = {}, {}, {}

        # Connect to cassandra server
        cluster = Cluster()
        session = cluster.connect(DataLoader.DATABASE_NAME)

        if not self.only_additional_tables:
            self.load_warehouse_data(open(self.WAREHOUSE_FILE_PATH), session)
            self.load_district_data(open(self.DISTRICT_FILE_PATH), session)
            self.load_customer_data(open(self.CUSTOMER_FILE_PATH), session)
            self.load_order_data(open(self.ORDER_FILE_PATH), session)
            self.load_item_data(open(self.ITEM_FILE_PATH), session)
            self.load_order_line_data(open(self.ORDER_LINE_FILE_PATH), session)
            self.load_stock_data(open(self.STOCK_FILE_PATH), session)
            self.load_district_next_smallest_order_id_data(open(self.DISTRICT_FILE_PATH), session)
        self.load_warehouse_tax_data(open(self.WAREHOUSE_FILE_PATH), session)
        self.load_district_next_order_id_data(open(self.DISTRICT_FILE_PATH), session)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    # List of arguments
    ap.add_argument("-p", "--path", required=True, help="Path to directory containing data")
    ap.add_argument("-c", "--count", required=False, help="Limit number of rows to be processed")
    ap.add_argument("-a", "--add", required=False, help="Only load additional tables (warehouse_tax, district_next_order_data)")
    args = vars(ap.parse_args())

    # Getting arguments from user
    dir_path = args['path']
    row_count = int(args['count']) if args['count'] else args['count']
    only_add = args['add']

    loader = DataLoader(dir_path, row_count, only_add)
    loader.execute()
