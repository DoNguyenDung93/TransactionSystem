import argparse
import csv

from Utility import Utility
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent

class DataLoader:

    DATABASE_NAME = "cs4224"
    DEFAULT_DIRECTORY = "."
    DEFAULT_ROW_READ = 1000000000
    TOKEN_SEPARATOR = ","

    def __init__(self, dir_path = DEFAULT_DIRECTORY, row_count = DEFAULT_ROW_READ):
        self.DIR_PATH = dir_path
        self.ROW_COUNT = row_count
        self.WAREHOUSE_FILE_PATH  = self.DIR_PATH + "/warehouse.csv"
        self.ORDER_FILE_PATH      = self.DIR_PATH + "/order.csv"
        self.DISTRICT_FILE_PATH   = self.DIR_PATH + "/district.csv"
        self.CUSTOMER_FILE_PATH   = self.DIR_PATH + "/customer.csv"
        self.ITEM_FILE_PATH       = self.DIR_PATH + "/item.csv"
        self.ORDER_LINE_FILE_PATH = self.DIR_PATH + "/order-line.csv"
        self.STOCK_FILE_PATH      = self.DIR_PATH + "/stock.csv"


    """
        Load warehouse data
    """
    def load_warehouse_data(self, csv_file, session):
        query = session.prepare("INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state,"
                                "w_zip, w_ytd) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        query_and_params = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            params = (int(line[0]), line[1], line[2], line[3], line[4], line[5], line[6], float(line[8]))
            query_and_params.append((query, params))
            # Duplicate w_name for customer table
            w_id, w_name = line[0], line[1]
            self.map_w_name[w_id] = w_name

        execute_concurrent(session, query_and_params, raise_on_first_error=True)


    """
        Load district data
    """
    def load_district_data(self, csv_file, session):
        query = session.prepare("INSERT INTO district (d_w_id, d_id, d_name,"
                                "d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd)"
                                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        query_and_params = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            params = (int(line[0]), int(line[1]), line[2], line[3], line[4], line[5], line[6], line[7], float(line[9]))
            query_and_params.append((query, params))
            # Duplicate d_name for customer table
            d_w_id, d_id, d_name = line[0], line[1], line[2]
            self.map_d_name[d_w_id + "-" + d_id] = d_name

        execute_concurrent(session, query_and_params, raise_on_first_error=True)

    """
        Load customer data
    """
    def load_customer_data(self, csv_file, session):
        query = session.prepare("INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, "
                                "c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, "
                                "c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, "
                                "c_payment_cnt, c_delivery_cnt, c_data, w_name, d_name) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        query_and_params = []
        query_count = 0
        for line in itertools.islice(reader, self.ROW_COUNT):
            datetime = Utility.convert_to_datetime_object(line[12])
            # Reading w_name, d_name from maps created in previous procedures
            c_w_id, c_d_id = line[0], line[1]
            c_w_name, c_d_name = self.map_w_name[c_w_id], self.map_d_name[c_w_id + "-" + c_d_id]
            params = (int(line[0]), int(line[1]), int(line[2]), line[3], line[4], line[5],
                      line[6], line[7], line[8], line[9], line[10], line[11], datetime,
                      line[13], float(line[14]), float(line[15]), float(line[16]), float(line[17]),
                      int(line[18]), int(line[19]), line[20], c_w_name, c_d_name)
            query_and_params.append((query, params))

        execute_concurrent(session, query_and_params, raise_on_first_error=True)

    """
        Loads order data
    """
    def load_order_data(self, csv_file, session):
        pass


    """
        Reads and passes data to corresponding handling method.
    """
    def execute(self):
        # Initialize map for denormalizing tables
        self.map_d_name, self.map_w_name = {}, {}

        # Connect to cassandra server
        cluster = Cluster()
        session = cluster.connect(DataLoader.DATABASE_NAME)

        self.load_warehouse_data(open(self.WAREHOUSE_FILE_PATH), session)
        self.load_district_data(open(self.DISTRICT_FILE_PATH), session)
        self.load_customer_data(open(self.CUSTOMER_FILE_PATH), session)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    # List of arguments
    ap.add_argument("-p", "--path", required=True, help="Path to directory containing data")
    ap.add_argument("-c", "--count", required=False, help="Limit number of rows to be processed")
    args = vars(ap.parse_args())

    # Getting arguments from user
    dir_path = args['path']
    row_count = args['count']

    loader = DataLoader(dir_path, row_count)
    loader.execute()
