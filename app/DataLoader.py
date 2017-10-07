import argparse
import csv

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent

class DataLoader:

    DATABASE_NAME = "cs4224"
    DEFAULT_DIRECTORY = "."
    TOKEN_SEPARATOR = ","

    def __init__(self, dir_path = DEFAULT_DIRECTORY):
        self.DIR_PATH = dir_path
        self.WAREHOUSE_FILE_PATH  = self.DIR_PATH + "/warehouse.csv"
        self.ORDER_FILE_PATH      = self.DIR_PATH + "/order.csv"
        self.DISTRICT_FILE_PATH   = self.DIR_PATH + "/district.csv"
        self.CUSTOMER_FILE_PATH   = self.DIR_PATH + "/customer.csv"
        self.ITEM_FILE_PATH       = self.DIR_PATH + "/item.csv"
        self.ORDER_LINE_FILE_PATH = self.DIR_PATH + "/order-line.csv"
        self.STOCK_FILE_PATH      = self.DIR_PATH + "/stock.csv"

    # Load warehouse data
    def load_warehouse_data(self, csv_file, session):
        query = session.prepare("INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state,"
                                "w_zip, w_ytd) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        reader = csv.reader(csv_file)
        query_and_params = []
        for line in reader:
            w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd =      \
                int(line[0]), line[1], line[2], line[3], line[4], line[5], line[6], float(line[8])
            params = (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd)
            query_and_params.append((query, params))

        execute_concurrent(session, query_and_params, raise_on_first_error=True)

    # Load district data
    def load_district_data(self, csv_file, session):
        pass

    def execute(self):
        # Connect to cassandra server
        cluster = Cluster()
        session = cluster.connect(DataLoader.DATABASE_NAME)

        self.load_warehouse_data(open(self.WAREHOUSE_FILE_PATH), session)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    # List of arguments
    ap.add_argument("-p", "--path", required=True, help="Path to directory containing data")
    args = vars(ap.parse_args())

    dir_path = args['path']

    loader = DataLoader(dir_path)
    loader.execute()
