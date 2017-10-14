import argparse
import csv
import itertools

from Utility import Utility
from StatsCollector import StatsCollector

class DataTransformer:

    DATABASE_NAME = "cs4224"
    DEFAULT_DIRECTORY = "."
    DEFAULT_ROW_READ = 1000000000
    DEFAULT_OUT_DIR = "testdata"
    TOKEN_SEPARATOR = ","
    JOIN_CH = "-"

    def __init__(self, dir_path = DEFAULT_DIRECTORY, row_count = DEFAULT_ROW_READ, out_dir = DEFAULT_OUT_DIR):
        self.OUT_DIR = out_dir
        self.DIR_PATH = dir_path
        self.ROW_COUNT = row_count
        self.WAREHOUSE_FILE_PATH  = self.DIR_PATH + "/warehouse.csv"
        self.ORDER_FILE_PATH      = self.DIR_PATH + "/order.csv"
        self.DISTRICT_FILE_PATH   = self.DIR_PATH + "/district.csv"
        self.CUSTOMER_FILE_PATH   = self.DIR_PATH + "/customer.csv"
        self.ITEM_FILE_PATH       = self.DIR_PATH + "/item.csv"
        self.ORDER_LINE_FILE_PATH = self.DIR_PATH + "/order-line.csv"
        self.STOCK_FILE_PATH      = self.DIR_PATH + "/stock.csv"

        self.OUT_WAREHOUSE_FILE_PATH  = self.OUT_DIR + "/warehouse.csv"
        self.OUT_ORDER_FILE_PATH      = self.OUT_DIR + "/order.csv"
        self.OUT_DISTRICT_FILE_PATH   = self.OUT_DIR + "/district.csv"
        self.OUT_CUSTOMER_FILE_PATH   = self.OUT_DIR + "/customer.csv"
        self.OUT_ITEM_FILE_PATH       = self.OUT_DIR + "/item.csv"
        self.OUT_ORDER_LINE_FILE_PATH = self.OUT_DIR + "/order-line.csv"
        self.OUT_STOCK_FILE_PATH      = self.OUT_DIR + "/stock.csv"
        self.OUT_WAREHOUSE_TAX_FILE_PATH = self.OUT_DIR + "/warehouse-tax.csv"
        self.OUT_DISTRICT_NEXT_ORDER_ID  = self.OUT_DIR + "/district-next-order-id.csv"
        self.OUT_DISTRICT_NEXT_SMALLEST_ORDER_ID_DATA =           \
            self.OUT_DIR + "/district-next-smallest-order-id.csv"

    def timemeasure(original_function):
        def new_function(*args, **kwargs):
            timer = StatsCollector.Timer()
            timer.start()
            original_function(*args, **kwargs)
            timer.finish()
            print "%s finished in %s s" % (original_function.__name__, timer.get_last())
        return new_function


    """
        Load warehouse data
    """
    @timemeasure
    def load_warehouse_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            params = (line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[8])
            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")

            # Stores data needed for other methods
            w_id, w_name = line[0], line[1]
            self.map_w_name[w_id] = w_name


    """
        Load district data
    """
    @timemeasure
    def load_district_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            params = (line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[9])
            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")

            # Stores data needed for other methods
            d_w_id, d_id, d_name = line[0], line[1], line[2]
            self.map_d_name[self.JOIN_CH.join((d_w_id, d_id))] = d_name


    """
        Load district_next_smallest_order_id data
    """
    @timemeasure
    def load_district_next_smallest_order_id_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            d_w_id, d_id = line[0], line[1]
            key = self.JOIN_CH.join((d_w_id, d_id))
            d_next_smallest_o_id = (self.map_last_delivery[key] if key in self.map_last_delivery else 0) + 1
            params = (d_w_id, d_id, str(d_next_smallest_o_id))
            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")


    """
        Load customer data
    """
    @timemeasure
    def load_customer_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            # Reading w_name, d_name from maps created in previous procedures
            c_w_id, c_d_id = line[0], line[1]
            c_w_name, c_d_name = self.map_w_name[c_w_id], self.map_d_name[self.JOIN_CH.join((c_w_id, c_d_id))]
            params = (line[0], line[1], line[2], line[3], line[4], line[5],
                      line[6], line[7], line[8], line[9], line[10], line[11], line[12],
                      line[13], line[14], line[15], line[16], line[17],
                      line[18], line[19], line[20], c_w_name, c_d_name)

            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")

            # Extract data for other methods
            c_w_id, c_d_id, c_id, c_first, c_middle, c_last =                       \
                line[0], line[1], line[2], line[3], line[4], line[5]
            key = self.JOIN_CH.join((c_w_id, c_d_id, c_id))
            self.map_c_name[key] = (c_first, c_middle, c_last)


    """
        Loads order data
    """
    @timemeasure
    def load_order_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            o_entry_d = Utility.convert_to_datetime_object(line[7])
            o_w_id, o_d_id, o_c_id = line[0], line[1], line[3]
            c_first, c_middle, c_last = self.map_c_name[self.JOIN_CH.join((o_w_id, o_d_id, o_c_id))]

            params = (line[0], line[1], line[2], line[3],
                      line[4], line[5], line[6], line[7], c_first, c_middle, c_last)
            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")

            # Extract data for other methods
            o_w_id, o_d_id, o_carrier_id = line[0], line[1], int(line[4])
            if o_carrier_id > 0:
                key = self.JOIN_CH.join((o_w_id, o_d_id))
                last_delivery = self.map_last_delivery[key] if key in self.map_last_delivery else 0
                self.map_last_delivery[key] = max(last_delivery, o_carrier_id)


    """
        Loads item data
    """
    @timemeasure
    def load_item_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            params = (line[0], line[1], line[2], line[3], line[4])
            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")

            # Extract data for other methods
            i_id, i_name, i_price = line[0], line[1], line[2]
            self.map_i_name[i_id] = i_name
            self.map_i_price[i_id] = i_price


    """
        Loads order-line data
    """
    @timemeasure
    def load_order_line_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            ol_i_id = line[4]
            i_name = self.map_i_name[ol_i_id]
            params = (line[0], line[1], line[2], line[3], line[4],
                      line[5], line[6], line[7], line[8], line[9], i_name)
            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")


    """
        Loads stock data
    """
    @timemeasure
    def load_stock_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            s_i_id = line[1]
            i_price = self.map_i_price[s_i_id]
            i_name = self.map_i_name[s_i_id]
            params = (line[0], line[1], line[2], line[3], line[4],
                      line[5], line[6], line[7], line[8], line[9], line[10], line[11],
                      line[12], line[13], line[14], line[15], line[16], i_price, i_name)
            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")


    """
        Loads warehouse tax data
    """
    @timemeasure
    def load_warehouse_tax_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            params = (line[0], line[7])
            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")


    """
        Loads district next order id data
    """
    @timemeasure
    def load_district_next_order_id_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            params = (line[0], line[1], line[8], line[10])
            out_file.write(self.TOKEN_SEPARATOR.join(params) + "\n")


    """
        Reads and passes data to corresponding handling method.
    """
    def execute(self):
        # Initialize map for denormalizing tables
        self.map_d_name, self.map_w_name, self.map_c_name = {}, {}, {}
        self.map_last_delivery, self.map_i_name, self.map_i_price = {}, {}, {}

        self.load_warehouse_data(
                open(self.WAREHOUSE_FILE_PATH),
                open(self.OUT_WAREHOUSE_FILE_PATH, "w"))
        self.load_district_data(
                open(self.DISTRICT_FILE_PATH),
                open(self.OUT_DISTRICT_FILE_PATH, "w"))
        self.load_customer_data(
                open(self.CUSTOMER_FILE_PATH),
                open(self.OUT_CUSTOMER_FILE_PATH, "w"))
        self.load_order_data(
                open(self.ORDER_FILE_PATH),
                open(self.OUT_ORDER_FILE_PATH, "w"))
        self.load_item_data(
                open(self.ITEM_FILE_PATH),
                open(self.OUT_ITEM_FILE_PATH, "w"))
        self.load_order_line_data(
                open(self.ORDER_LINE_FILE_PATH),
                open(self.OUT_ORDER_LINE_FILE_PATH, "w"))
        self.load_stock_data(
                open(self.STOCK_FILE_PATH),
                open(self.OUT_STOCK_FILE_PATH, "w"))
        self.load_district_next_smallest_order_id_data(
                open(self.DISTRICT_FILE_PATH),
                open(self.OUT_DISTRICT_NEXT_SMALLEST_ORDER_ID_DATA, "w"))
        self.load_warehouse_tax_data(
                open(self.WAREHOUSE_FILE_PATH),
                open(self.OUT_WAREHOUSE_TAX_FILE_PATH, "w"))
        self.load_district_next_order_id_data(
                open(self.DISTRICT_FILE_PATH),
                open(self.OUT_DISTRICT_NEXT_ORDER_ID, "w"))


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    # List of arguments
    ap.add_argument("-p", "--path", required=True, help="Path to directory containing data")
    ap.add_argument("-c", "--count", required=False, help="Limit number of rows to be processed")
    ap.add_argument("-o", "--output", required=True, help="Output directory")
    args = vars(ap.parse_args())

    # Getting arguments from user
    dir_path = args['path']
    row_count = int(args['count']) if args['count'] else args['count']
    out_dir = args['output']

    transformer = DataTransformer(dir_path, row_count, out_dir)
    transformer.execute()
