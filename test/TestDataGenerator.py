import random
import string

from datetime import datetime

class Randomizer:

    def __init__(self):
        pass

    @staticmethod
    def random_string(length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))

    @staticmethod
    def random_int(length):
        digits = string.digits
        return ''.join(random.choice(digits) for i in range(length))

    @staticmethod
    def random_float(int_length, frac_length):
        digits = string.digits
        int_part = ''.join(random.choice(digits) for i in range(int_length))
        frac_part = ''.join(random.choice(digits) for i in range(frac_length))
        return int_part + '.' + frac_part

    @staticmethod
    def random_time():
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def flip_coin():
        return random.randint(0, 1) == 0

class TestGen:

    DIRECTORY = "test_data"
    WAREHOUSE = DIRECTORY + "/warehouse.csv"
    DISTRICT = DIRECTORY + "/district.csv"
    CUSTOMER = DIRECTORY + "/customer.csv"
    ORDER = DIRECTORY + "/order.csv"
    ITEM = DIRECTORY + "/item.csv"
    ORDER_LINE = DIRECTORY + "/order-line.csv"
    STOCK = DIRECTORY + "/stock.csv"

    N_WAREHOUSE = 10
    N_DISTRICT_PER_W = 10
    N_CUSTOMER_PER_D = 10
    N_ORDER_PER_D = 50
    N_ORDER_LINE_PER_O = 5
    N_ITEM = 100

    def __init__(self):
        pass

    def generate_warehouse(self, csv_file):
        for w_id in range(TestGen.N_WAREHOUSE):
            w_name = Randomizer.random_string(10)
            w_street_1 = Randomizer.random_string(20)
            w_street_2 = Randomizer.random_string(20)
            w_city = Randomizer.random_string(20)
            w_state = Randomizer.random_string(2)
            w_zip = Randomizer.random_string(9)
            w_tax = Randomizer.random_float(4, 4)
            w_ytd = Randomizer.random_float(12, 2)
            line = ','.join((str(w_id), w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd))
            csv_file.write(line + '\n')

    def generate_district(self, csv_file):
        for d_w_id in range(TestGen.N_WAREHOUSE):
            for d_id in range(TestGen.N_DISTRICT_PER_W):
                d_name = Randomizer.random_string(10)
                d_street_1 = Randomizer.random_string(20)
                d_street_2 = Randomizer.random_string(20)
                d_city = Randomizer.random_string(20)
                d_state = Randomizer.random_string(2)
                d_zip = Randomizer.random_string(9)
                d_tax = Randomizer.random_float(4, 4)
                d_ytd = Randomizer.random_float(12, 2)
                d_next_o_id = TestGen.N_ORDER_PER_D
                line = ','.join((str(d_w_id), str(d_id), d_name, d_street_1, d_street_2, d_city,
                                d_state, d_zip, d_tax, d_ytd, str(d_next_o_id)))
                csv_file.write(line + '\n')

    def generate_customer(self, csv_file):
        for c_w_id in range(TestGen.N_WAREHOUSE):
            for c_d_id in range(TestGen.N_DISTRICT_PER_W):
                for c_id in range(TestGen.N_CUSTOMER_PER_D):
                    c_first = Randomizer.random_string(16)
                    c_middle = Randomizer.random_string(2)
                    c_last = Randomizer.random_string(16)
                    c_street_1 = Randomizer.random_string(20)
                    c_street_2 = Randomizer.random_string(20)
                    c_city = Randomizer.random_string(20)
                    c_state = Randomizer.random_string(2)
                    c_zip = Randomizer.random_string(9)
                    c_phone = Randomizer.random_string(16)
                    c_since = Randomizer.random_time()
                    c_credit = Randomizer.random_string(2)
                    c_credit_lim = Randomizer.random_float(12, 2)
                    c_discount = Randomizer.random_float(4, 4)
                    c_balance = Randomizer.random_float(12, 2)
                    c_ytd_payment = Randomizer.random_float(4, 4)
                    c_payment_cnt = Randomizer.random_int(3)
                    c_deliver_cnt = Randomizer.random_int(3)
                    c_data = Randomizer.random_string(500)
                    line = ','.join((str(c_w_id), str(c_d_id), str(c_id), c_first, c_middle, c_last,
                                    c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
                                    c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment,
                                    c_payment_cnt, c_deliver_cnt, c_data))
                    csv_file.write(line + '\n')

    def generate_order(self, csv_file):
        for o_w_id in range(TestGen.N_WAREHOUSE):
            for o_d_id in range(TestGen.N_DISTRICT_PER_W):
                for o_id in range(TestGen.N_ORDER_PER_D):
                    o_c_id = random.randint(0, TestGen.N_CUSTOMER_PER_D - 1)
                    o_carrier_id = "0" if Randomizer.flip_coin() else Randomizer.random_int(3)
                    o_ol_cnt = Randomizer.random_float(2, 0)
                    o_all_local = Randomizer.random_float(1, 0)
                    o_entry_d = Randomizer.random_time()
                    line = ','.join((str(o_w_id), str(o_d_id), str(o_id), str(o_c_id), o_carrier_id, o_ol_cnt, o_all_local, o_entry_d))
                    csv_file.write(line + '\n')

    def generate_item(self, csv_file):
        for i_id in range(TestGen.N_ITEM):
            i_name = Randomizer.random_string(24)
            i_price = Randomizer.random_float(5, 2)
            i_im_id = Randomizer.random_int(4)
            i_data = Randomizer.random_string(50)
            line = ','.join((str(i_id), i_name, i_price, i_im_id, i_data))
            csv_file.write(line + '\n')

    def generate_order_line(self, csv_file):
        for ol_w_id in range(TestGen.N_WAREHOUSE):
            for ol_d_id in range(TestGen.N_DISTRICT_PER_W):
                for ol_o_id in range(TestGen.N_ORDER_PER_D):
                    for ol_number in range(TestGen.N_ORDER_LINE_PER_O):
                        ol_i_id = random.randint(0, TestGen.N_ITEM - 1)
                        ol_delivery_d = Randomizer.random_time()
                        ol_amount = Randomizer.random_float(4, 2)
                        ol_supply_w_id = random.randint(0, TestGen.N_WAREHOUSE - 1)
                        ol_quantity = Randomizer.random_float(2, 0)
                        ol_dist_info = Randomizer.random_string(24)
                        line = ','.join((str(ol_w_id), str(ol_d_id), str(ol_o_id), str(ol_number),
                                        str(ol_i_id), ol_delivery_d, ol_amount, str(ol_supply_w_id), ol_quantity,
                                        ol_dist_info))
                        csv_file.write(line + '\n')

    def generate_stock(self, csv_file):
        for s_w_id in range(TestGen.N_WAREHOUSE):
            for s_i_id in range(TestGen.N_ITEM):
                s_quantity = Randomizer.random_float(4, 0)
                s_ytd = Randomizer.random_float(8, 2)
                s_order_cnt = Randomizer.random_int(3)
                s_remote_cnt = Randomizer.random_int(3)
                s_dist_01 = Randomizer.random_string(24)
                s_dist_02 = Randomizer.random_string(24)
                s_dist_03 = Randomizer.random_string(24)
                s_dist_04 = Randomizer.random_string(24)
                s_dist_05 = Randomizer.random_string(24)
                s_dist_06 = Randomizer.random_string(24)
                s_dist_07 = Randomizer.random_string(24)
                s_dist_08 = Randomizer.random_string(24)
                s_dist_09 = Randomizer.random_string(24)
                s_dist_10 = Randomizer.random_string(24)
                s_data = Randomizer.random_string(50)
                line = ','.join((str(s_w_id), str(s_i_id), s_quantity, s_ytd, s_order_cnt, s_remote_cnt,
                                s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
                                s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data))
                csv_file.write(line + '\n')


    def execute(self):
        self.generate_warehouse(open(TestGen.WAREHOUSE, "w"))
        self.generate_district(open(TestGen.DISTRICT, "w"))
        self.generate_customer(open(TestGen.CUSTOMER, "w"))
        self.generate_order(open(TestGen.ORDER, "w"))
        self.generate_item(open(TestGen.ITEM, "w"))
        self.generate_order_line(open(TestGen.ORDER_LINE, "w"))
        self.generate_stock(open(TestGen.STOCK, "w"))

if __name__ == "__main__":
    test_generator = TestGen()
    test_generator.execute()
