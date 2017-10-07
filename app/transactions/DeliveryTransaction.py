from Transaction import Transaction
from datetime import datetime
import decimal

# Delivery Transaction (Transaction #3)
# This transaction is used to process the delivery of the oldest yet-to-be-delivered order for each of the 10
# districts in a specified warehouse.

class DeliveryTransaction(Transaction):
    def execute(self, params):
        w_id = int(params['w_id'])
        carrier_id = int(params['carrier_id'])

        # Prepared Statements
        # self.get_smallest_order_number_query = self.session.prepare('select o_id, o_c_id from order_ where o_w_id = {} '
        #                                                             'and o_d_id = ?'
        #                                                             'and o_carrier_id = -1 limit 1 allow filtering'
        #                                                             .format(w_id))
        self.get_smallest_order_number_query = self.session.prepare('select d_next_smallest_o_id from '
                                                                    'district_next_smallest_order_id where d_w_id = {]'
                                                                    'and d_id = ?'.format(w_id))
        self.get_customer_id_query = self.session.prepare('select o_c_id from order_ where o_w_id = {} '
                                                          'and o_d_id = ? and o_id = ?'.format(w_id))
        self.update_order_query = self.session.prepare('update order_ set o_carrier_id = ? where o_id = ? '
                                                       'and o_w_id = {} and o_d_id = ?'.format(w_id))
        self.get_order_line_number_query = self.session.prepare('select ol_number, ol_amount from order_line where '
                                                                'ol_o_id = ? and ol_w_id = {} and ol_d_id = ?'
                                                                .format(w_id))
        self.update_order_line_query = self.session.prepare('update order_line set ol_delivery_d = ? where ol_o_id = ? '
                                                            'and ol_w_id = {} and ol_d_id = ? and ol_number = ?'
                                                            .format(w_id))
        self.get_customer_balance_delivery_query = self.session.prepare('select c_balance, c_delivery_cnt '
                                                                        'from customer where c_id = ? and c_w_id = {} '
                                                                        'and c_d_id = ?'.format(w_id))
        self.update_customer_balance_delivery_query = self.session.prepare('update customer set c_balance = ?,'
                                                                           'c_delivery_cnt = ? '
                                                                           'where c_id = ? and c_w_id = {} '
                                                                           'and c_d_id = ?'.format(w_id))

        for num in range(1, 10):
            # order_info = self.get_smallest_order_number(num)
            # if order_info is None:
            #     continue
            # smallest_order_number = int(order_info[0])
            # customer_id = int(order_info[1])
            smallest_order_number = self.get_smallest_order_number(num)
            if smallest_order_number is None:
                continue
            customer_id = self.get_customer_id(num, smallest_order_number)
            self.update_order(smallest_order_number, carrier_id, num)
            sum_order = decimal.Decimal(0)
            for ol_number in self.get_order_line_number(smallest_order_number, num):
                sum_order += ol_number[1]
                self.update_order_line(smallest_order_number, num, ol_number[0])
            customer_balance_delivery = self.get_customer_balance_delivery(customer_id, num)
            delivery_cnt = customer_balance_delivery.c_delivery_cnt
            current_balance = customer_balance_delivery.c_balance
            self.update_customer_balance_delivery(customer_id, sum_order, num, current_balance, delivery_cnt)

    # Find the order with the smallest O_ID using W_ID and DISTRICT_NO from 1 to 10
    # and O_CARRIER_ID = -1
    def get_smallest_order_number(self, num):
        result = self.session.execute(self.get_smallest_order_number_query.bind([num]))
        # smallest_result = result[0].o_id
        # for index in range(len(list(result))):
        #     if result[index].o_id < smallest_result:
        #         smallest_result = result[index].o_id
        return result[0].d_next_smallest_o_id

    # Get the customer id of the smallest order id
    def get_customer_id(self, num, smallest_order_number):
        result = self.session.execute(self.get_customer_id_query.bind([num, smallest_order_number]))
        return result[0].o_c_id

    # Update the O_CARRIER_ID with CARRIER_ID input
    def update_order(self, smallest_order_number, carrier_id, num):
        self.session.execute(self.update_order_query.bind([carrier_id, smallest_order_number, num]))

    # Find all the order-line numbers of each order
    def get_order_line_number(self, smallest_order_number, num):
        return self.session.execute(self.get_order_line_number_query.bind([smallest_order_number, num]))

    # Update all the order-line in the order by setting OL_DELIVERY_D to the current date and time
    def update_order_line(self, smallest_order_number, num, ol_number):
        # time = datetime.utcnow().isoformat(' ')
        self.session.execute(self.update_order_line_query.bind(
            [datetime.strptime(datetime.utcnow().isoformat(' '), '%Y-%m-%d %H:%M:%S.%f'),
             smallest_order_number, num, ol_number]))

    # Get the current balance of the customer
    def get_customer_balance_delivery(self, customer_id, num):
        result = self.session.execute(self.get_customer_balance_delivery_query.bind([customer_id, num]))
        return result[0]

    # Increase the current customer balance with the value of the order
    def update_customer_balance_delivery(self, customer_id, sum_order, num, current_balance, delivery_cnt):
        self.session.execute(self.update_customer_balance_delivery_query.bind([current_balance + sum_order,
                                                                               delivery_cnt + 1, customer_id, num]))

