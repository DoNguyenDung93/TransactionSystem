from Transaction import Transaction
from datetime import datetime

# Delivery Transaction (Transaction #3)
# This transaction is used to process the delivery of the oldest yet-to-be-delivered order for each of the 10
# districts in a specified warehouse.

class DeliveryTransaction(Transaction):
    def execute(self, params):
        w_id = int(params['w_id'])
        carrier_id = int(params['carrier_id'])

        # Prepared Statements
        self.get_smallest_order_number_query = self.session.prepare('select o_id from order_ where o_w_id = {} '
                                                                    'and o_d_id = ?'
                                                                    'and o_carrier_id = -1 limit 1 allow filtering'
                                                                    .format(w_id))
        self.find_customer_query = self.session.prepare('select o_c_id from order_ where o_id = ? '
                                                        'and o_w_id = {} and o_d_id = ? '
                                                        'allow filtering'
                                                        .format(w_id))
        self.update_order_query = self.session.prepare('update order_ set o_carrier_id = ? where o_id = ? '
                                                       'and o_w_id = {} and o_d_id = ?'.format(w_id))
        self.get_order_line_number_query = self.session.prepare('select ol_number from order_line where '
                                                                'ol_o_id = ? and ol_w_id = {} and ol_d_id = ?'
                                                                .format(w_id))
        self.update_order_line_query = self.session.prepare('update order_line set ol_delivery_d = ? where ol_o_id = ? '
                                                            'and ol_w_id = {} and ol_d_id = ? and ol_number = ?'
                                                            .format(w_id))
        self.get_order_sum_query = self.session.prepare('select ol_amount from order_line where ol_o_id = ? '
                                                        'and ol_d_id = ? and ol_w_id = {} allow filtering'
                                                        .format(w_id))
        self.get_customer_balance_query = self.session.prepare('select c_balance from customer where '
                                                               'c_id = ? and c_w_id = {} '
                                                               'and c_d_id = ?'.format(w_id))
        self.update_customer_balance_query = self.session.prepare('update customer set c_balance = ? '
                                                                  'where c_id = ? and c_w_id = {} '
                                                                  'and c_d_id = ?'.format(w_id))
        self.get_delivery_count_query = self.session.prepare('select c_delivery_cnt from customer where '
                                                             'c_id = ? and c_w_id = {} and c_d_id = ?'.format(w_id))
        self.update_customer_delivery_cnt_query = self.session.prepare('update customer set c_delivery_cnt '
                                                                       '= ? where c_id = ? '
                                                                       'and c_w_id = {} and c_d_id = ?'.format(w_id))

        for num in range(1, 10):
            smallest_order_number = self.get_smallest_order_number(num)
            customer_id = self.find_customer(smallest_order_number, num)
            self.update_order(smallest_order_number, carrier_id, num)
            ol_number = self.get_order_line_number(smallest_order_number, num)
            for index in range(len(list(ol_number))):
                self.update_order_line(smallest_order_number, num, ol_number[index])
            sum_order = self.get_order_sum(smallest_order_number, num)
            current_balance = self.get_customer_balance(customer_id, num)
            self.update_customer_balance(customer_id, sum_order, num, current_balance)
            delivery_cnt = self.get_delivery_count(customer_id, num)
            self.update_customer_delivery_cnt(customer_id, num, delivery_cnt)

    # Find the order with the smallest O_ID using W_ID and DISTRICT_NO from 1 to 10
    # and O_CARRIER_ID = -1
    def get_smallest_order_number(self, num):
        result = self.session.execute(self.get_smallest_order_number_query.bind(num))
        # smallest_result = result[0].o_id
        # for index in range(len(list(result))):
        #     if result[index].o_id < smallest_result:
        #         smallest_result = result[index].o_id
        return result

    # Find customer corresponding to the smallest order number
    def find_customer(self, smallest_order_number, num):
        result = self.session.execute(self.find_customer_query.bind(smallest_order_number, num))
        return result[0].o_c_id

    # Update the O_CARRIER_ID with CARRIER_ID input
    def update_order(self, smallest_order_number, carrier_id, num):
        self.session.execute(self.update_order_query.bind(carrier_id, smallest_order_number, num))

    # Find all the order-line numbers of each order
    def get_order_line_number(self, smallest_order_number, num):
        return self.session.execute(self.get_order_line_number_query.bind(smallest_order_number, num))

    # Update all the order-line in the order by setting OL_DELIVERY_D to the current date and time
    def update_order_line(self, smallest_order_number, num, ol_number):
        time = datetime.utcnow()
        self.session.execute(self.update_order_line_query.bind(
            time.strftime('%Y-%m-%d %H:%M:%S'), smallest_order_number, num, ol_number))

    # Get the sum of value of all the items in the order
    def get_order_sum(self, smallest_order_number, num):
        result = self.session.execute(self.get_order_sum_query.bind(smallest_order_number, num))
        sum_order = 0.0
        for index in range(len(list(result))):
            sum_order += result[index].ol_amount
        return sum_order

    # Get the current balance of the customer
    def get_customer_balance(self, customer_id, num):
        result = self.session.execute(self.get_customer_balance_query.bind(customer_id, num))
        return result[0].c_balance

    # Increase the current customer balance with the value of the order
    def update_customer_balance(self, customer_id, sum_order, num, current_balance):
        self.session.execute(self.update_customer_balance_query.bind(current_balance + sum_order, customer_id, num))

    # Get the delivery count of the customer
    def get_delivery_count(self, customer_id, num):
        result = self.session.execute(self.get_delivery_count_query.bind(customer_id, num))
        return result[0].c_delivery_cnt

    # Increase the number of delivery to the customer by 1
    def update_customer_delivery_cnt(self, customer_id, num, delivery_cnt):
        self.session.execute(self.update_customer_delivery_cnt_query.bind(delivery_cnt + 1, customer_id, num))
