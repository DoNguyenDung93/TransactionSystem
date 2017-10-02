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
                                                                    'and o_carrier_id = -1 allow filtering'.format(w_id))
        self.find_customer_query = self.session.prepare('select o_c_id from order_ where o_id = ? '
                                                        'and o_w_id = {} and o_d_id = ? '
                                                        'allow filtering'
                                                        .format(w_id))
        self.update_order_query = self.session.prepare('update order_ set o_carrier_id = ? where o_id = ? '
                                                       'and o_w_id = {} and o_d_id = ?'.format(w_id))
        self.update_order_line_query = self.session.prepare('update order_line set ol_delivery_d = ? where ol_o_id = ? '
                                                            'and ol_w_id = {} and ol_d_id = ? '.format(w_id))
        self.get_order_sum_query = self.session.prepare('select ol_amount from order_line where ol_o_id = ? '
                                                        'and ol_d_id = ? and ol_w_id = {} allow filtering'
                                                        .format(w_id))
        self.update_customer_balance_query = self.session.prepare('update customer set c_balance = c_balance + ? '
                                                                  'where c_id = ? and c_w_id = {} '
                                                                  'and c_d_id = ?'.format(w_id))
        self.update_customer_delivery_cnt_query = self.session.prepare('update customer set c_delivery_cnt '
                                                                       '= c_delivery_cnt + 1 where c_id = ? '
                                                                       'and c_w_id = {} and c_d_id = ?'.format(w_id))

        for num in range(1, 10):
            smallest_order_number = self.get_smallest_order_number(num)
            customer_id = self.find_customer(smallest_order_number, num)
            self.update_order(smallest_order_number, carrier_id, num)
            self.update_order_line(smallest_order_number, num)
            sum_order = self.get_order_sum(smallest_order_number, num)
            self.update_customer_balance(customer_id, sum_order, num)
            self.update_customer_delivery_cnt(customer_id, num)

    # Find the order with the smallest O_ID using W_ID and DISTRICT_NO from 1 to 10
    # and O_CARRIER_ID = null
    def get_smallest_order_number(self, num):
        result = self.session.execute(self.get_smallest_order_number_query.bind(num))
        smallest_result = result[0].o_id
        for index in range(len(list(result))):
            if result[index].o_id < smallest_result:
                smallest_result = result[index].o_id
        return smallest_result

    # Find customer corresponding to the smallest order number
    def find_customer(self, smallest_order_number, num):
        result = self.session.execute(self.find_customer_query.bind(smallest_order_number, num))
        return result[0].o_c_id

    # Update the O_CARRIER_ID with CARRIER_ID input
    def update_order(self, smallest_order_number, carrier_id, num):
        self.session.execute(self.update_order_query.bind(carrier_id, smallest_order_number, num))

    # Update all the order-line in the order by setting OL_DELIVERY_D to the current date and time
    def update_order_line(self, smallest_order_number, num):
        time = datetime.utcnow()
        self.session.execute(self.update_order_line_query.bind(
            time.strftime('%Y-%m-%d %H:%M:%S'), smallest_order_number, num))

    # Get the sum of value of all the items in the order
    def get_order_sum(self, smallest_order_number, num):
        result = self.session.execute(self.get_order_sum_query.bind(smallest_order_number, num))
        sum_order = 0.0
        for index in range(len(result)):
            sum_order += result[index].ol_amount
        return sum_order

    # Increase the current customer balance with the value of the order
    def update_customer_balance(self, customer_id, sum_order, num):
        self.session.execute(self.update_customer_balance_query.bind(sum_order, customer_id, num))

    # Increase the number of delivery to the customer by 1
    def update_customer_delivery_cnt(self, customer_id, num):
        self.session.execute(self.update_customer_delivery_cnt_query.bind(customer_id, num))
