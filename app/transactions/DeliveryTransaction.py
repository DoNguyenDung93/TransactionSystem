from Transaction import Transaction
from datetime import datetime

# Delivery Transaction (Transaction #3)
# This transaction is used to process the delivery of the oldest yet-to-be-delivered order for each of the 10
# districts in a specified warehouse.

class DeliveryTransaction(Transaction):
    def execute(self, params):
        w_id = params['w_id']
        carrier_id = params['carrier_id']

        for num in range(1, 10):
            smallest_order_number = self.get_smallest_order_number(w_id, num)
            customer_id = self.find_customer(smallest_order_number)
            self.update_order(smallest_order_number, carrier_id)
            self.update_order_line(smallest_order_number)
            sum_order = self.get_order_sum(smallest_order_number)
            self.update_customer_balance(customer_id, sum_order)
            self.update_customer_delivery_cnt(customer_id)

    # Find the order with the smallest O_ID using W_ID and DISTRICT_NO from 1 to 10
    # and O_CARRIER_ID = null
    def get_smallest_order_number(self, w_id, num):
        result = self.session.execute('select o_id from order_ where o_w_id = {}'
                                      ' and o_d_id = {} and o_carrier_id = null'.format(w_id, num))
        smallest_result = result[0].o_id
        for index in range(len(result)):
            if result[index].o_id < smallest_result:
                smallest_result = result[index].o_id
        return smallest_result

    # Find customer corresponding to the smallest order number
    def find_customer(self, smallest_order_number):
        result = self.session.execute('select o_c_id from order_ where o_id = {}'.format(smallest_order_number))
        return result[0].o_c_id

    # Update the O_CARRIER_ID with CARRIER_ID input
    def update_order(self, smallest_order_number, carrier_id):
        self.session.execute('update order_ set o_carrier_id = {} where o_id = {}'
                             .format(carrier_id, smallest_order_number))

    # Update all the order-line in the order by setting OL_DELIVERY_D to the current date and time
    def update_order_line(self, smallest_order_number):
        time = datetime.utcnow()
        self.session.execute('update order_line set ol_delivery_d = {} where ol_o_id = {}'
                             .format(time.strftime('%Y-%m-%d %H:%M:%S'), smallest_order_number))

    # Get the sum of value of all the items in the order
    def get_order_sum(self, smallest_order_number):
        result = self.session.execute('select ol_amount from order_line where o_id = {}'.format(smallest_order_number))
        sum_order = 0.0
        for index in range(len(result)):
            sum_order += result[index].ol_amount
        return sum_order

    # Increase the current customer balance with the value of the order
    def update_customer_balance(self, customer_id, sum_order):
        self.session.execute('update customer set c_balance = c_balance + {} where c_id = {}'
                             .format(sum_order, customer_id))

    # Increase the number of delivery to the customer by 1
    def update_customer_delivery_cnt(self, customer_id):
        self.session.execute('update customer set c_delivery_cnt = c_delivery_cnt + 1 where c_id = {}'
                             .format(customer_id))
