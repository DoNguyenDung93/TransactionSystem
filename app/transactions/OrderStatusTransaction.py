from Transaction import Transaction
from datetime import datetime

# Order Status Transaction (Transaction #4)
# This transaction queries the status of the last order of a customer

class OrderStatusTransaction(Transaction):
    def execute(self, params):
        c_w_id = int(params['c_w_id'])
        c_d_id = int(params['c_d_id'])
        c_id = int(params['c_id'])

        customer_info = self.get_customer_info(c_w_id, c_d_id, c_id)
        # print customer_info
        customer_name_first = customer_info.c_first
        customer_name_middle = customer_info.c_middle
        customer_name_last = customer_info.c_last
        customer_balance = customer_info.c_balance

        last_order = self.get_last_order(c_w_id, c_d_id, c_id)
        # print last_order
        order_number = last_order.o_id
        entry_date = last_order.o_entry_d
        carrier = last_order.o_carrier_id

        order_line = self.get_order_line(c_w_id, c_d_id, order_number)

        print 'Customer Info: ', customer_name_first, customer_name_middle, customer_name_last,\
            ' has balance of ', customer_balance
        print 'Last order info: ', order_number, entry_date, carrier

        for index in order_line:
            # print 'what', index
            item_number = index.ol_i_id
            supply_warehouse = index.ol_supply_w_id
            quantity = index.ol_quantity
            total_price = index.ol_amount
            date_time = index.ol_delivery_d
            print 'Order-line info: ', item_number, supply_warehouse, quantity, total_price, date_time

    # Get the customer info using C_ID
    def get_customer_info(self, c_w_id, c_d_id, c_id):
        result = self.session.execute('select c_first, c_middle, c_last, c_balance from customer where'
                                      ' c_w_id = {} and c_d_id = {} and c_id = {}'.format(c_w_id, c_d_id, c_id))
        if not result:
            print 'Cannot find customer with w_id {} d_id {} c_id {}'.format(c_w_id, c_d_id, c_id)
            return
        return result[0]

    # Get the last order info from the customer
    def get_last_order(self, c_w_id, c_d_id, c_id):
        result = self.session.execute('select o_id, o_entry_d, o_carrier_id from order_by_customer where'
                                      ' o_w_id = {} and o_d_id = {} and o_c_id = {} limit 1'
                                      .format(c_w_id, c_d_id, c_id))

        return result[0]

    # Get info of each item in the latest order
    def get_order_line(self, c_w_id, c_d_id, o_id):
        result = self.session.execute('select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d'
                                      ' from order_line where ol_w_id ={} and ol_d_id = {} and ol_o_id = {}'
                                      .format(c_w_id, c_d_id, o_id))
        return result


