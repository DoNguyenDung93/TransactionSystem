from Transaction import Transaction
from datetime import datetime

# Order Status Transaction (Transaction #4)
# This transaction queries the status of the last order of a customer

class OrderStatusTransaction(Transaction):
    def execute(self, params):
        c_w_id = params['c_w_id']
        c_d_id = params['c_d_id']
        c_id = params['c_id']

        customer_info = self.get_customer_info(c_w_id, c_d_id, c_id)
        customer_name_first = customer_info.c_first
        customer_name_middle = customer_info.c_middle
        customer_name_last = customer_info.c_last
        customer_balance = customer_info.c_balance

        last_order = self.get_last_order(c_w_id, c_d_id, c_id)
        order_number = last_order.o_id
        entry_date = last_order.o_entry_d
        carrier = last_order.o_carrier.id

        item_number = []
        supply_warehouse = []
        quantity = []
        total_price = []
        date_time = []
        order_line = self.get_order_line(c_w_id, c_id, order_number)

        for index in range(len(order_line)):
            item_number[index] = order_line[index].ol_i_id
            supply_warehouse[index] = order_line[index].ol_supply_w_id
            quantity[index] = order_line[index].ol_quantity
            total_price[index] = order_line[index].ol_amount
            date_time[index] = order_line[index].ol_delivery_d

        print 'Customer Info: ', customer_name_first, customer_name_middle, customer_name_last,\
            ' has balance of ', customer_balance
        print 'Last order info: ', order_number, entry_date, carrier

    # Get the customer info using C_ID
    def get_customer_info(self, c_w_id, c_d_id, c_id):
        result = self.session.execute('select c_first, c_middle, c_last, c_balance from customer where'
                                      ' c_w_id = {}, c_d_id = {}, c_id = {}'.format(c_w_id, c_d_id, c_id))
        return result[0]

    # Get the last order info from the customer
    def get_last_order(self, c_w_id, c_d_id, c_id):
        result = self.session.execute('select o_id, o_entry_d, o_carrier_id from order_ where'
                                      ' o_w_id = {}, o_d_id = {}, o_c_id = {}'.format(c_w_id, c_d_id, c_id))
        latest_time = datetime.strptime(result[0].o_entry_d, '%Y-%m-%d %H-%M-%S')
        latest_order = 0
        for index in range(len(result)):
            time = datetime.strptime(result[index].o_entry_d, '%Y-%m-%d %H-%M-%S')
            if time > latest_time:
                latest_time = time
                latest_order = index

        return result[latest_order]

    # Get info of each item in the latest order
    def get_order_line(self, c_w_id, c_d_id, o_id):
        result = self.session.execute('select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d'
                                      ' from order_line where ol_w_id ={}, ol_d_id = {}, ol_o_id = {}'
                                      .format(c_w_id, c_d_id, o_id))
        return result


