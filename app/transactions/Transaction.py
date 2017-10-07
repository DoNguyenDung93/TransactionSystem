class Transaction:
    def __init__(self, session):
        self.session = session

    # params passed as a dictionary
    def execute(self, params):
        pass

    """int: next order id
     Get next order id for a particular (warehouse_id, district_id) key
    """
    def get_next_order_id(self, w_id, d_id):
        result = self.session\
            .execute('SELECT d_next_o_id FROM district_next_order_id WHERE d_w_id = {} AND d_id = {}'.format(w_id, d_id))
        return int(result[0].d_next_o_id)

    """
    Get customer info with warehouse, district id
    """
    def get_customer_info(self, c_w_id, c_d_id, c_id):
        result = self.session.execute('select c_first, c_middle, c_last, c_balance from customer where'
                                      ' c_w_id = {}, c_d_id = {}, c_id = {}'.format(c_w_id, c_d_id, c_id))
        return result[0]
