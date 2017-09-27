class Transaction:
    def __init__(self, session):
        self.session = session

    # params passed as a dictionary
    def execute(self, params):
        pass

    def get_next_order_id(self, w_id, d_id):
        result = self.session\
            .execute('select d_next_o_id from district where d_w_id = {} and d_id = {}'.format(w_id, d_id))
        return int(result[0].d_next_o_id)