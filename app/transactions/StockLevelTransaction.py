from Transaction import Transaction

class StockLevelTransaction(Transaction):
    def execute(self, params):
        w_id = params['w_id']
        d_id = params['d_id']
        threshold = params['t']
        num_last_orders = params['l']

        next_order_id = self.get_next_order_id(w_id, d_id)
        last_l_item_ids = self.get_last_l_item_ids(w_id, d_id, next_order_id, num_last_orders)
        num_items = self.count_items_below_threshold(w_id, last_l_item_ids, threshold)
        print 'Total Num Items', num_items

    def get_last_l_item_ids(self, w_id, d_id, next_order_id, num_last_orders):
        results = self.session.execute('select ol_i_id from order_line'
                                  ' where ol_w_id = {} and ol_d_id = {} and ol_o_id >= {}'
                                  .format(w_id, d_id, next_order_id - num_last_orders))
        return set(map(lambda result: int(result.ol_i_id), results))

    def count_items_below_threshold(self, w_id, item_ids, threshold):
        prepared_query = self.session.prepare('select s_quantity from stock where s_w_id = {} and s_i_id = ?'.format(w_id))

        count = 0

        for item_id in item_ids:
            bound_query = prepared_query.bind([item_id])
            result = self.session.execute(bound_query)
            count += 1 if int(result[0].s_quantity) < threshold else 0

        return count