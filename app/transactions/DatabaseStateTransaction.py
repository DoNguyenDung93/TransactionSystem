from Transaction import Transaction

class DatabaseStateTransaction(Transaction):
    def execute(self, params):
        join_cast_str = lambda lst: ' '.join(map(str, list(lst)))

        print '--------------------------------------------------------------------------------------------------'
        print 'Final Database State'
        print 'sum(w_ytd) from warehouse', join_cast_str(self.session.execute('select sum(w_ytd) from warehouse')[0])
        print 'sum(d_ytd), sum(d_next_o_id) from district', \
            join_cast_str(self.session.execute('select sum(d_ytd) from district')[0]), \
            join_cast_str(self.session.execute('select sum(d_next_o_id) from district_next_order_id')[0])
        print 'sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) from customer', \
            join_cast_str(self.session.execute('select sum(c_balance), sum(c_ytd_payment), '
                                 ' sum(c_payment_cnt), sum(c_delivery_cnt) from customer')[0])
        print 'max(o_id), sum(o_ol_cnt) from order_', \
            join_cast_str(self.session.execute('select max(o_id), sum(o_ol_cnt) from order_')[0])
        print 'sum(ol_amount), sum(ol_quantity) from order_line', \
            join_cast_str(self.get_orderline_stat())
        print 'sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) from stock', \
            join_cast_str(self.get_stock_stat())
        print '--------------------------------------------------------------------------------------------------'

    def get_orderline_stat(self):
        ol_amount = 0
        ol_quantity = 0
        for w_id in range(1, 17):
            for d_id in range(1, 11):
                query = 'select sum(ol_amount), sum(ol_quantity) from order_line' \
                        ' where ol_w_id = {} and ol_d_id = {}'.format(w_id, d_id)
                result = list(self.session.execute(query))[0]
                ol_amount += int(result[0])
                ol_quantity += int(result[1])
        return ol_amount, ol_quantity


    def get_stock_stat(self):
        s_quantity = 0
        s_ytd = 0
        s_order_cnt = 0
        s_remote_cnt = 0
        for w_id in range(1, 17):
            query = 'select sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) from stock' \
                    ' where s_w_id = {}'.format(w_id)
            result = list(self.session.execute(query))[0]
            s_quantity += int(result[0])
            s_ytd += int(result[1])
            s_order_cnt += int(result[2])
            s_remote_cnt += int(result[3])
        return s_quantity, s_ytd, s_order_cnt, s_remote_cnt
