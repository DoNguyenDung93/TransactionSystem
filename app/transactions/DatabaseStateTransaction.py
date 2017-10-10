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
            join_cast_str(self.session.execute('select sum(ol_amount), sum(ol_quantity) from order_line')[0])
        print 'sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) from stock', \
            join_cast_str(self.session.execute('select sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) from stock')[0])
        print '--------------------------------------------------------------------------------------------------'