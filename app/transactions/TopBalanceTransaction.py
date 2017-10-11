from Transaction import Transaction

class TopBalanceTransaction(Transaction):
    def __init__(self, session):
        self.session = session

    """ Get users with the top 10 balance from a warehouse district
     """

    def get_top_10_balance_users_by_w_id_and_d_id(self, w_id, d_id):
        prepared_query = self.session.prepare('select c_w_id, c_d_id, c_first, c_middle, c_last, c_balance, d_name, w_name '
                                              'from cs4224.customer_balance where c_w_id = ? and c_d_id = ? limit 10')
        bound_query = prepared_query.bind([w_id, d_id])
        results = self.session.execute(bound_query)
        users = list(results)
        # users.sort(self.compare_user_balance, reverse=True)
        return users

    """ Get users with the top 10 balance from a warehouse
    """
    def get_top_10_balance_users_by_w_id(self, w_id):
        results = []
        for d_id in range(1, 11):
            district_top_balance = self.get_top_10_balance_users_by_w_id_and_d_id(w_id, d_id)
            # Combine results but only keep 10 top results
            results += district_top_balance
            results.sort(key=lambda v: float(v.c_balance), reverse=True)
            results = results[:10]
        return results

    """ Get users with the top 10 balance in all warehouse
    """
    def get_top_10_balance_users(self):

        result = []
        for warehouse_id in range(1, 17):
            warehouse_top_balance = self.get_top_10_balance_users_by_w_id(warehouse_id)
            # Combine results but only keep 10 top results
            result += warehouse_top_balance
            result.sort(key=lambda v: float(v.c_balance), reverse=True)
            result = result[:10]
        return result

    def execute(self, params):
        # Get top 10 users with most balance
        users = self.get_top_10_balance_users()

        print 'Top 10 customers'
        for user in users:
            print 'Name:', user.c_first, user.c_middle, user.c_last
            print 'Balance:', user.c_balance
            print 'Warehouse:', user.w_name
            print 'District', user.d_name
            print
