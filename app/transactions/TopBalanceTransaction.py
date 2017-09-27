from Transaction import Transaction

class TopBalanceTransaction(Transaction):
    def __init__(self, session):
        self.session = session

    """ Compares two users by balance
    """
    def compare_user_balance(self, u1, u2):
        if u1["c_balance"] < u2["c_balance"]:
            return -1
        elif u2["c_balance"] == u2["c_balance"]:
            return 0
        else:
            return 1

    """ Get users with the top 10 balance from a warehouse
    """
    def get_top_10_balance_users_by_w_id(self, w_id):
        query = "select c_w_id, c_d_id, c_first, c_middle, c_last, c_balance " \
                "from cs4224.customer where c_w_id = %s" % w_id
        results = self.session.execute(query)
        users = list(map(lambda result: 
            {"c_w_id"   : result.c_w_id,
             "c_d_id"   : result.c_d_id,
             "c_first"  : result.c_first,
             "c_middle" : result.c_middle,
             "c_last"   : result.c_last,
             "c_balance": result.c_balance}, results))
        users.sort(self.compare_user_balance, reverse=True)
        return users[:10]

    """ Get users with the top 10 balance in all warehouse
    """
    def get_top_10_balance_users(self):
        query = "select w_id from cs4224.warehouse" 
        warehouse_ids = self.session.execute(query)

        result = []
        for warehouse_id in warehouse_ids:
            warehouse_top_balance = self.get_top_10_balance_users_by_w_id(warehouse_id)
            # Combine results but only keep 10 top results
            result += warehouse_top_balance
            result.sort(self.compare_user_balance, reverse=True)
            result = result[:10]
        return result

    def execute(self, params):
        # Get top 10 users with most balance
        users = self.get_top_10_balance_users()

        # Create queries for w_name and d_name
        query = "select w_name "         \
                "from cs4224.warehouse " \
                "where w_id = ? allow filtering"
        w_name_query = self.session.prepare(query)
        
        query = "select d_name "        \
                "from cs4224.district " \
                "where d_id = ? allow filtering"
        d_name_query = self.session.prepare(query)

        # Get d_name and w_name for each user
        results = []
        for user in users:
            result = user
            result["w_name"] = self.session.execute(w_name_query.bind([user["c_w_id"]]))[0].w_name
            result["d_name"] = self.session.execute(d_name_query.bind([user["c_d_id"]]))[0].d_name
            results.append(result)
        return results
