from Transaction import Transaction

class PopularItemTransaction(Transaction):

    def execute(self, params):
        w_id = int(params['w_id'])
        d_id = int(params['d_id'])
        num_last_orders = int(params['l'])

        last_l_orders = self.get_last_l_orders(w_id, d_id, num_last_orders)
        orderlines_for_orders = self.get_orderlines_for_orders(w_id, d_id, last_l_orders)

        if not orderlines_for_orders:
            print 'Cannot get any order lines'
            print
            return

        popular_items_with_quantity = self.get_popular_items_with_quantity(orderlines_for_orders)
        item_ids_names = self.get_items_names(orderlines_for_orders)

        print 'District Identifier ({}, {})'.format(w_id, d_id)
        print 'Number of last orders to be examined {}'.format(num_last_orders)
        self.print_order_info(last_l_orders, popular_items_with_quantity, item_ids_names)
        self.print_popular_items_info(popular_items_with_quantity, item_ids_names)

    """list((any, any, any, any, any, any)): list of last orders, where each order has o_id, o_entry, c_first,
        c_middle, c_last
     Get the last num_last_orders orders belonging to a (warehouse_id, district_id) 
    """
    def get_last_l_orders(self, w_id, d_id, num_last_orders):
        results = self.session.execute('select o_id, o_entry_d, c_first, c_middle, c_last from order_'
                                  ' where o_w_id = {} and o_d_id = {} order by o_id desc limit {}'
                                  .format(w_id, d_id, num_last_orders))
        return list(results)

    """list(list((any, any, any)): list of order lines for each order. Each orderline has ol_quantity, ol_i_id, ol_i_id, 
        i_name
     Return the list of order lines for each order in a list of order a particular warehouse id
    """
    def get_orderlines_for_orders(self, w_id, d_id, orders):
        prepared_query = self.session.prepare('SELECT ol_quantity, ol_i_id, ol_i_id, i_name FROM order_line'
                                              ' WHERE ol_w_id = {} AND ol_d_id = {}'
                                              ' AND ol_o_id = ?'.format(w_id, d_id))

        orderlines_for_orders = []

        for order in orders:
            order_id = int(order.o_id)
            bound_query = prepared_query.bind([order_id])
            results = self.session.execute(bound_query)
            orderlines_for_orders.append(list(results))

        return orderlines_for_orders

    """list((set(int), int)): List of (set of pupular item, quantity) tuple
     Get the popular items and their quantity for each order, using the list of order lines for each order
    """
    def get_popular_items_with_quantity(self, orderlines_list):
        def get_popular_items_for_orderlines(orderlines):
            max_quantity = max(map(lambda ol: ol.ol_quantity, orderlines))
            popular_items = map(lambda ol: int(ol.ol_i_id), filter(lambda ol: ol.ol_quantity == max_quantity, orderlines))
            return (set(popular_items), max_quantity)
        return map(get_popular_items_for_orderlines, orderlines_list)

    """dict(int, string): mapping for item id to name
     Get the mapping of item id to name using the list of order lines for each order
    """
    def get_items_names(self, orderlines_for_orders):
        item_id_name = {}

        for orderlines in orderlines_for_orders:
            for ol in orderlines:
                item_id_name[int(ol.ol_i_id)] = ol.i_name

        return item_id_name

    """
     Print order info
    """
    def print_order_info(self, orders, popular_items_with_quantity, item_ids_name):
        print

        for (order, (popular_item_ids, quantity)) in zip(orders, popular_items_with_quantity):
            print 'Order Number {} at {}'.format(order.o_id, order.o_entry_d)

            print 'By Customer {} {} {}'.format(order.c_first, order.c_middle, order.c_last)

            for item_id in popular_item_ids:
                print 'Item {} with quantity {}'.format(item_ids_name[item_id], quantity)

            print

    """
     Print popular item info
    """
    def print_popular_items_info(self, popular_items_with_quantity, item_ids_name):
        print
        num_orders = float(len(popular_items_with_quantity))
        item_ids_count = {}
        for (item_ids, _) in popular_items_with_quantity:
            for item_id in item_ids:
                if not item_ids_count.get(item_id):
                    item_ids_count[item_id] = 0.
                item_ids_count[item_id] += 1.

        for (item_id, count) in item_ids_count.iteritems():
            print 'Item {}: {}%'.format(item_ids_name[item_id], count / num_orders * 100)

