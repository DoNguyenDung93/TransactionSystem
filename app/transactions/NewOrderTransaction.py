from Transaction import Transaction
from datetime import datetime
from decimal import *

class NewOrderTransaction(Transaction):

	def execute(self, params):
		# inputs
		w_id = int(params['w_id'])
		d_id = int(params['d_id'])
		c_id = int(params['c_id'])
		num_items = int(params['num_items'])
		if (num_items > 20):
			print "Number of items is greater than 20. Exit"
			return
		orders = map(lambda ol: map(int, ol), params['items'])

		# intermediate data
		d_tax = self.session.execute('SELECT d_tax FROM district WHERE d_id = {} AND d_w_id = {}'.format(d_id, w_id))
		d_tax = d_tax[0].d_tax
		w_tax = self.session.execute('SELECT w_tax FROM warehouse WHERE w_id = {}'.format(w_id))
		w_tax = w_tax[0].w_tax
		customer = self.session.execute('SELECT c_first, c_middle, c_last, c_credit, c_credit_lim, c_discount FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}'.format(w_id, d_id, c_id))
		customer = customer[0]
		c_discount = customer.c_discount

		# processing steps
		next_o_id = self.get_next_order_id(w_id, d_id)
		self.update_d_next_o_id(w_id, d_id)
		entry_date = self.create_new_order(w_id, d_id, c_id, next_o_id, num_items, orders)
		print_item_results, total_amount = self.update_stock_and_create_order_line(w_id, d_id, c_id, next_o_id, orders, d_tax, w_tax, c_discount)
		self.print_output(customer, w_tax, d_tax, next_o_id, entry_date, num_items, total_amount)
		self.print_items(print_item_results)

	def update_d_next_o_id(self, w_id, d_id):
		row = self.session.execute('SELECT d_next_o_id FROM district WHERE d_id = {} AND d_w_id = {}'.format(d_id, w_id))
		new_d_next_o_id = row[0].d_next_o_id + 1
		self.session.execute('UPDATE district SET d_next_o_id = {} WHERE d_id = {} AND d_w_id = {}'.format(new_d_next_o_id, d_id, w_id))

	def get_all_local(self, w_id, orders):
		for (_, supply_warehouse_id, _) in orders:
			if w_id != supply_warehouse_id:
				return  0
		return 1

	def create_new_order(self, w_id, d_id, c_id, o_id, num_items, orders):
		all_local = self.get_all_local(w_id, orders)
		time = datetime.now()
		prepared_query = self.session.prepare('INSERT INTO order_ (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
		bound_query = prepared_query.bind([int(w_id), int(d_id), int(o_id), int(c_id), 0, int(num_items), all_local, time])
		self.session.execute(bound_query)
		return time

	def update_stock_and_create_order_line(self, w_id, d_id, c_id, n, orders, d_tax, w_tax, c_discount):
		total_amount = 0
		result = []
		for index, (item_number, supplier_warehouse, quantity) in enumerate(orders):
			item_result = []
			item_result.append(item_number)
			row = self.session.execute('SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock WHERE s_w_id = {} AND s_i_id = {}'.format(int(supplier_warehouse), int(item_number)))
			row = row[0]
			adjusted_qty = int(row.s_quantity) - quantity
			if adjusted_qty < 10:
				adjusted_qty += 100
			new_s_ytd = row.s_ytd + quantity
			new_s_order_cnt = row.s_order_cnt + 1
			if supplier_warehouse != w_id:
				counter = 1
			else:
				counter = 0
			new_s_remote_cnt = row.s_remote_cnt + counter
			prepared_query = self.session.prepare('UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? WHERE s_w_id = ? AND s_i_id = ?')
			bound_query = prepared_query.bind([Decimal(adjusted_qty), Decimal(new_s_ytd), int(new_s_order_cnt), int(new_s_remote_cnt), int(supplier_warehouse), int(item_number)])
			self.session.execute(bound_query)
			item = self.session.execute('SELECT i_price, i_name FROM item WHERE i_id = {}'.format(item_number))
			item = item[0]
			item_result.append(item.i_name)
			item_result.append(supplier_warehouse)
			item_result.append(quantity)
			item_amount = quantity * item.i_price
			item_result.append(item_amount)
			item_result.append(adjusted_qty)
			total_amount = total_amount + item_amount
			prepared_query = self.session.prepare('INSERT INTO order_line(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
			bound_query = prepared_query.bind([int(w_id), int(d_id), int(n), int(index), int(item_number), None, item_amount, supplier_warehouse, quantity, 'S_DIST'+str(d_id)])
			self.session.execute(bound_query)
			result.append(item_result)

		total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)
		return result, total_amount

	def print_output(self, customer, w_tax, d_tax, o_id, entry_date, num_items, total_amount):
		print
		print "Customer:		{} {} {}".format(customer.c_first, customer.c_middle, customer.c_last)
		print "Credit:			{} with limit {}".format(customer.c_credit, customer.c_credit_lim)
		print "Discount:		{}".format(customer.c_discount)
		print "Warehouse tax:		{}".format(w_tax)
		print "District tax:		{}".format(d_tax)
		print "Order number:		{}".format(o_id)
		print "Entry date:		{}".format(entry_date)
		print "Number of items:	{}".format(num_items)
		print "Total amount:		{}".format(total_amount)

	def print_items(self, items):
		for (item_num, i_name, supplier_warehouse, quantity, ol_amount, s_quantity) in items:
			print
			print "Item number:		{}".format(item_num)
			print "Item name:		{}".format(i_name)
			print "Supplier warehouse:	{}".format(supplier_warehouse)
			print "Quantity:		{}".format(quantity)
			print "OL_amount:		{}".format(ol_amount)
			print "S_quantity:		{}".format(s_quantity)
