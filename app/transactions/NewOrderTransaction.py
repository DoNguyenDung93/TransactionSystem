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
			print "Number of items in this new order is greater than 20. Exit"
			return
		orders = map(lambda ol: map(int, ol), params['items'])

		# intermediate data
		w_tax = self.get_w_tax(w_id)
		customer = self.get_customer_for_output(w_id, d_id, c_id)

		# processing steps (follow sequence in project.pdf)
		next_o_id, d_tax = self.get_d_next_o_id_and_d_tax(w_id, d_id)
		self.update_d_next_o_id(w_id, d_id, next_o_id+1)
		entry_date = self.create_new_order(w_id, d_id, c_id, next_o_id, num_items, orders)
		c_discount = Decimal(customer.c_discount)
		print_item_results, total_amount = self.update_stock_and_create_order_line(w_id, d_id, c_id, next_o_id, orders, d_tax, w_tax, c_discount)
		self.print_output(w_id, d_id, c_id, customer, w_tax, d_tax, next_o_id, entry_date, num_items, total_amount)
		self.print_items(print_item_results)

	def get_w_tax(self, w_id):
		"""Get warehouse tax from the vertical partition table warehouse_tax"""
		prepared_query = self.session.prepare('SELECT w_tax FROM warehouse_tax WHERE w_id = ?')
		bound_query = prepared_query.bind([w_id])
		rows = list(self.session.execute(bound_query))
		if not rows:
			print "Cannot find any warehouse with w_id {}".format(w_id)
			return
		else:
			return Decimal(rows[0].w_tax)

	def get_customer_for_output(self, w_id, d_id, c_id):
		"""Get customer info (c_last, c_credit, c_discount) for printing output"""
		prepared_query = self.session.prepare('SELECT c_last, c_credit, c_discount FROM customer WHERE  c_w_id = ? AND c_d_id = ? AND c_id = ?')
		bound_query = prepared_query.bind([w_id, d_id, c_id])
		rows = list(self.session.execute(bound_query))
		if not rows:
			print "Cannot find any customer with w_id d_id c_id {} {} {}".format(w_id, d_id, c_id)
			return
		else:
			return rows[0]

	def get_d_next_o_id_and_d_tax(self, w_id, d_id):
		"""Get d_next_o_id and d_tax from vertical partition district_next_order_id table"""
		prepared_query = self.session.prepare('SELECT d_next_o_id, d_tax FROM district_next_order_id WHERE d_w_id = ? AND d_id = ?')
		bound_query = prepared_query.bind([w_id, d_id])
		rows = list(self.session.execute(bound_query))
		if not rows:
			print "Cannot find any district with w_id d_id {} {}".format(w_id, d_id)
			return
		else:
			return int(rows[0].d_next_o_id), Decimal(rows[0].d_tax)

	def update_d_next_o_id(self, w_id, d_id, new_d_next_o_id):
		"""Increment d_next_o_id of district_next_order_id table"""
		prepared_query = self.session.prepare('UPDATE district_next_order_id SET d_next_o_id = ? WHERE d_id = ? AND d_w_id = ?')
		bound_query = prepared_query.bind([w_id, d_id, new_d_next_o_id])
		self.session.execute(bound_query)

	def get_all_local(self, w_id, orders):
		for (_, supply_warehouse_id, _) in orders:
			if w_id != supply_warehouse_id:
				return  0
		return 1

	def create_new_order(self, w_id, d_id, c_id, o_id, num_items, orders):
		"""Create a new order (insert row into order_ table)"""
		all_local = self.get_all_local(w_id, orders)
		time = datetime.strptime(datetime.utcnow().isoformat(' '), '%Y-%m-%d %H:%M:%S.%f')
		prepared_query = self.session.prepare('INSERT INTO order_ (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
		bound_query = prepared_query.bind([w_id, d_id, o_id, c_id, -1, num_items, all_local, time])
		self.session.execute(bound_query)
		return time

	def update_stock_and_create_order_line(self, w_id, d_id, c_id, n, orders, d_tax, w_tax, c_discount):
		"""Update stock table, create a new order-line for each new item"""
		total_amount = 0
		result = []
		for index, (item_number, supplier_warehouse, quantity) in enumerate(orders):
			item_result = []
			item_result.append(item_number)
			row = self.session.execute('SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt, i_price, i_name FROM stock WHERE s_w_id = {} AND s_i_id = {}'.format(int(supplier_warehouse), int(item_number)))
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
			prepared_query1 = self.session.prepare('UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? WHERE s_w_id = ? AND s_i_id = ?')
			bound_query1 = prepared_query1.bind([Decimal(adjusted_qty), Decimal(new_s_ytd), int(new_s_order_cnt), int(new_s_remote_cnt), int(supplier_warehouse), int(item_number)])
			self.session.execute(bound_query1)
			item_result.append(row.i_name)
			item_result.append(supplier_warehouse)
			item_result.append(quantity)
			item_amount = quantity * row.i_price
			item_result.append(item_amount)
			item_result.append(adjusted_qty)
			total_amount = total_amount + item_amount
			prepared_query2 = self.session.prepare('INSERT INTO order_line(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, i_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
			bound_query2 = prepared_query2.bind([int(w_id), int(d_id), int(n), int(index), int(item_number), None, item_amount, supplier_warehouse, quantity, 'S_DIST'+str(d_id), row.i_name])
			self.session.execute(bound_query2)
			result.append(item_result)

		total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)
		return result, total_amount

	def print_output(self, w_id, d_id, c_id, customer, w_tax, d_tax, o_id, entry_date, num_items, total_amount):
		"""Print output for the customer"""
		print
		print "Customer w_d_c_id:	{} {} {}".format(w_id, d_id, c_id)
		print "Customer lastname:	{}".format(customer.c_last)
		print "Credit:			{}".format(customer.c_credit)
		print "Discount:		{}".format(customer.c_discount)
		print "Warehouse tax:		{}".format(w_tax)
		print "District tax:		{}".format(d_tax)
		print "Order number:		{}".format(o_id)
		print "Entry date:		{}".format(entry_date)
		print "Number of items:	{}".format(num_items)
		print "Total amount:		{}".format(total_amount)

	def print_items(self, items):
		"""Print output for each item"""
		for (item_num, i_name, supplier_warehouse, quantity, ol_amount, s_quantity) in items:
			print
			print "Item number:		{}".format(item_num)
			print "Item name:		{}".format(i_name)
			print "Supplier warehouse:	{}".format(supplier_warehouse)
			print "Quantity:		{}".format(quantity)
			print "OL_amount:		{}".format(ol_amount)
			print "S_quantity:		{}".format(s_quantity)
