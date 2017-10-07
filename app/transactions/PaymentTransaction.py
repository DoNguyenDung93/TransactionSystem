from Transaction import Transaction
from decimal import *

class PaymentTransaction(Transaction):

	def execute(self, params):
		# inputs
		c_w_id = int(params['c_w_id'])
		c_d_id = int(params['c_d_id'])
		c_id = int(params['c_id'])
		payment = float(params['payment'])

		# processing steps
		warehouse_print = self.update_warehouse(c_w_id, payment)
		district_print = self.update_district(c_w_id, c_d_id, payment)
		customer_print, new_c_balance = self.update_customer(c_w_id, c_d_id, c_id, payment)
		self.print_output(c_id, c_w_id, c_d_id, warehouse_print, district_print, customer_print, new_c_balance, payment)

	def update_warehouse(self, warehouse_id, payment):
		"""Increment w_ytd by payment, return warehouse print outputs"""
		prepared_query = self.session.prepare('SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd FROM warehouse WHERE w_id = ?')
		bound_query = prepared_query.bind([warehouse_id])
		rows = list(self.session.execute(bound_query))
		if not rows:
			print "Cannot find any warehouse with w_id {}".format(warehouse_id)
			return
		else:
			row = rows[0]
			new_w_ytd = row.w_ytd + Decimal(payment)
			prepared_query = self.session.prepare('UPDATE warehouse SET w_ytd = ? WHERE w_id = ?')
			bound_query = prepared_query.bind([new_w_ytd, warehouse_id])
			self.session.execute(bound_query)
			return row

	def update_district(self, warehouse_id, district_id, payment):
		"""Increment d_ytd by payment, return district print outputs"""
		prepared_query = self.session.prepare('SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd FROM district WHERE d_w_id = ? AND d_id = ?')
		bound_query = prepared_query.bind([warehouse_id, district_id])
		rows = list(self.session.execute(bound_query))
		if not rows:
			print "Cannot find any district with w_id d_id {} {}".format(warehouse_id, district_id)
			return
		else:
			row = rows[0]
			new_d_ytd = row.d_ytd + Decimal(payment)
			prepared_query = self.session.prepare('UPDATE district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?')
			bound_query = prepared_query.bind([new_d_ytd, warehouse_id, district_id])
			self.session.execute(bound_query)
			return row

	def update_customer(self, warehouse_id, district_id, customer_id, payment):
		"""Decrease c_balance by payment, increase c_ytd_payment by payment, increment c_payment_cnt, return customer print outputs"""
		prepared_query = self.session.prepare('SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_discount, c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt FROM customer WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?')
		bound_query = prepared_query.bind([customer_id, warehouse_id, district_id])
		rows = list(self.session.execute(bound_query))
		if not rows:
			print "Cannot find any customer with c_id w_id d_id {} {} {}".format(customer_id, warehouse_id, district_id)
			return
		else:
			row = rows[0]
			new_c_balance = row.c_balance - Decimal(payment)
			new_c_ytd_payment = row.c_ytd_payment + float(payment)
			new_c_payment_cnt = row.c_payment_cnt + 1
			prepared_query = self.session.prepare('UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?')
			bound_query = prepared_query.bind([new_c_balance, new_c_ytd_payment, new_c_payment_cnt, customer_id, warehouse_id, district_id])
			self.session.execute(bound_query)
			return row, new_c_balance

	def print_output(self, customer_id, warehouse_id, district_id, warehouse, district, customer, c_balance, payment):
		"""Print required outputs"""
		print
		print "Customer w_d_c_id:	{} {} {}".format(warehouse_id, district_id, customer_id)
		print "Customer name:		{} {} {}".format(customer.c_first, customer.c_middle, customer.c_last)
		print "Address:		{} {} {} {} {}".format(customer.c_street_1, customer.c_street_2, customer.c_city, customer.c_state, customer.c_zip)
		print "Phone:			{}".format(customer.c_phone)
		print "Since:			{}".format(customer.c_since)
		print "Credit:			{} with limit {}".format(customer.c_credit, customer.c_credit_lim)
		print "Discount:		{}".format(customer.c_discount)
		print "Balance:		{}".format(c_balance)
		print "Warehouse:		{} {} {} {} {}".format(warehouse.w_street_1, warehouse.w_street_2, warehouse.w_city, warehouse.w_state, warehouse.w_zip)
		print "District:		{} {} {} {} {}".format(district.d_street_1, district.d_street_2, district.d_city, district.d_state, district.d_zip)
		print "Payment:		{}".format(payment)