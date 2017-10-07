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
		customer_print, c_balance = self.update_customer(c_w_id, c_d_id, c_id, payment)
		self.print_output(warehouse_print, district_print, customer_print, c_balance, payment)

	def update_warehouse(self, c_w_id, payment):
		row = self.session.execute('SELECT * FROM warehouse WHERE w_id = {}'.format(c_w_id))
		row = row[0]
		new_w_ytd = row.w_ytd + Decimal(payment)
		self.session.execute('UPDATE warehouse SET w_ytd = {} WHERE w_id = {}'.format(new_w_ytd, c_w_id))
		return row

	def update_district(self, c_w_id, c_d_id, payment):
		row = self.session.execute('SELECT * FROM district WHERE d_w_id = {} AND d_id = {}'.format(c_w_id, c_d_id))
		row = row[0]
		new_d_ytd = row.d_ytd + Decimal(payment)
		self.session.execute('UPDATE district SET d_ytd = {} WHERE d_w_id = {} AND d_id = {}'.format(new_d_ytd, c_w_id, c_d_id))
		return row

	def update_customer(self, c_w_id, c_d_id, c_id, payment):
		row = self.session.execute('SELECT * FROM customer WHERE c_id = {} AND c_w_id = {} AND c_d_id = {}'.format(c_id, c_w_id, c_d_id))
		row = row[0]
		new_c_balance = row.c_balance - Decimal(payment)
		new_c_ytd_payment = row.c_ytd_payment + float(payment)
		new_c_payment_cnt = row.c_payment_cnt + 1
		self.session.execute('UPDATE customer SET c_balance = {}, c_ytd_payment = {}, c_payment_cnt = {} WHERE c_id = {} AND c_w_id = {} AND c_d_id = {}'.format(new_c_balance, new_c_ytd_payment, new_c_payment_cnt, c_id, c_w_id, c_d_id))
		return row, new_c_balance

	def print_output(self, warehouse, district, customer, c_balance, payment):
		print
		print "Customer:		{} {} {}".format(customer.c_first, customer.c_middle, customer.c_last)
		print "Address:		{} {} {} {} {}".format(customer.c_street_1, customer.c_street_2, customer.c_city, customer.c_state, customer.c_zip)
		print "Phone:			{}".format(customer.c_phone)
		print "Since:			{}".format(customer.c_since)
		print "Credit:			{} with limit {}".format(customer.c_credit, customer.c_credit_lim)
		print "Discount:		{}".format(customer.c_discount)
		print "Balance:		{}".format(c_balance)
		print "Warehouse:		{} {} {} {} {}".format(warehouse.w_street_1, warehouse.w_street_2, warehouse.w_city, warehouse.w_state, warehouse.w_zip)
		print "District:		{} {} {} {} {}".format(district.d_street_1, district.d_street_2, district.d_city, district.d_state, district.d_zip)
		print "Payment:		{}".format(payment)