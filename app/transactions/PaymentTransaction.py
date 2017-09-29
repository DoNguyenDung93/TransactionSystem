from Transaction import Transaction
from decimal import *

class PaymentTransaction(Transaction):

	def execute(self, params):
		# inputs
		c_w_id = params['c_w_id']
		c_d_id = params['c_d_id']
		c_id = params['c_id']
		payment = params['payment']

		# processing steps
		self.update_warehouse(c_w_id, payment)
		self.update_district(c_w_id, c_d_id, payment)
		self.update_customer(c_w_id, c_d_id, c_id, payment)
		self.print_output(c_w_id, c_d_id, c_id, payment)

	def update_warehouse(self, c_w_id, payment):
		row = self.session.execute('select w_ytd from warehouse where w_id = {}'.format(c_w_id))
		new_w_ytd = row[0].w_ytd + Decimal(payment)
		self.session.execute('update warehouse set w_ytd = {} where w_id = {}'.format(new_w_ytd, c_w_id))

	def update_district(self, c_w_id, c_d_id, payment):
		row = self.session.execute('select d_ytd from district where d_w_id = {} and d_id = {}'.format(c_w_id, c_d_id))
		new_d_ytd = row[0].d_ytd + Decimal(payment)
		self.session.execute('update district set d_ytd = {} where d_w_id = {} and d_id = {}'.format(new_d_ytd, c_w_id, c_d_id))

	def update_customer(self, c_w_id, c_d_id, c_id, payment):
		row = self.session.execute('select c_balance, c_ytd_payment, c_payment_cnt from customer where c_id = {} and c_w_id = {} and c_d_id = {}'.format(c_id, c_w_id, c_d_id))
		new_c_balance = row[0].c_balance - Decimal(payment)
		new_c_ytd_payment = row[0].c_ytd_payment + float(payment)
		new_c_payment_cnt = row[0].c_payment_cnt + 1
		self.session.execute('update customer set c_balance = {}, c_ytd_payment = {}, c_payment_cnt = {} where c_id = {} and c_w_id = {} and c_d_id = {}'.format(new_c_balance, new_c_ytd_payment, new_c_payment_cnt, c_id, c_w_id, c_d_id))

	def print_output(self, c_w_id, c_d_id, c_id, payment):
		customer_info = self.session.execute('select * from customer where c_w_id = {} and c_d_id = {} and c_id = {}'.format(c_w_id, c_d_id, c_id))
		customer_info = customer_info[0]
		print "Customer:		{} {} {}".format(customer_info.c_first, customer_info.c_middle, customer_info.c_last)
		print "Address:		{} {} {} {} {}".format(customer_info.c_street_1, customer_info.c_street_2, customer_info.c_city, customer_info.c_state, customer_info.c_zip)
		print "Phone:			{}".format(customer_info.c_phone)
		print "Since:			{}".format(customer_info.c_since)
		print "Credit:			{} with limit {}".format(customer_info.c_credit, customer_info.c_credit_lim)
		print "Discount:		{}".format(customer_info.c_discount)
		print "Balance:		{}".format(customer_info.c_balance)

		warehouse = self.session.execute('select w_street_1, w_street_2, w_city, w_state, w_zip from warehouse where w_id = {}'.format(c_w_id))
		warehouse = warehouse[0]
		print "Warehouse:		{} {} {} {} {}".format(warehouse.w_street_1, warehouse.w_street_2, warehouse.w_city, warehouse.w_state, warehouse.w_zip)

		district = self.session.execute('select d_street_1, d_street_2, d_city, d_state, d_zip from district where d_id = {} and d_w_id = {}'.format(c_d_id, c_w_id))
		district = district[0]
		print "District:		{} {} {} {} {}".format(district.d_street_1, district.d_street_2, district.d_city, district.d_state, district.d_zip)

		print "Payment:		{}".format(payment)