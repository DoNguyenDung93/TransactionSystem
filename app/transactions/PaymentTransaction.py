from Transaction import Transaction

class PaymentTransaction(Transaction):

	def execute(self, params):
		c_w_id = params['c_w_id']
		c_d_id = params['c_d_id']
		c_id = params['c_id']
		payment = params['payment']

		self.update_warehouse(c_w_id, payment)
		self.update_district(c_w_id, c_d_id, payment)
		self.update_customer(c_w_id, c_d_id, c_id, payment)
		self.print_output(c_w_id, c_d_id, c_id)

	def update_warehouse(self, c_w_id, payment):
		self.session.execute('update warehouse set w_ytd = w_ytd + {} where w_id = {}'.format(payment, c_w_id))

	def update_district(self, c_w_id, c_d_id, payment):
		self.session.execute('update district set d_ytd = d_ytd + {} where d_w_id = {} and d_id = {}'.format(payment, c_w_id, c_d_id))

	def update_customer(self, c_w_id, c_d_id, c_id, payment):
		self.session.execute('update customer set c_balance = c_balance - {}, c_ytd_payment = c_ytd_payment + {}, c_payment_cnt = c_payment_cnt + 1 where c_id = {} and c_w_id = {} and c_d_id = {}'.format(payment, payment, c_id, c_w_id, c_d_id))

	def print_output(self, c_w_id, c_d_id, c_id):
		customer_info = self.get_customer_info(c_w_id, c_d_id, c_id)
		print "Customer: {} {} {}".format(customer_info.c_first, customer_info.c_middle, customer_info.c_last)
