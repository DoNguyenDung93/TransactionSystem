from Transaction import Transaction

class NewOrderTransaction(Transaction):

	def execute(self, params):
		w_id = params['w_id']
		d_id = params['d_id']
		c_id = params['c_id']
		num_items = params['num_items']