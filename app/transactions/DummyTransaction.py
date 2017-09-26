from Transaction import Transaction

class DummyTransaction(Transaction):
    def execute(self, params):
        w_id = params['w_id']
        print self.session.execute('select * from warehouse where w_id = {}'.format(w_id))