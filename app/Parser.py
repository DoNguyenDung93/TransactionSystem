class Parser():

    # Transaction type string values
    NEW_ORDER = "N"
    PAYMENT = "P"
    DELIVERY = "D"
    ORDER_STATUS = "O"
    STOCK_LEVEL = "S"
    POPULAR_ITEM = "I"
    TOP_BALANCE = "T"
    ORDER_LINE = "L"

    # Transaction raw string line separator
    LINE_SEPARATOR = ","

    def __init__(self):
        pass

    def get_transaction_type(self, line):
        # Split line into tokens
        return line.split(Parser.LINE_SEPARATOR)[0]

    def get_transaction_extra_line_count(self, transaction_type, line):
        if transaction_type == Parser.NEW_ORDER:
            return int(line.split(Parser.LINE_SEPARATOR)[-1])
        else:
            return 0


    def parse(self, line, transaction_type = "", extra_infos = []):
        # Split line into tokens
        tokens = line.split(Parser.LINE_SEPARATOR)

        # Transaction type can be given by user or extracted from tokens
        if not transaction_type:
            transaction_type = tokens[0]

        if transaction_type == Parser.NEW_ORDER:
            # Extract list of order lines
            orders = []
            for info in extra_infos:
                order = info.split(Parser.LINE_SEPARATOR)
                orders.append(order)
            return {"c_id" : tokens[1],
                    "w_id" : tokens[2],
                    "d_id" : tokens[3],
                    "num_items" : tokens[4],
                    "items": orders}

        elif transaction_type == Parser.PAYMENT:
            return {"c_w_id" : tokens[1],
                    "c_d_id" : tokens[2],
                    "c_id"   : tokens[3],
                    "payment": tokens[4]}

        elif transaction_type == Parser.DELIVERY:
            return {"w_id"      : tokens[1],
                    "carrier_id": tokens[2]}

        elif transaction_type == Parser.ORDER_STATUS:
            return {"c_w_id": tokens[1],
                    "c_d_id": tokens[2],
                    "c_id"  : tokens[3]}

        elif transaction_type == Parser.STOCK_LEVEL:
            return {"w_id": tokens[1],
                    "d_id": tokens[2],
                    "t"   : tokens[3],
                    "l"   : tokens[4]}

        elif transaction_type == Parser.POPULAR_ITEM:
            return {"w_id": tokens[1],
                    "d_id": tokens[2],
                    "l"   : tokens[3]}

        elif transaction_type == Parser.TOP_BALANCE:
            return {}
