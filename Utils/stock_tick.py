class StockTick:
    def __init__(
            self, code, name, open, last_close, price, high, low, accum_volume, accum_amount,
            bid_1_price, bid_1_vol, bid_2_price, bid_2_vol, bid_3_price,
            bid_3_vol, bid_4_price, bid_4_vol, bid_5_price, bid_5_vol,
            ask_1_price, ask_1_vol, ask_2_price, ask_2_vol, ask_3_price,
            ask_3_vol, ask_4_price, ask_4_vol, ask_5_price, ask_5_vol, date, time
    ):
        self.code = code
        self.name = name
        self.price = price
        self.accum_volume = accum_volume
        self.data = [code, name, open, last_close, price, high, low, accum_volume, accum_amount,
                     bid_1_price, bid_1_vol, bid_2_price, bid_2_vol, bid_3_price,
                     bid_3_vol, bid_4_price, bid_4_vol, bid_5_price, bid_5_vol,
                     ask_1_price, ask_1_vol, ask_2_price, ask_2_vol, ask_3_price,
                     ask_3_vol, ask_4_price, ask_4_vol, ask_5_price, ask_5_vol, date, time]

    def to_str(self):
        return ','.join(self.data)

    def process_for_sql(self):
        self.data[0] = "\'" + self.data[0] + "\'"
        self.data[1] = "\'" + self.data[1] + "\'"
        self.data[-1] = "\'" + self.data[-1] + "\'"
        self.data[-2] = "\'" + self.data[-2] + "\'"

