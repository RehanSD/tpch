from ponder_queries import utils
from pandas import Timestamp

Q_NUM = 12


def q():
    date1 = Timestamp("1994-01-01")
    date2 = Timestamp("1995-01-01")

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()

    def query():
        nonlocal line_item_ds
        nonlocal orders_ds

        lineitem = line_item_ds()
        orders = orders_ds()

        sel = (
            (lineitem.L_RECEIPTDATE < date2)
            & (lineitem.L_COMMITDATE < date2)
            & (lineitem.L_SHIPDATE < date2)
            & (lineitem.L_SHIPDATE < lineitem.L_COMMITDATE)
            & (lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE)
            & (lineitem.L_RECEIPTDATE >= date1)
            & ((lineitem.L_SHIPMODE == "MAIL") | (lineitem.L_SHIPMODE == "SHIP"))
        )
        flineitem = lineitem[sel]
        jn = flineitem.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")

        def g1(x):
            return ((x == "1-URGENT") | (x == "2-HIGH")).sum()

        def g2(x):
            return ((x != "1-URGENT") & (x != "2-HIGH")).sum()

        total = jn.groupby("L_SHIPMODE", as_index=False)["O_ORDERPRIORITY"].agg((g1, g2))
        total = total.reset_index()  # reset index to keep consistency with pandas
        # skip sort when groupby does sort already
        total = total.sort_values("L_SHIPMODE")
        total._query_compiler._dataframe._query_tree.execute()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
