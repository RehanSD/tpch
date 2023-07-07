from datetime import datetime
from pandas import Timestamp

from pandas_queries import utils

Q_NUM = 4


def q():
    date1 = Timestamp(datetime(1993, 10, 1))
    date2 = Timestamp(datetime(1993, 7, 1))

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

        lsel = lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE
        osel = (orders.O_ORDERDATE < date1) & (orders.O_ORDERDATE >= date2)
        flineitem = lineitem[lsel]
        forders = orders[osel]
        jn = forders[forders["O_ORDERKEY"].isin(flineitem["L_ORDERKEY"])]
        total = (
            jn.groupby("O_ORDERPRIORITY", as_index=False)["O_ORDERKEY"].count().sort_values(["O_ORDERPRIORITY"]).rename(columns={"O_ORDERKEY": "ORDER_COUNT"})
        )
        total._query_compiler._dataframe._query_tree.execute()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
