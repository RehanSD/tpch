from datetime import datetime
from ponder_queries import utils

Q_NUM = 3


def q():
    from pandas import Timestamp
    var1 = var2 = Timestamp(datetime(1995, 3, 15))
    var3 = "BUILDING"

    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds()
    line_item_ds()
    orders_ds()

    def query():
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        customer = customer_ds()
        lineitem = line_item_ds()
        orders = orders_ds()

        lineitem_filtered = lineitem.loc[
            :, ["L_ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]
        ]
        orders_filtered = orders.loc[
            :, ["O_ORDERKEY", "O_CUSTKEY", "O_ORDERDATE", "O_SHIPPRIORITY"]
        ]
        customer_filtered = customer.loc[:, ["C_MKTSEGMENT", "C_CUSTKEY"]]
        lsel = lineitem_filtered.L_SHIPDATE > var1
        osel = orders_filtered.O_ORDERDATE < var2
        csel = customer_filtered.C_MKTSEGMENT == var3
        flineitem = lineitem_filtered[lsel]
        forders = orders_filtered[osel]
        fcustomer = customer_filtered[csel]
        jn1 = fcustomer.merge(forders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        jn2 = jn1.merge(flineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        jn2["TMP"] = jn2.L_EXTENDEDPRICE * (1 - jn2.L_DISCOUNT)
        total = jn2.groupby(
                ["L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"], as_index=False, sort=False
            )["TMP"].sum().sort_values(by=["TMP"], ascending=False)
        res = total.loc[:, ["L_ORDERKEY", "TMP", "O_ORDERDATE", "O_SHIPPRIORITY"]]
        res._query_compiler._dataframe._query_tree.execute()
        return res

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
