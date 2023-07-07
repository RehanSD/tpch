from pandas_queries import utils

Q_NUM = 18


def q():
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

        lineitem = line_item_ds()
        orders = orders_ds()
        customer = customer_ds()

        gb1 = lineitem.groupby("L_ORDERKEY", as_index=False, sort=False)["L_QUANTITY"].sum()
        fgb1 = gb1[gb1.L_QUANTITY > 300]
        jn1 = fgb1.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        jn2 = jn1.merge(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
        gb2 = jn2.groupby(
            ["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"],
            as_index=False,
            sort=False,
        )["L_QUANTITY"].sum()
        total = gb2.sort_values(["O_TOTALPRICE", "O_ORDERDATE"], ascending=[False, True])
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
