from pandas_queries import utils
from pandas import Timestamp

Q_NUM = 10


def q():
    date1 = Timestamp("1994-11-01")
    date2 = Timestamp("1995-02-01")
    nation_ds = utils.get_nation_ds
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    nation_ds()
    customer_ds()
    line_item_ds()
    orders_ds()

    def query():
        nonlocal nation_ds
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds

        lineitem = line_item_ds()
        orders = orders_ds()
        nation = nation_ds()
        customer = customer_ds

        osel = (orders.O_ORDERDATE >= date1) & (orders.O_ORDERDATE < date2)
        lsel = lineitem.L_RETURNFLAG == "R"
        forders = orders[osel]
        flineitem = lineitem[lsel]
        jn1 = flineitem.merge(forders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        jn2 = jn1.merge(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
        jn3 = jn2.merge(nation, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
        jn3["TMP"] = jn3.L_EXTENDEDPRICE * (1.0 - jn3.L_DISCOUNT)
        gb = jn3.groupby(
            [
                "C_CUSTKEY",
                "C_NAME",
                "C_ACCTBAL",
                "C_PHONE",
                "N_NAME",
                "C_ADDRESS",
                "C_COMMENT",
            ],
            as_index=False,
            sort=False,
        )["TMP"].sum()
        total = gb.sort_values("TMP", ascending=False)
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
