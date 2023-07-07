import datetime
from pandas import Timestamp
from pandas_queries import utils

Q_NUM = 5


def q():
    date1 = Timestamp(datetime.datetime.strptime("1994-01-01", "%Y-%m-%d"))
    date2 = Timestamp(datetime.datetime.strptime("1995-01-01", "%Y-%m-%d"))

    region_ds = utils.get_region_ds
    nation_ds = utils.get_nation_ds
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    region_ds()
    nation_ds()
    customer_ds()
    line_item_ds()
    orders_ds()
    supplier_ds()

    def query():
        nonlocal region_ds
        nonlocal nation_ds
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal supplier_ds

        region = region_ds()
        nation = nation_ds()
        customer = customer_ds()
        lineitem = line_item_ds()
        orders = orders_ds()
        supplier = supplier_ds()

        rsel = region.R_NAME == "ASIA"
        osel = (orders.O_ORDERDATE >= date1) & (orders.O_ORDERDATE < date2)
        forders = orders[osel]
        fregion = region[rsel]
        jn1 = fregion.merge(nation, left_on="R_REGIONKEY", right_on="N_REGIONKEY")
        jn2 = jn1.merge(customer, left_on="N_NATIONKEY", right_on="C_NATIONKEY")
        jn3 = jn2.merge(forders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        jn4 = jn3.merge(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        jn5 = supplier.merge(
            jn4, left_on=["S_SUPPKEY", "S_NATIONKEY"], right_on=["L_SUPPKEY", "N_NATIONKEY"]
        )
        jn5["TMP"] = jn5.L_EXTENDEDPRICE * (1.0 - jn5.L_DISCOUNT)
        gb = jn5.groupby("N_NAME", as_index=False, sort=False)["TMP"].sum()
        total = gb.sort_values("TMP", ascending=False)
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
