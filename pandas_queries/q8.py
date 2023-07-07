from datetime import datetime

import pandas as pd

from pandas_queries import utils

Q_NUM = 8


def q():
    nation_ds = utils.get_nation_ds
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    nation_ds()
    customer_ds()
    line_item_ds()
    orders_ds()
    supplier_ds()

    def query():
        nonlocal nation_ds
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal supplier_ds

        part = utils.get_part_ds()
        supplier = supplier_ds()
        lineitem = line_item_ds()
        orders = orders_ds()
        customer = customer_ds()
        nation = nation_ds()
        region = utils.get_region_ds()

        part_filtered = part[(part["P_TYPE"] == "ECONOMY ANODIZED STEEL")]
        part_filtered = part_filtered.loc[:, ["P_PARTKEY"]]
        lineitem_filtered = lineitem.loc[:, ["L_PARTKEY", "L_SUPPKEY", "L_ORDERKEY"]]
        lineitem_filtered["VOLUME"] = lineitem["L_EXTENDEDPRICE"] * (
            1.0 - lineitem["L_DISCOUNT"]
        )
        total = part_filtered.merge(
            lineitem_filtered, left_on="P_PARTKEY", right_on="L_PARTKEY", how="inner"
        )
        total = total.loc[:, ["L_SUPPKEY", "L_ORDERKEY", "VOLUME"]]
        supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
        total = total.merge(
            supplier_filtered, left_on="L_SUPPKEY", right_on="S_SUPPKEY", how="inner"
        )
        total = total.loc[:, ["L_ORDERKEY", "VOLUME", "S_NATIONKEY"]]
        orders_filtered = orders[
            (orders["O_ORDERDATE"] >= pd.Timestamp("1995-01-01"))
            & (orders["O_ORDERDATE"] < pd.Timestamp("1997-01-01"))
        ]
        orders_filtered["O_YEAR"] = orders_filtered["O_ORDERDATE"].dt.year
        orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY", "O_CUSTKEY", "O_YEAR"]]
        total = total.merge(
            orders_filtered, left_on="L_ORDERKEY", right_on="O_ORDERKEY", how="inner"
        )
        total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_CUSTKEY", "O_YEAR"]]
        customer_filtered = customer.loc[:, ["C_CUSTKEY", "C_NATIONKEY"]]
        total = total.merge(
            customer_filtered, left_on="O_CUSTKEY", right_on="C_CUSTKEY", how="inner"
        )
        total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_YEAR", "C_NATIONKEY"]]
        n1_filtered = nation.loc[:, ["N_NATIONKEY", "N_REGIONKEY"]]
        n2_filtered = nation.loc[:, ["N_NATIONKEY", "N_NAME"]].rename(
            columns={"N_NAME": "NATION"}
        )
        total = total.merge(
            n1_filtered, left_on="C_NATIONKEY", right_on="N_NATIONKEY", how="inner"
        )
        total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_YEAR", "N_REGIONKEY"]]
        total = total.merge(
            n2_filtered, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner"
        )
        total = total.loc[:, ["VOLUME", "O_YEAR", "N_REGIONKEY", "NATION"]]
        region_filtered = region[(region["R_NAME"] == "AMERICA")]
        region_filtered = region_filtered.loc[:, ["R_REGIONKEY"]]
        total = total.merge(
            region_filtered, left_on="N_REGIONKEY", right_on="R_REGIONKEY", how="inner"
        )
        total = total.loc[:, ["VOLUME", "O_YEAR", "NATION"]]

        def udf(df):
            demonimator = df["VOLUME"].sum()
            df = df[df["NATION"] == "BRAZIL"]
            numerator = df["VOLUME"].sum()
            return numerator / demonimator

        total = total.groupby("O_YEAR", as_index=False).apply(udf)
        total.columns = ["O_YEAR", "MKT_SHARE"]
        total = total.sort_values(by=["O_YEAR"], ascending=[True])
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
