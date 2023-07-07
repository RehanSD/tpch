
import modin.pandas as pd

from pandas_queries import utils

Q_NUM = 7


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

        nation = nation_ds()
        customer = customer_ds()
        lineitem = line_item_ds()
        orders = orders_ds()
        supplier = supplier_ds()

        lineitem_filtered = lineitem[
            (lineitem["L_SHIPDATE"] >= pd.Timestamp("1995-01-01"))
            & (lineitem["L_SHIPDATE"] < pd.Timestamp("1997-01-01"))
        ]
        lineitem_filtered["L_YEAR"] = lineitem_filtered["L_SHIPDATE"].dt.year
        lineitem_filtered["VOLUME"] = lineitem_filtered["L_EXTENDEDPRICE"] * (
            1.0 - lineitem_filtered["L_DISCOUNT"]
        )
        lineitem_filtered = lineitem_filtered.loc[
            :, ["L_ORDERKEY", "L_SUPPKEY", "L_YEAR", "VOLUME"]
        ]
        supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
        orders_filtered = orders.loc[:, ["O_ORDERKEY", "O_CUSTKEY"]]
        customer_filtered = customer.loc[:, ["C_CUSTKEY", "C_NATIONKEY"]]
        n1 = nation[(nation["N_NAME"] == "FRANCE")].loc[:, ["N_NATIONKEY", "N_NAME"]]
        n2 = nation[(nation["N_NAME"] == "GERMANY")].loc[:, ["N_NATIONKEY", "N_NAME"]]

        # ----- do nation 1 -----
        N1_C = customer_filtered.merge(
            n1, left_on="C_NATIONKEY", right_on="N_NATIONKEY", how="inner"
        )
        N1_C = N1_C.drop(columns=["C_NATIONKEY", "N_NATIONKEY"]).rename(
            columns={"N_NAME": "CUST_NATION"}
        )
        N1_C_O = N1_C.merge(
            orders_filtered, left_on="C_CUSTKEY", right_on="O_CUSTKEY", how="inner"
        )
        N1_C_O = N1_C_O.drop(columns=["C_CUSTKEY", "O_CUSTKEY"])

        N2_S = supplier_filtered.merge(
            n2, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner"
        )
        N2_S = N2_S.drop(columns=["S_NATIONKEY", "N_NATIONKEY"]).rename(
            columns={"N_NAME": "SUPP_NATION"}
        )
        N2_S_L = N2_S.merge(
            lineitem_filtered, left_on="S_SUPPKEY", right_on="L_SUPPKEY", how="inner"
        )
        N2_S_L = N2_S_L.drop(columns=["S_SUPPKEY", "L_SUPPKEY"])

        total1 = N1_C_O.merge(
            N2_S_L, left_on="O_ORDERKEY", right_on="L_ORDERKEY", how="inner"
        )
        total1 = total1.drop(columns=["O_ORDERKEY", "L_ORDERKEY"])

        # ----- do nation 2 -----
        N2_C = customer_filtered.merge(
            n2, left_on="C_NATIONKEY", right_on="N_NATIONKEY", how="inner"
        )
        N2_C = N2_C.drop(columns=["C_NATIONKEY", "N_NATIONKEY"]).rename(
            columns={"N_NAME": "CUST_NATION"}
        )
        N2_C_O = N2_C.merge(
            orders_filtered, left_on="C_CUSTKEY", right_on="O_CUSTKEY", how="inner"
        )
        N2_C_O = N2_C_O.drop(columns=["C_CUSTKEY", "O_CUSTKEY"])

        N1_S = supplier_filtered.merge(
            n1, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner"
        )
        N1_S = N1_S.drop(columns=["S_NATIONKEY", "N_NATIONKEY"]).rename(
            columns={"N_NAME": "SUPP_NATION"}
        )
        N1_S_L = N1_S.merge(
            lineitem_filtered, left_on="S_SUPPKEY", right_on="L_SUPPKEY", how="inner"
        )
        N1_S_L = N1_S_L.drop(columns=["S_SUPPKEY", "L_SUPPKEY"])

        total2 = N2_C_O.merge(
            N1_S_L, left_on="O_ORDERKEY", right_on="L_ORDERKEY", how="inner"
        )
        total2 = total2.drop(columns=["O_ORDERKEY", "L_ORDERKEY"])

        # concat results
        total = pd.concat([total1, total2])

        total = total.groupby(["SUPP_NATION", "CUST_NATION", "L_YEAR"], as_index=False).agg(
            REVENUE=pd.NamedAgg(column="VOLUME", aggfunc="sum")
        )
        # skip sort when Mars groupby does sort already
        total = total.sort_values(
            by=["SUPP_NATION", "CUST_NATION", "L_YEAR"], ascending=[True, True, True]
        )
        total._query_compiler._dataframe._query_tree.execute()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
