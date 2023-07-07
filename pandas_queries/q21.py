from pandas_queries import utils

Q_NUM = 21


def q():
    nation_ds = utils.get_nation_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    nation_ds()
    line_item_ds()
    orders_ds()
    supplier_ds()

    def query():
        nonlocal nation_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal supplier_ds

        supplier = supplier_ds()
        lineitem = line_item_ds()
        orders = orders_ds()
        nation = nation_ds()

        lineitem_filtered = lineitem.loc[
            :, ["L_ORDERKEY", "L_SUPPKEY", "L_RECEIPTDATE", "L_COMMITDATE"]
        ]

        # Keep all rows that have another row in linetiem with the same orderkey and different suppkey
        lineitem_orderkeys = (
            lineitem_filtered.loc[:, ["L_ORDERKEY", "L_SUPPKEY"]]
            .groupby("L_ORDERKEY", as_index=False, sort=False)["L_SUPPKEY"]
            .nunique()
        )
        lineitem_orderkeys.columns = ["L_ORDERKEY", "nunique_col"]
        lineitem_orderkeys = lineitem_orderkeys[lineitem_orderkeys["nunique_col"] > 1]
        lineitem_orderkeys = lineitem_orderkeys.loc[:, ["L_ORDERKEY"]]

        # Keep all rows that have l_receiptdate > l_commitdate
        lineitem_filtered = lineitem_filtered[
            lineitem_filtered["L_RECEIPTDATE"] > lineitem_filtered["L_COMMITDATE"]
        ]
        lineitem_filtered = lineitem_filtered.loc[:, ["L_ORDERKEY", "L_SUPPKEY"]]

        # Merge Filter + Exists
        lineitem_filtered = lineitem_filtered.merge(
            lineitem_orderkeys, on="L_ORDERKEY", how="inner"
        )

        # Not Exists: Check the exists condition isn't still satisfied on the output.
        lineitem_orderkeys = lineitem_filtered.groupby(
            "L_ORDERKEY", as_index=False, sort=False
        )["L_SUPPKEY"].nunique()
        lineitem_orderkeys.columns = ["L_ORDERKEY", "nunique_col"]
        lineitem_orderkeys = lineitem_orderkeys[lineitem_orderkeys["nunique_col"] == 1]
        lineitem_orderkeys = lineitem_orderkeys.loc[:, ["L_ORDERKEY"]]

        # Merge Filter + Not Exists
        lineitem_filtered = lineitem_filtered.merge(
            lineitem_orderkeys, on="L_ORDERKEY", how="inner"
        )

        orders_filtered = orders.loc[:, ["O_ORDERSTATUS", "O_ORDERKEY"]]
        orders_filtered = orders_filtered[orders_filtered["O_ORDERSTATUS"] == "F"]
        orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY"]]
        total = lineitem_filtered.merge(
            orders_filtered, left_on="L_ORDERKEY", right_on="O_ORDERKEY", how="inner"
        )
        total = total.loc[:, ["L_SUPPKEY"]]

        supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY", "S_NAME"]]
        total = total.merge(
            supplier_filtered, left_on="L_SUPPKEY", right_on="S_SUPPKEY", how="inner"
        )
        total = total.loc[:, ["S_NATIONKEY", "S_NAME"]]
        nation_filtered = nation.loc[:, ["N_NAME", "N_NATIONKEY"]]
        nation_filtered = nation_filtered[nation_filtered["N_NAME"] == "SAUDI ARABIA"]
        total = total.merge(
            nation_filtered, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner"
        )
        total = total.loc[:, ["S_NAME"]]
        total = total.groupby("S_NAME", as_index=False, sort=False).size()
        total.columns = ["S_NAME", "NUMWAIT"]
        total = total.sort_values(by=["NUMWAIT", "S_NAME"], ascending=[False, True])
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
