from ponder_queries import utils

Q_NUM = 1


def q():
    from pandas import Timestamp
    date = Timestamp("1998-09-02")

    lineitem = utils.get_line_item_ds
    # first call one time to cache in case we don't include the IO times
    lineitem()

    def query():
        nonlocal lineitem
        lineitem = lineitem()

        lineitem_filtered = lineitem.loc[
            :,
            [
                "L_QUANTITY",
                "L_EXTENDEDPRICE",
                "L_DISCOUNT",
                "L_TAX",
                "L_RETURNFLAG",
                "L_LINESTATUS",
                "L_SHIPDATE",
                "L_ORDERKEY",
            ],
        ]
        sel = lineitem_filtered.L_SHIPDATE <= date
        lineitem_filtered = lineitem_filtered[sel]
        lineitem_filtered["AVG_QTY"] = lineitem_filtered.L_QUANTITY
        lineitem_filtered["AVG_PRICE"] = lineitem_filtered.L_EXTENDEDPRICE
        lineitem_filtered["DISC_PRICE"] = lineitem_filtered.L_EXTENDEDPRICE * (
            1 - lineitem_filtered.L_DISCOUNT
        )
        lineitem_filtered["CHARGE"] = (
            lineitem_filtered.L_EXTENDEDPRICE
            * (1 - lineitem_filtered.L_DISCOUNT)
            * (1 + lineitem_filtered.L_TAX)
        )
        gb = lineitem_filtered.groupby(["L_RETURNFLAG", "L_LINESTATUS"], as_index=False)[
            [
                "L_QUANTITY",
                "L_EXTENDEDPRICE",
                "DISC_PRICE",
                "CHARGE",
                "AVG_QTY",
                "AVG_PRICE",
                "L_DISCOUNT",
                "L_ORDERKEY",
            ]
        ]
        total = gb.agg(
            {
                "L_QUANTITY": "sum",
                "L_EXTENDEDPRICE": "sum",
                "DISC_PRICE": "sum",
                "CHARGE": "sum",
                "AVG_QTY": "mean",
                "AVG_PRICE": "mean",
                "L_DISCOUNT": "mean",
                "L_ORDERKEY": "count",
            }
        )
        total = total.sort_values(["L_RETURNFLAG", "L_LINESTATUS"])
        total._query_compiler._dataframe._query_tree.execute()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
