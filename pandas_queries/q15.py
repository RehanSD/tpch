from pandas_queries import utils
from pandas import Timestamp, DateOffset, NamedAgg

Q_NUM = 15


def q():
    line_item_ds = utils.get_line_item_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    supplier_ds()

    def query():
        nonlocal line_item_ds
        nonlocal supplier_ds

        supplier = supplier_ds()
        lineitem = line_item_ds()

        lineitem_filtered = lineitem[
            (lineitem["L_SHIPDATE"] >= Timestamp("1996-01-01"))
            & (
                lineitem["L_SHIPDATE"]
                < (Timestamp("1996-01-01") + DateOffset(months=3))
            )
        ]
        lineitem_filtered["REVENUE_PARTS"] = lineitem_filtered["L_EXTENDEDPRICE"] * (
            1.0 - lineitem_filtered["L_DISCOUNT"]
        )
        lineitem_filtered = lineitem_filtered.loc[:, ["L_SUPPKEY", "REVENUE_PARTS"]]
        revenue_table = (
            lineitem_filtered.groupby("L_SUPPKEY", as_index=False, sort=False)
            .agg(TOTAL_REVENUE=NamedAgg(column="REVENUE_PARTS", aggfunc="sum"))
            .rename(columns={"L_SUPPKEY": "SUPPLIER_NO"})
        )
        max_revenue = revenue_table["TOTAL_REVENUE"].max()
        revenue_table = revenue_table[revenue_table["TOTAL_REVENUE"] == max_revenue]
        supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE"]]
        total = supplier_filtered.merge(
            revenue_table, left_on="S_SUPPKEY", right_on="SUPPLIER_NO", how="inner"
        )
        total = total.loc[
            :, ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE"]
        ]
        return total
    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
