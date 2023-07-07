from pandas_queries import utils
from pandas import NamedAgg
import pandas as pd

Q_NUM = 17


def q():
    line_item_ds = utils.get_line_item_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()

    def query():
        nonlocal line_item_ds

        part = utils.get_part_ds()
        lineitem = line_item_ds()

        left = lineitem.loc[:, ["L_PARTKEY", "L_QUANTITY", "L_EXTENDEDPRICE"]]
        right = part[((part["P_BRAND"] == "Brand#23") & (part["P_CONTAINER"] == "MED BOX"))]
        right = right.loc[:, ["P_PARTKEY"]]
        line_part_merge = left.merge(
            right, left_on="L_PARTKEY", right_on="P_PARTKEY", how="inner"
        )
        line_part_merge = line_part_merge.loc[
            :, ["L_QUANTITY", "L_EXTENDEDPRICE", "P_PARTKEY"]
        ]
        lineitem_filtered = lineitem.loc[:, ["L_PARTKEY", "L_QUANTITY"]]
        lineitem_avg = lineitem_filtered.groupby(
            ["L_PARTKEY"], as_index=False, sort=False
        ).agg(avg=NamedAgg(column="L_QUANTITY", aggfunc="mean"))
        lineitem_avg["avg"] = 0.2 * lineitem_avg["avg"]
        lineitem_avg = lineitem_avg.loc[:, ["L_PARTKEY", "avg"]]
        total = line_part_merge.merge(
            lineitem_avg, left_on="P_PARTKEY", right_on="L_PARTKEY", how="inner"
        )
        total = total[total["L_QUANTITY"] < total["avg"]]
        total = pd.DataFrame({"avg_yearly": [total["L_EXTENDEDPRICE"].sum() / 7.0]})
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
