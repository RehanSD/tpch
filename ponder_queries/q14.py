from ponder_queries import utils
from pandas import Timestamp

Q_NUM = 14


def q():
    startDate = Timestamp("1994-03-01")
    endDate = Timestamp("1994-04-01")
    line_item_ds = utils.get_line_item_ds
    # first call one time to cache in case we don't include the IO times
    line_item_ds()

    def query():
        nonlocal line_item_ds

        part = utils.get_part_ds()
        lineitem = line_item_ds()

        p_type_like = "PROMO"
        part_filtered = part.loc[:, ["P_PARTKEY", "P_TYPE"]]
        lineitem_filtered = lineitem.loc[
            :, ["L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE", "L_PARTKEY"]
        ]
        sel = (lineitem_filtered.L_SHIPDATE >= startDate) & (
            lineitem_filtered.L_SHIPDATE < endDate
        )
        flineitem = lineitem_filtered[sel]
        jn = flineitem.merge(part_filtered, left_on="L_PARTKEY", right_on="P_PARTKEY")
        jn["TMP"] = jn.L_EXTENDEDPRICE * (1.0 - jn.L_DISCOUNT)
        total = jn[jn.P_TYPE.str.startswith(p_type_like)].TMP.sum() * 100 / jn.TMP.sum()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
