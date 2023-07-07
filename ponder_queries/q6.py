from datetime import datetime
from pandas import Timestamp
import modin.pandas as pd

from ponder_queries import utils

Q_NUM = 6


def q():
    date1 = Timestamp(datetime(1994, 1, 1))
    date2 = Timestamp(datetime(1995, 1, 1))
    var3 = 24

    line_item_ds = utils.get_line_item_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()

    def query():
        nonlocal line_item_ds
        lineitem = line_item_ds()

        lineitem_filtered = lineitem.loc[
            :, ["L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]
        ]
        sel = (
            (lineitem_filtered.L_SHIPDATE >= date1)
            & (lineitem_filtered.L_SHIPDATE < date2)
            & (lineitem_filtered.L_DISCOUNT >= 0.05)
            & (lineitem_filtered.L_DISCOUNT <= 0.07)
            & (lineitem_filtered.L_QUANTITY < var3)
        )

        flineitem = lineitem_filtered[sel]
        total = (flineitem.L_EXTENDEDPRICE * flineitem.L_DISCOUNT).sum()
        result_df = pd.DataFrame({"revenue": [total]})
        result_df._query_compiler._dataframe._query_tree.execute()
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
