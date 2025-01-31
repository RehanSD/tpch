from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 12


def q():
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()

    var_1 = "MAIL"
    var_2 = "SHIP"
    var_3 = datetime(1994, 1, 1)
    var_4 = datetime(1995, 1, 1)

    q_final = (
        orders_ds.join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("l_shipmode").is_in([var_1, var_2]))
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .filter(pl.col("l_shipdate") < pl.col("l_commitdate"))
        .filter(pl.col("l_receiptdate").is_between(var_3, var_4, closed="left"))
        .with_columns(
            [
                pl.when(pl.col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"]))
                .then(1)
                .otherwise(0)
                .alias("high_line_count"),
                pl.when(
                    pl.col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"]).is_not()
                )
                .then(1)
                .otherwise(0)
                .alias("low_line_count"),
            ]
        )
        .groupby("l_shipmode")
        .agg([pl.col("high_line_count").sum(), pl.col("low_line_count").sum()])
        .sort("l_shipmode")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
