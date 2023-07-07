from ponder_queries import utils

Q_NUM = 13


def q():
    customer_ds = utils.get_customer_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds()
    orders_ds()

    def query():
        nonlocal customer_ds
        nonlocal orders_ds

        orders = orders_ds()
        customer = customer_ds()

        customer_filtered = customer.loc[:, ["C_CUSTKEY"]]
        orders_filtered = orders[
            ~orders["O_COMMENT"].str.contains(r"special[\S|\s]*requests")
        ]
        orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY", "O_CUSTKEY"]]
        c_o_merged = customer_filtered.merge(
            orders_filtered, left_on="C_CUSTKEY", right_on="O_CUSTKEY", how="left"
        )
        c_o_merged = c_o_merged.loc[:, ["C_CUSTKEY", "O_ORDERKEY"]]
        count_df = c_o_merged.groupby(["C_CUSTKEY"], as_index=False, sort=False).agg(
            {"O_ORDERKEY": "count"}
        ).rename(columns={"O_ORDERKEY":"C_COUNT"})
        total = count_df.groupby(["C_COUNT"], as_index=False, sort=False).size()
        total.columns = ["C_COUNT", "CUSTDIST"]
        total = total.sort_values(by=["CUSTDIST", "C_COUNT"], ascending=[False, False])
        total._query_compiler._dataframe._query_tree.execute()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
