from ponder_queries import utils

Q_NUM = 22


def q():
    orders_ds = utils.get_orders_ds
    customer_ds = utils.get_customer_ds

    # first call one time to cache in case we don't include the IO times
    orders_ds()
    customer_ds()

    def query():
        nonlocal customer_ds
        nonlocal orders_ds

        orders = orders_ds()
        customer = customer_ds()

        customer_filtered = customer.loc[:, ["C_ACCTBAL", "C_CUSTKEY"]]
        customer_filtered["CNTRYCODE"] = customer["C_PHONE"].str.slice(0, 2)
        customer_filtered = customer_filtered[
            (customer["C_ACCTBAL"] > 0.00)
            & customer_filtered["CNTRYCODE"].isin(
                ["13", "31", "23", "29", "30", "18", "17"]
            )
        ]
        avg_value = customer_filtered["C_ACCTBAL"].mean()
        customer_filtered = customer_filtered[customer_filtered["C_ACCTBAL"] > avg_value]
        # Select only the keys that don't match by performing a left join and only selecting columns with an na value
        orders_filtered = orders.loc[:, ["O_CUSTKEY"]].drop_duplicates()
        customer_keys = customer_filtered.loc[:, ["C_CUSTKEY"]].drop_duplicates()
        customer_selected = customer_keys.merge(
            orders_filtered, left_on="C_CUSTKEY", right_on="O_CUSTKEY", how="left"
        )
        customer_selected = customer_selected[customer_selected["O_CUSTKEY"].isna()]
        customer_selected = customer_selected.loc[:, ["C_CUSTKEY"]]
        customer_selected = customer_selected.merge(
            customer_filtered, on="C_CUSTKEY", how="inner"
        )
        customer_selected = customer_selected.loc[:, ["CNTRYCODE", "C_ACCTBAL"]]
        agg1 = customer_selected.groupby(["CNTRYCODE"], as_index=False, sort=False).size()
        agg1.columns = ["CNTRYCODE", "NUMCUST"]
        agg2 = customer_selected.groupby(["CNTRYCODE"], as_index=False, sort=False).agg(
            {"C_ACCTBAL": "sum"}
        ).rename(columns={"C_ACCTBAL": "TOTACCTBAL"})
        total = agg1.merge(agg2, on="CNTRYCODE", how="inner")
        total = total.sort_values(by=["CNTRYCODE"], ascending=[True])
        total._query_compiler._dataframe._query_tree.execute()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
