from pandas_queries import utils

Q_NUM = 16


def q():
    supplier_ds = utils.get_supplier_ds
    part_supp_ds = utils.get_part_supp_ds

    # first call one time to cache in case we don't include the IO times
    supplier_ds()
    part_supp_ds()

    def query():
        nonlocal supplier_ds
        nonlocal part_supp_ds

        part = utils.get_part_ds()
        supplier = supplier_ds()
        partsupp = part_supp_ds()

        part_filtered = part[
            (part["P_BRAND"] != "Brand#45")
            & (~part["P_TYPE"].str.contains("^MEDIUM POLISHED"))
            & part["P_SIZE"].isin([49, 14, 23, 45, 19, 3, 36, 9])
        ]
        part_filtered = part_filtered.loc[:, ["P_PARTKEY", "P_BRAND", "P_TYPE", "P_SIZE"]]
        partsupp_filtered = partsupp.loc[:, ["PS_PARTKEY", "PS_SUPPKEY"]]
        total = part_filtered.merge(
            partsupp_filtered, left_on="P_PARTKEY", right_on="PS_PARTKEY", how="inner"
        )
        total = total.loc[:, ["P_BRAND", "P_TYPE", "P_SIZE", "PS_SUPPKEY"]]
        supplier_filtered = supplier[
            supplier["S_COMMENT"].str.contains(r"Customer(\S|\s)*Complaints")
        ]
        supplier_filtered = supplier_filtered.loc[:, ["S_SUPPKEY"]].drop_duplicates()
        # left merge to select only PS_SUPPKEY values not in supplier_filtered
        total = total.merge(
            supplier_filtered, left_on="PS_SUPPKEY", right_on="S_SUPPKEY", how="left"
        )
        total = total[total["S_SUPPKEY"].isna()]
        total = total.loc[:, ["P_BRAND", "P_TYPE", "P_SIZE", "PS_SUPPKEY"]]
        total = total.groupby(["P_BRAND", "P_TYPE", "P_SIZE"], as_index=False, sort=False)[
            "PS_SUPPKEY"
        ].nunique()
        total.columns = ["P_BRAND", "P_TYPE", "P_SIZE", "SUPPLIER_CNT"]
        total = total.sort_values(
            by=["SUPPLIER_CNT", "P_BRAND", "P_TYPE", "P_SIZE"],
            ascending=[False, True, True, True],
        )
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
