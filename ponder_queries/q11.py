from ponder_queries import utils

Q_NUM = 11


def q():
    nation_ds = utils.get_nation_ds
    supplier_ds = utils.get_supplier_ds
    part_supp_ds = utils.get_part_supp_ds

    # first call one time to cache in case we don't include the IO times
    nation_ds()
    supplier_ds()
    part_supp_ds()

    def query():
        nonlocal nation_ds
        nonlocal supplier_ds
        nonlocal part_supp_ds

        supplier = supplier_ds()
        nation = nation_ds()
        partsupp = part_supp_ds()

        partsupp_filtered = partsupp.loc[:, ["PS_PARTKEY", "PS_SUPPKEY"]]
        partsupp_filtered["TOTAL_COST"] = (
            partsupp["PS_SUPPLYCOST"] * partsupp["PS_AVAILQTY"]
        )
        supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
        ps_supp_merge = partsupp_filtered.merge(
            supplier_filtered, left_on="PS_SUPPKEY", right_on="S_SUPPKEY", how="inner"
        )
        ps_supp_merge = ps_supp_merge.loc[:, ["PS_PARTKEY", "S_NATIONKEY", "TOTAL_COST"]]
        nation_filtered = nation[(nation["N_NAME"] == "GERMANY")]
        nation_filtered = nation_filtered.loc[:, ["N_NATIONKEY"]]
        ps_supp_n_merge = ps_supp_merge.merge(
            nation_filtered, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner"
        )
        ps_supp_n_merge = ps_supp_n_merge.loc[:, ["PS_PARTKEY", "TOTAL_COST"]]
        sum_val = ps_supp_n_merge["TOTAL_COST"].sum() * 0.0001
        total = ps_supp_n_merge.groupby(["PS_PARTKEY"], as_index=False, sort=False).agg(
            {"TOTAL_COST": "sum"}
        ).rename(columns={"TOTAL_COST":"VALUE"})
        total = total[total["VALUE"] > sum_val]
        total = total.sort_values("VALUE", ascending=False)
        total._query_compiler._dataframe._query_tree.execute()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
