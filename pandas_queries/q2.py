from pandas_queries import utils

Q_NUM = 2


def q():
    region_ds = utils.get_region_ds
    nation_ds = utils.get_nation_ds
    supplier_ds = utils.get_supplier_ds
    part_ds = utils.get_part_ds
    part_supp_ds = utils.get_part_supp_ds

    # first call one time to cache in case we don't include the IO times
    region_ds()
    nation_ds()
    supplier_ds()
    part_ds()
    part_supp_ds()

    def query():
        nonlocal region_ds
        nonlocal nation_ds
        nonlocal supplier_ds
        nonlocal part_ds
        nonlocal part_supp_ds
        region = region_ds()
        nation = nation_ds()
        supplier = supplier_ds()
        part = part_ds()
        partsupp = part_supp_ds()

        nation_filtered = nation.loc[:, ["N_NATIONKEY", "N_NAME", "N_REGIONKEY"]]
        region_filtered = region[(region["R_NAME"] == "EUROPE")]
        region_filtered = region_filtered.loc[:, ["R_REGIONKEY"]]
        r_n_merged = nation_filtered.merge(
            region_filtered, left_on="N_REGIONKEY", right_on="R_REGIONKEY", how="inner"
        )
        r_n_merged = r_n_merged.loc[:, ["N_NATIONKEY", "N_NAME"]]
        supplier_filtered = supplier.loc[
            :,
            [
                "S_SUPPKEY",
                "S_NAME",
                "S_ADDRESS",
                "S_NATIONKEY",
                "S_PHONE",
                "S_ACCTBAL",
                "S_COMMENT",
            ],
        ]
        s_r_n_merged = r_n_merged.merge(
            supplier_filtered, left_on="N_NATIONKEY", right_on="S_NATIONKEY", how="inner"
        )
        s_r_n_merged = s_r_n_merged.loc[
            :,
            [
                "N_NAME",
                "S_SUPPKEY",
                "S_NAME",
                "S_ADDRESS",
                "S_PHONE",
                "S_ACCTBAL",
                "S_COMMENT",
            ],
        ]
        partsupp_filtered = partsupp.loc[:, ["PS_PARTKEY", "PS_SUPPKEY", "PS_SUPPLYCOST"]]
        ps_s_r_n_merged = s_r_n_merged.merge(
            partsupp_filtered, left_on="S_SUPPKEY", right_on="PS_SUPPKEY", how="inner"
        )
        ps_s_r_n_merged = ps_s_r_n_merged.loc[
            :,
            [
                "N_NAME",
                "S_NAME",
                "S_ADDRESS",
                "S_PHONE",
                "S_ACCTBAL",
                "S_COMMENT",
                "PS_PARTKEY",
                "PS_SUPPLYCOST",
            ],
        ]
        part_filtered = part.loc[:, ["P_PARTKEY", "P_MFGR", "P_SIZE", "P_TYPE"]]
        part_filtered = part_filtered[
            (part_filtered["P_SIZE"] == 15)
            & (part_filtered["P_TYPE"].str.endswith("BRASS"))
        ]
        part_filtered = part_filtered.loc[:, ["P_PARTKEY", "P_MFGR"]]
        merged_df = part_filtered.merge(
            ps_s_r_n_merged, left_on="P_PARTKEY", right_on="PS_PARTKEY", how="inner"
        )
        merged_df = merged_df.loc[
            :,
            [
                "N_NAME",
                "S_NAME",
                "S_ADDRESS",
                "S_PHONE",
                "S_ACCTBAL",
                "S_COMMENT",
                "PS_SUPPLYCOST",
                "P_PARTKEY",
                "P_MFGR",
            ],
        ]
        min_values = merged_df.groupby("P_PARTKEY", as_index=False, sort=False)[
            "PS_SUPPLYCOST"
        ].min()
        min_values.columns = ["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]
        merged_df = merged_df.merge(
            min_values,
            left_on=["P_PARTKEY", "PS_SUPPLYCOST"],
            right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"],
            how="inner",
        )
        total = merged_df.loc[
            :,
            [
                "S_ACCTBAL",
                "S_NAME",
                "N_NAME",
                "P_PARTKEY",
                "P_MFGR",
                "S_ADDRESS",
                "S_PHONE",
                "S_COMMENT",
            ],
        ]
        total = total.sort_values(
            by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"],
            ascending=[False, True, True, True],
        )
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
