from pandas_queries import utils
from pandas import Timestamp

Q_NUM = 20


def q():
    date1 = Timestamp("1996-01-01")
    date2 = Timestamp("1997-01-01")

    nation_ds = utils.get_nation_ds
    line_item_ds = utils.get_line_item_ds
    part_supp_ds = utils.get_part_supp_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    nation_ds()
    line_item_ds()
    part_supp_ds()
    supplier_ds()

    def query():
        nonlocal nation_ds
        nonlocal line_item_ds
        nonlocal part_supp_ds
        nonlocal supplier_ds

        part = utils.get_part_ds()
        lineitem = line_item_ds()
        nation = nation_ds()
        partsupp = part_supp_ds()
        supplier = supplier_ds()

        psel = part.P_NAME.str.startswith("azure")
        nsel = nation.N_NAME == "JORDAN"
        lsel = (lineitem.L_SHIPDATE >= date1) & (lineitem.L_SHIPDATE < date2)
        fpart = part[psel]
        fnation = nation[nsel]
        flineitem = lineitem[lsel]
        jn1 = fpart.merge(partsupp, left_on="P_PARTKEY", right_on="PS_PARTKEY")
        jn2 = jn1.merge(
            flineitem,
            left_on=["PS_PARTKEY", "PS_SUPPKEY"],
            right_on=["L_PARTKEY", "L_SUPPKEY"],
        )
        gb = jn2.groupby(
            ["PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY"], as_index=False, sort=False
        )["L_QUANTITY"].sum()
        gbsel = gb.PS_AVAILQTY > (0.5 * gb.L_QUANTITY)
        fgb = gb[gbsel]
        jn3 = fgb.merge(supplier, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
        jn4 = fnation.merge(jn3, left_on="N_NATIONKEY", right_on="S_NATIONKEY")
        jn4 = jn4.loc[:, ["S_NAME", "S_ADDRESS"]]
        total = jn4.sort_values("S_NAME").drop_duplicates()
        total._query_compiler._dataframe._query_tree.execute()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
