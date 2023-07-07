from pandas_queries import utils

Q_NUM = 9


def q():
    nation_ds = utils.get_nation_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds
    part_supp_ds = utils.get_part_supp_ds

    # first call one time to cache in case we don't include the IO times
    nation_ds()
    line_item_ds()
    orders_ds()
    supplier_ds()
    part_supp_ds()

    def query():
        nonlocal nation_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal supplier_ds
        nonlocal part_supp_ds

        part = utils.get_part_ds()
        supplier = supplier_ds()
        lineitem = line_item_ds()
        orders = orders_ds()
        nation = nation_ds()
        partsupp = part_supp_ds()

        psel = part.P_NAME.str.contains("ghost")
        fpart = part[psel]
        jn1 = lineitem.merge(fpart, left_on="L_PARTKEY", right_on="P_PARTKEY")
        jn2 = jn1.merge(supplier, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        jn3 = jn2.merge(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        jn4 = partsupp.merge(
            jn3, left_on=["PS_PARTKEY", "PS_SUPPKEY"], right_on=["L_PARTKEY", "L_SUPPKEY"]
        )
        jn5 = jn4.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        jn5["TMP"] = jn5.L_EXTENDEDPRICE * (1 - jn5.L_DISCOUNT) - (
            (1 * jn5.PS_SUPPLYCOST) * jn5.L_QUANTITY
        )
        jn5["O_YEAR"] = jn5.O_ORDERDATE.dt.year
        gb = jn5.groupby(["N_NAME", "O_YEAR"], as_index=False, sort=False)["TMP"].sum()
        total = gb.sort_values(["N_NAME", "O_YEAR"], ascending=[True, False])
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
