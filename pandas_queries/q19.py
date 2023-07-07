from pandas_queries import utils

Q_NUM = 19


def q():
    Brand31 = "Brand#31"
    Brand43 = "Brand#43"
    SMBOX = "SM BOX"
    SMCASE = "SM CASE"
    SMPACK = "SM PACK"
    SMPKG = "SM PKG"
    MEDBAG = "MED BAG"
    MEDBOX = "MED BOX"
    MEDPACK = "MED PACK"
    MEDPKG = "MED PKG"
    LGBOX = "LG BOX"
    LGCASE = "LG CASE"
    LGPACK = "LG PACK"
    LGPKG = "LG PKG"
    DELIVERINPERSON = "DELIVER IN PERSON"
    AIR = "AIR"
    AIRREG = "AIRREG"
    line_item_ds = utils.get_line_item_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()

    def query():
        nonlocal line_item_ds

        part = utils.get_part_ds()
        lineitem = line_item_ds()

        lsel = (
            (
                ((lineitem.L_QUANTITY <= 36) & (lineitem.L_QUANTITY >= 26))
                | ((lineitem.L_QUANTITY <= 25) & (lineitem.L_QUANTITY >= 15))
                | ((lineitem.L_QUANTITY <= 14) & (lineitem.L_QUANTITY >= 4))
            )
            & (lineitem.L_SHIPINSTRUCT == DELIVERINPERSON)
            & ((lineitem.L_SHIPMODE == AIR) | (lineitem.L_SHIPMODE == AIRREG))
        )
        psel = (part.P_SIZE >= 1) & (
            (
                (part.P_SIZE <= 5)
                & (part.P_BRAND == Brand31)
                & (
                    (part.P_CONTAINER == SMBOX)
                    | (part.P_CONTAINER == SMCASE)
                    | (part.P_CONTAINER == SMPACK)
                    | (part.P_CONTAINER == SMPKG)
                )
            )
            | (
                (part.P_SIZE <= 10)
                & (part.P_BRAND == Brand43)
                & (
                    (part.P_CONTAINER == MEDBAG)
                    | (part.P_CONTAINER == MEDBOX)
                    | (part.P_CONTAINER == MEDPACK)
                    | (part.P_CONTAINER == MEDPKG)
                )
            )
            | (
                (part.P_SIZE <= 15)
                & (part.P_BRAND == Brand43)
                & (
                    (part.P_CONTAINER == LGBOX)
                    | (part.P_CONTAINER == LGCASE)
                    | (part.P_CONTAINER == LGPACK)
                    | (part.P_CONTAINER == LGPKG)
                )
            )
        )
        flineitem = lineitem[lsel]
        fpart = part[psel]
        jn = flineitem.merge(fpart, left_on="L_PARTKEY", right_on="P_PARTKEY")
        jnsel = (
            (jn.P_BRAND == Brand31)
            & (
                (jn.P_CONTAINER == SMBOX)
                | (jn.P_CONTAINER == SMCASE)
                | (jn.P_CONTAINER == SMPACK)
                | (jn.P_CONTAINER == SMPKG)
            )
            & (jn.L_QUANTITY >= 4)
            & (jn.L_QUANTITY <= 14)
            & (jn.P_SIZE <= 5)
            | (jn.P_BRAND == Brand43)
            & (
                (jn.P_CONTAINER == MEDBAG)
                | (jn.P_CONTAINER == MEDBOX)
                | (jn.P_CONTAINER == MEDPACK)
                | (jn.P_CONTAINER == MEDPKG)
            )
            & (jn.L_QUANTITY >= 15)
            & (jn.L_QUANTITY <= 25)
            & (jn.P_SIZE <= 10)
            | (jn.P_BRAND == Brand43)
            & (
                (jn.P_CONTAINER == LGBOX)
                | (jn.P_CONTAINER == LGCASE)
                | (jn.P_CONTAINER == LGPACK)
                | (jn.P_CONTAINER == LGPKG)
            )
            & (jn.L_QUANTITY >= 26)
            & (jn.L_QUANTITY <= 36)
            & (jn.P_SIZE <= 15)
        )
        jn = jn[jnsel]
        total = (jn.L_EXTENDEDPRICE * (1.0 - jn.L_DISCOUNT)).sum()
        return total

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
