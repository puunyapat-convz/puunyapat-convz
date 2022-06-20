import math

all_tables = {
    "ODP_JDA": 
    [
        "BCH_JDA_DataPlatform_APADDR",  "BCH_JDA_DataPlatform_APPSUPR", "BCH_JDA_DataPlatform_APSUPP",
        "BCH_JDA_DataPlatform_APSUPX",  "BCH_JDA_DataPlatform_APTERM",  "BCH_JDA_DataPlatform_APVATR",
        "BCH_JDA_DataPlatform_FACTAG",  "BCH_JDA_DataPlatform_FACTAGE", "BCH_JDA_DataPlatform_INVDPT",
        "BCH_JDA_DataPlatform_INVMFG",  "BCH_JDA_DataPlatform_INVUMR",  "BCH_JDA_DataPlatform_JDAACSTK",
        "BCH_JDA_DataPlatform_JDABAR",  "BCH_JDA_DataPlatform_JDAPHC",  "BCH_JDA_DataPlatform_JDAPRC", 
        "BCH_JDA_DataPlatform_JDASAL",  "BCH_JDA_DataPlatform_JDASKU",  "BCH_JDA_DataPlatform_JDASTK",
        "BCH_JDA_DataPlatform_JDATRN",  "BCH_JDA_DataPlatform_MGRBVD",  "BCH_JDA_DataPlatform_MGROBS1",
        "BCH_JDA_DataPlatform_MSTTYP",  "BCH_JDA_DataPlatform_RPLPRF",  "BCH_JDA_DataPlatform_RPLPRM", 
        "BCH_JDA_DataPlatform_RPLSDT",  "BCH_JDA_DataPlatform_RPLSHD",  "BCH_JDA_DataPlatform_SETDTL",
        "BCH_JDA_DataPlatform_TAXTRT",  "BCH_JDA_DataPlatform_TBLCNT",  "BCH_JDA_DataPlatform_TBLCOL",
        "BCH_JDA_DataPlatform_TBLDIS",  "BCH_JDA_DataPlatform_TBLDST",  "BCH_JDA_DataPlatform_TBLFIN",
        "BCH_JDA_DataPlatform_TBLFLD",  "BCH_JDA_DataPlatform_TBLPRV",  "BCH_JDA_DataPlatform_TBLREG",
        "BCH_JDA_DataPlatform_TBLSIZ",  "BCH_JDA_DataPlatform_TBLSTR"
    ],

    "ODP_JDA_2": 
    [
        "BCH_JDA_DataPlatform_INVCAL",  "BCH_JDA_DataPlatform_JDAPCALW","BCH_JDA_DataPlatform_JDAPCDSQ",
        "BCH_JDA_DataPlatform_JDAPCDTL","BCH_JDA_DataPlatform_JDAPCFGP","BCH_JDA_DataPlatform_JDAPCHDR",
        "BCH_JDA_DataPlatform_JDAPOALW","BCH_JDA_DataPlatform_JDAPODSQ","BCH_JDA_DataPlatform_JDAPODTL",
        "BCH_JDA_DataPlatform_JDAPOFGP","BCH_JDA_DataPlatform_JDAPOHDR","BCH_JDA_DataPlatform_MSTFLD",
        "BCH_JDA_DataPlatform_MSTITA"
    ],

    "B2S_JDA": 
    [
        "BCH_JDA_DataPlatform_APADDR",  "BCH_JDA_DataPlatform_APPSUPR", "BCH_JDA_DataPlatform_APSUPP",
        "BCH_JDA_DataPlatform_APSUPX",  "BCH_JDA_DataPlatform_APTERM",  "BCH_JDA_DataPlatform_APVATR",
        "BCH_JDA_DataPlatform_FACTAG",  "BCH_JDA_DataPlatform_FACTAGE", "BCH_JDA_DataPlatform_INVDPT",
        "BCH_JDA_DataPlatform_INVMFG",  "BCH_JDA_DataPlatform_INVUMR",  "BCH_JDA_DataPlatform_JDAACSTK",
        "BCH_JDA_DataPlatform_JDABAR",  "BCH_JDA_DataPlatform_JDAPHC",  "BCH_JDA_DataPlatform_JDAPRC",
        "BCH_JDA_DataPlatform_JDASAL",  "BCH_JDA_DataPlatform_JDASKU",  "BCH_JDA_DataPlatform_JDASTK",
        "BCH_JDA_DataPlatform_JDATRN",  "BCH_JDA_DataPlatform_MGRBVD",  "BCH_JDA_DataPlatform_MGROBS1",
        "BCH_JDA_DataPlatform_MSTTYP",  "BCH_JDA_DataPlatform_RPLPRF",  "BCH_JDA_DataPlatform_RPLPRM",
        "BCH_JDA_DataPlatform_RPLSDT",  "BCH_JDA_DataPlatform_RPLSHD",  "BCH_JDA_DataPlatform_SETDTL",
        "BCH_JDA_DataPlatform_TAXTRT",  "BCH_JDA_DataPlatform_TBLCNT",  "BCH_JDA_DataPlatform_TBLCOL",
        "BCH_JDA_DataPlatform_TBLDIS",  "BCH_JDA_DataPlatform_TBLDST",  "BCH_JDA_DataPlatform_TBLFIN",
        "BCH_JDA_DataPlatform_TBLFLD",  "BCH_JDA_DataPlatform_TBLPRV",  "BCH_JDA_DataPlatform_TBLREG",
        "BCH_JDA_DataPlatform_TBLSIZ",  "BCH_JDA_DataPlatform_TBLSTR",
        "BCH_JDA_DataPlatform_TBLBOK",  "BCH_JDA_DataPlatform_MSTBOK",  "BCH_JDA_DataPlatform_MSTISB"
    ],
    
    "B2S_JDA_2": 
    [
        "BCH_JDA_DataPlatform_INVCAL",  "BCH_JDA_DataPlatform_JDAPCALW","BCH_JDA_DataPlatform_JDAPCDSQ",
        "BCH_JDA_DataPlatform_JDAPCDTL","BCH_JDA_DataPlatform_JDAPCFGP","BCH_JDA_DataPlatform_JDAPCHDR",
        "BCH_JDA_DataPlatform_JDAPOALW","BCH_JDA_DataPlatform_JDAPODSQ","BCH_JDA_DataPlatform_JDAPODTL",
        "BCH_JDA_DataPlatform_JDAPOFGP","BCH_JDA_DataPlatform_JDAPOHDR","BCH_JDA_DataPlatform_MSTFLD",
        "BCH_JDA_DataPlatform_MSTITA"
    ],

    "ODP_POS": 
    [
        "POS_DataPlatform_Master_Discount",     "POS_DataPlatform_Master_DiscountList",
        "POS_DataPlatform_Master_PaymentMedia", "POS_DataPlatform_Master_Promotion",
        "POS_DataPlatform_Txn_DiscountCoupon",  "POS_DataPlatform_Txn_Installment",
        "POS_DataPlatform_Txn_Payment",         "POS_DataPlatform_Txn_Sales",
        "POS_DataPlatform_Txn_Translator"
    ],

    "B2S_POS": 
    [
        "POS_DataPlatform_Master_Discount",     "POS_DataPlatform_Master_DiscountList",
        "POS_DataPlatform_Master_PaymentMedia", "POS_DataPlatform_Master_Promotion",
        "POS_DataPlatform_Txn_DiscountCoupon",  "POS_DataPlatform_Txn_Installment",
        "POS_DataPlatform_Txn_Payment",         "POS_DataPlatform_Txn_Sales",
        "POS_DataPlatform_Txn_Translator"
    ]
}

# tables   = all_tables.get("B2S_JDA")
max_conn = 7

# div by 7
for table_set in all_tables.keys():
    for index, table in enumerate(all_tables.get(table_set)):
        order = math.floor(index/max_conn)
        if "POS" in table:
            order += 4
        delay = order * 5
        print(index, table, delay)
