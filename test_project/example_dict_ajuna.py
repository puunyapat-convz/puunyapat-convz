## dictionary get tutorial https://note.nkmk.me/en/python-dict-get/

MAIN_FOLDER    = 'POS'
all_extension  = { 'JDA':['dat','ctrl'], 'POS':['txt','log'] }

# getting value from key name with .get() method
file_extension = all_extension.get(MAIN_FOLDER)[0]

# getting path for control files by specify element of list
control_path   = all_extension.get(MAIN_FOLDER)[1]

print("   extension: %s" % file_extension)
print("control path: %s" % control_path)

ALL_FOLDERS={
    "ODP/JDA": [
                    "BCH_JDA_DataPlatform_APADDR", "BCH_JDA_DataPlatform_APPSUPR", "BCH_JDA_DataPlatform_APSUPP",
                    "BCH_JDA_DataPlatform_APSUPX", "BCH_JDA_DataPlatform_APTERM" , "BCH_JDA_DataPlatform_APVATR",
                    "BCH_JDA_DataPlatform_FACTAG", "BCH_JDA_DataPlatform_FACTAGE", "BCH_JDA_DataPlatform_INVDPT",
                    "BCH_JDA_DataPlatform_INVMFG", "BCH_JDA_DataPlatform_INVUMR" , "BCH_JDA_DataPlatform_JDAACSTK",
                    "BCH_JDA_DataPlatform_JDABAR", "BCH_JDA_DataPlatform_JDAPHC" , "BCH_JDA_DataPlatform_JDAPRC",
                    "BCH_JDA_DataPlatform_JDASAL", "BCH_JDA_DataPlatform_JDASKU" , "BCH_JDA_DataPlatform_JDASTK",
                    "BCH_JDA_DataPlatform_JDATRN", "BCH_JDA_DataPlatform_MGRBVD" , "BCH_JDA_DataPlatform_MGROBS1",
                    "BCH_JDA_DataPlatform_MSTTYP", "BCH_JDA_DataPlatform_RPLPRF" , "BCH_JDA_DataPlatform_RPLPRM", 
                    "BCH_JDA_DataPlatform_RPLSDT", "BCH_JDA_DataPlatform_RPLSHD" , "BCH_JDA_DataPlatform_SETDTL", 
                    "BCH_JDA_DataPlatform_TAXTRT", "BCH_JDA_DataPlatform_TBLCNT" , "BCH_JDA_DataPlatform_TBLCOL",
                    "BCH_JDA_DataPlatform_TBLDIS", "BCH_JDA_DataPlatform_TBLDST" , "BCH_JDA_DataPlatform_TBLFIN",
                    "BCH_JDA_DataPlatform_TBLFLD", "BCH_JDA_DataPlatform_TBLPRV" , "BCH_JDA_DataPlatform_TBLREG",
                    "BCH_JDA_DataPlatform_TBLSIZ", "BCH_JDA_DataPlatform_TBLSTR"
                ],
    "B2S/JDA":  [   "BCH_JDA_DataPlatform_APADDRx", "BCH_JDA_DataPlatform_APPSUPR", "BCH_JDA_DataPlatform_APSUPP",
                    "BCH_JDA_DataPlatform_APSUPX", "BCH_JDA_DataPlatform_APTERM" , "BCH_JDA_DataPlatform_APVATR",
                    "BCH_JDA_DataPlatform_FACTAG", "BCH_JDA_DataPlatform_FACTAGE", "BCH_JDA_DataPlatform_INVDPT",
                    "BCH_JDA_DataPlatform_INVMFG", "BCH_JDA_DataPlatform_INVUMR" , "BCH_JDA_DataPlatform_JDAACSTK",
                    "BCH_JDA_DataPlatform_JDABAR", "BCH_JDA_DataPlatform_JDAPHC" , "BCH_JDA_DataPlatform_JDAPRC",
                    "BCH_JDA_DataPlatform_JDASAL", "BCH_JDA_DataPlatform_JDASKU" , "BCH_JDA_DataPlatform_JDASTK",
                    "BCH_JDA_DataPlatform_JDATRN", "BCH_JDA_DataPlatform_MGRBVD" , "BCH_JDA_DataPlatform_MGROBS1",
                    "BCH_JDA_DataPlatform_MSTTYP", "BCH_JDA_DataPlatform_TBLBOK" , "BCH_JDA_DataPlatform_MSTBOK",
                    "BCH_JDA_DataPlatform_MSTISB", "BCH_JDA_DataPlatform_RPLPRF" , "BCH_JDA_DataPlatform_RPLPRM",
                    "BCH_JDA_DataPlatform_RPLSDT", "BCH_JDA_DataPlatform_RPLSHD" , "BCH_JDA_DataPlatform_SETDTL",
                    "BCH_JDA_DataPlatform_TAXTRT", "BCH_JDA_DataPlatform_TBLCNT" , "BCH_JDA_DataPlatform_TBLCOL",
                    "BCH_JDA_DataPlatform_TBLDIS", "BCH_JDA_DataPlatform_TBLDST" , "BCH_JDA_DataPlatform_TBLFIN",
                    "BCH_JDA_DataPlatform_TBLFLD", "BCH_JDA_DataPlatform_TBLPRV" , "BCH_JDA_DataPlatform_TBLREG",
                    "BCH_JDA_DataPlatform_TBLSIZ", "BCH_JDA_DataPlatform_TBLSTR"
                ]
}

MAIN_ROOT   = "B2S" # ODP or B2S
MAIN_FOLDER = "JDA" # JDA or POS

folder_name = ALL_FOLDERS.get(MAIN_ROOT+"/"+MAIN_FOLDER)
print(folder_name)