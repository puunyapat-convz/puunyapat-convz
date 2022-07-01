output = []

# pos_txn_02 = [ 
#   "POS_DataPlatform_Txn_DiscountCoupon",
#   "POS_DataPlatform_Txn_Installment",
#   "POS_DataPlatform_Txn_Translator"
# ]

ofm_intraday = [
  "OFM_Podetl",
  "OFM_Pohead",
  "OFM_SoDndetl",
  "OFM_Soivdetl",
  "OFM_Soivhead",
  "OFM_TBProductControlStatic",
  "OFM_TBProductMaster",
  "OFM_TBProductPrintingMaster",
  "OFM_TBProductStatic",
  "OFM_TBProductUnit",
  "OFM_TBProductWarehouse",
  "OFM_TBSOReceiptStore",
  "OFM_TdSystem",
  "OFM_sodnhead",
  "OFM_tballocate_stock_bychannel",
  "OFM_tbeo_detail",
  "OFM_tbeo_head",
  "OFM_tbso_detail",
  "OFM_tbso_head",
  "OFM_tbsopricebydate",
  "OFM_tbteam_master",
  "OFM_tbuser_master"
]

print(len(ofm_intraday))

for table in ofm_intraday:
    output.append(f'["central-cto-ofm-data-hub-prod.pos_b2s_daily_source.{table}","central-cto-ofm-data-hub-dev.b2s_pos_prod.{table}"],')

for item in output:
    print(item)
