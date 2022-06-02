data = {
    "central-cto-ofm-data-hub-dev.data_vs_ctrl_files.jda_b2s": [0,19],
    "central-cto-ofm-data-hub-dev.data_vs_ctrl_files.jda_ofm": [0,19],

    "central-cto-ofm-data-hub-prod.ofm_transport_reporting_views.fact_transport_daily": [1,62],
    "central-cto-ofm-data-hub-prod.market_basket_analysis.DIM_PRODUCT_ATTRIBUTE"   : [1,2],
    "central-cto-ofm-data-hub-prod.market_basket_analysis.DIM_PRODUCT_ATTRIBUTE_v2": [1,3]
}

for table in data.keys():
    date_start, date_end = data.get(table)
    for value in range(date_start, date_end):
        value = f"{value:02d}"
        print(value, end = ' ')
    print()
