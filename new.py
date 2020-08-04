def get_cdm_columns(output,cdm_table,source):
    #print(cdm_table)
    d = dt.now()
    tmstmp = d.strftime("%Y%m%d%H%M%S")
    prp_cdm_mapping_v10_pq = dataiku.Dataset(cdm_mapping)
    prp_cdm_mapping_v10_pq = dkuspark.get_dataframe(sqlContext, prp_cdm_mapping_v10_pq)
    outfilter1 = "cdmtable = '"+cdm_table+"'"
    #outfilter2 = "sourcetable like '%"+source.lower()+"%'"
    output_mapping = prp_cdm_mapping_v10_pq.filter(outfilter1).select('sourcetable','sourcecolumn','cdmdatatype','cdmcolumn','sourcenormalise','sourcevalue',prp_cdm_mapping_v10_pq.sequence_no.cast('float'))
    #output_mapping.show()
    #output_mapping = output_mapping.filter(outfilter2).select('sourcecolumn','cdmdatatype','cdmcolumn').orderBy('sequence_no')
    output_mapping = output_mapping.select('sourcecolumn','cdmdatatype','cdmcolumn','sequence_no','sourcenormalise','sourcevalue').sort('sequence_no', ascending=True)
    #output_mapping.show()
    a = output_mapping.rdd.map(lambda x: x).collect()
    col_list=[]
    for i in a:
        if(i["sourcenormalise"]=='Y'):
            col_list.append(i["sourcevalue"].replace(u'\xa0', u'')+" as "+i["cdmcolumn"].replace(u'\xa0', u''))
        if(i["sourcecolumn"] is not None and i["sourcenormalise"]=='N' ):
            col_list.append("cast("+i["sourcecolumn"].replace(u'\xa0', u'')+" as "+i["cdmdatatype"].replace(u'\xa0', u'')+") as "+i["cdmcolumn"].replace(u'\xa0', u''))
        #if(i["cdmcolumn"] in ["loc_id"]):
            #col_list.append("cast("+i["cdmcolumn"].replace(u'\xa0', u'')+" as "+i["cdmdatatype"].replace(u'\xa0', u'')+") as "+i["cdmcolumn"].replace(u'\xa0', u''))
        if(i["sourcecolumn"] is None and i["cdmcolumn"] is not None and i["sourcenormalise"]=='N'):
            col_list.append("NULL as "+i["cdmcolumn"].replace(u'\xa0', u''))
    col_list.append("'"+source+"_"+tmstmp+"' as load_id")
    #print(col_list)
    final_output = output.selectExpr(col_list)
    #final_output = final_output.withColumn('load_id', lit(source+"_"+tmstmp+"'"))
    return final_output