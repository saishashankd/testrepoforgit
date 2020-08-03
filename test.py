# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
src_rs_pfi_laad21_eucrisa_patd_hdfs_pq = dataiku.Dataset("src_rs_pfi_laad21_eucrisa_patd_hdfs_pq")
src_rs_pfi_laad21_eucrisa_patd_hdfs_pq_df = src_rs_pfi_laad21_eucrisa_patd_hdfs_pq.get_dataframe()


# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a Pandas dataframe
# NB: DSS also supports other kinds of APIs for reading and writing data. Please see doc.

test_output_df = src_rs_pfi_laad21_eucrisa_patd_hdfs_pq_df # For this sample code, simply copy input to output
#t "this is test"

# Write recipe outputs
test_output = dataiku.Dataset("test_output")
test_output.write_with_schema(test_output_df)