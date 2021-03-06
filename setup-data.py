#!/usr/bin/env python3
import os
import dask.dataframe as dd
import dask.array as da
import pandas as pd


def findEOFLineNumber(filename):
    with open(filename) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def PreprocessQuoteLabels():
    quote_labels = 'Time|Exchange|Symbol|Bid_Price|Bid_Size|Offer_Price|' \
    'Offer_Size|Quote_Condition|Sequence_Number|National_BBO_Ind|' \
    'FINRA_BBO_Indicator|FINRA_ADF_MPID_Indicator|Quote_Cancel_Correction|'\
    'Source_Of_Quote|NBBO_Quote_Condition|Best_Bid_Exchange|Best_Bid_Price|'\
    'Best_Bid_Size|Best_Bid_FINRA_Market_Maker_ID|Best_Offer_Exchange|'\
    'Best_Offer_Price|Best_Offer_Size|Best_Offer_FINRA_Market_Maker_ID|'\
    'LULD_Indicator|LULD_NBBO_Indicator|SIP_Generated_Message_Identifier|'\
    'Participant_Timestamp|FINRA_ADF_Timestamp'

    quote_labels = quote_labels.split('|')
    return quote_labels


input_path = '/media/jake/taq_data/taq/quotes'
output_folder = '/home/jake/Desktop/'
output_file = 'taqData.h5'
output_path = os.path.join(output_folder)
os.chdir(input_path)

# Setup HDF5 Output
hdf_group = 'quotes/'
if os.path.exists(os.path.join(output_path, output_file)) is not True:
    os.chdir(output_path)

hdf = pd.HDFStore(output_file)

os.chdir(input_path)
# Unzip Gzip file
i = 0
for filename in os.listdir(input_path):
    unzipped_file = filename.strip('.gz')
    out_name = 'QUOTES' + unzipped_file.strip('EQY_US_ALL_NBBO')
    hdf5_out = hdf_group + unzipped_file

    if filename.endswith('.gz') is False and ('.') not in filename:
        # print(f'Unzipping file {i} out of {len(os.listdir(input_path))}')
        # unzip_cmd = f'pigz -v -dp 1 {file}'
        # os.system(unzip_cmd)

        quotes_day_df = dd.read_csv(
            unzipped_file,
            sep="\n",
            delimiter="|",
            skiprows=4,
            names=PreprocessQuoteLabels(),
            usecols=PreprocessQuoteLabels(),
            assume_missing=True,
            dtype={
                'Time': 'str',
                'Participant_Timestamp': 'str',
                'Best_Bid_FINRA_Market_Maker_ID': 'str',
                'FINRA_ADF_Timestamp': 'str',
                'FINRA_BBO_Indicator': 'float64',
                'NBBO_Quote_Condition': 'object',
                'SIP_Generated_Message_Identifier': 'object'
            })
        # Drop columns that are generally blank/non-meaningful
        quotes_day_df = quotes_day_df.drop(
            [
                'FINRA_ADF_Timestamp', 'LULD_Indicator', 'LULD_NBBO_Indicator',
                'SIP_Generated_Message_Identifier',
                'Best_Offer_FINRA_Market_Maker_ID'
            ],
            axis=1)
        quotes_day_df.to_hdf('/home/jake/Desktop/taqData.h5',
                             f'/quotes/{hdf5_out}', complevel=4)
        # Output dask df to dask array -23 cols
        # quotes_day_array = quotes_day_df.to_records()
        # print(quotes_day_array.shape)

i += 1

hdf.close()
