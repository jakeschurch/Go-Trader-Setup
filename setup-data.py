#!/usr/bin/env python3
import gzip
import os
import dask.dataframe as dd
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
    print(len(quote_labels))
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

    if file.endswith('.gz') is False and ('.') not in file:
        # print(f'Unzipping file {i} out of {len(os.listdir(input_path))}')
        # unzip_cmd = f'pigz -v -dp 1 {file}'
        # os.system(unzip_cmd)

        df = dd.read_csv(unzipped_filename, sep="\n", delimiter="|", skiprows=4,
                         names=PreprocessQuoteLabels(), assume_missing=False,
                         usecols=PreprocessQuoteLabels(),
                         dtype={'Time': 'str', 'Participant_Timestamp': 'str',
                                'Best_Bid_FINRA_Market_Maker_ID': 'str',
                                'FINRA_ADF_Timestamp': 'str',
                                'FINRA_BBO_Indicator': 'float64',
                                'NBBO_Quote_Condition': 'object',
                                'SIP_Generated_Message_Identifier': 'object'})

        # Drop columns that are generally blank/non-meaningful
        df.drop(['FINRA_ADF_Timestamp', 'LULD_Indicator',
                 'LULD_NBBO_Indicator', 'SIP_Generated_Message_Identifier',
                 'Best_Offer_FINRA_Market_Maker_ID'], inplace=True)



        # hdf.put(hdf5_out, df)
        break
i += 1

hdf.close()
