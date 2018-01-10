#!/usr/bin/env python3
import os
import dask.dataframe as dd
import h5py
import fastparquet

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

tickersWanted = ['AAPL', "GOOG", "SPY", "CSCO"]

toRemove =[
    'FINRA_ADF_Timestamp', 'LULD_Indicator', 'LULD_NBBO_Indicator',
    'SIP_Generated_Message_Identifier',
    'Best_Offer_FINRA_Market_Maker_ID',
    'Best_Bid_FINRA_Market_Maker_ID', 'NBBO_Quote_Condition',
    'FINRA_ADF_MPID_Indicator', 'Quote_Cancel_Correction'
]

def ReducedLabels():
    out = [x for x in PreprocessQuoteLabels() if x not in toRemove]
    return out

def GetTickers():
    os.chdir('/home/jake/Desktop/')
    f = fastparquet.ParquetFile('testParquet/')
    df = f.to_pandas(['Symbol']).unique()
    print(df)


input_path = '/media/jake/taq_data/taq/quotes'
output_folder = '/home/jake/Desktop/'
output_file = 'taqData.hdf5'
output_path = os.path.join(output_folder)
os.chdir(input_path)

for filename in os.listdir(input_path):
    unzipped_file = filename.strip('.gz')
    out_name = unzipped_file.strip('EQY_US_ALL_NBBO')

    if ('.') not in filename:
        # print(f'Unzipping file {i} out of {len(os.listdir(input_path))}')
        # unzip_cmd = f'pigz -v -dp 1 {file}'
        # os.system(unzip_cmd)

        quotes_day_df = dd.read_csv(
            unzipped_file,
            delimiter="|",
            lineterminator="\n",
            skiprows=4,
            names=PreprocessQuoteLabels(),
            usecols=ReducedLabels(),
            assume_missing=True,
            dtype={
                'Time': 'str',
                'Participant_Timestamp': 'str',
                'Best_Bid_FINRA_Market_Maker_ID': 'str',
                'LULD_Indicator':'object',
                'FINRA_ADF_Timestamp': 'str',
                'FINRA_BBO_Indicator': 'float64',
                'NBBO_Quote_Condition': 'object',
                'SIP_Generated_Message_Identifier': 'object'
            })


        fname = '/home/jake/Desktop/testParquet'
        dd.to_parquet(quotes_day_df, fname, compression='GZIP')


        break
        quotes_day_df.to_hdf('/home/jake/Desktop/taqData.hdf5',
                             f'{hdf5_out}', complevel=4, append=True, mode='w')
        # Output dask df to dask array -23 cols
        print(f'{hdf5_out} has completed downloading.')

if __name__ == '__main__':
    GetTickers()
