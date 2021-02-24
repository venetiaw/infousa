"""Dec 3, 2020
Script to obtain county names from zip codes
Note: uszipcode library is not available on cluster - recommended to install and run on local instead

VARIABLES TO REPLACE:
- <String> MATCHFILE: file name where the match will be stored. The first col of the csv will be the zip code and the second will be the county name
- <String> INFOUSA_IN: infousa dataset file name
- <String> INFOUSA_OUT: merged infousa file name (to include county name column)
- <bool> SAVE_MAP: True to save the intermediate match file (MATCHFILE)
- <String> FLOWSOUT: file name to save reshaped file (to compare flows)
"""

from uszipcode import SearchEngine
import pandas as pd

def gen_match(zips, output_file, save_map=True):
    """ Returns a dataframe where each row contains the corresponding county name for a zip code
    (Can also save csv file with save_map=True)
    PARAMS:
    - <list> zips: list of zip codes
    - <String> output_file: file name to save the file mapping zip code to county
    - <bool> save_map: True to save match file
    """
    search = SearchEngine(simple_zipcode=True)
    counties = list(map(lambda x: search.by_zipcode(str(x)).values()[5], zips))
    df = pd.DataFrame(zips, counties).reset_index()
    df.columns = ['COUNTY_NAME', 'ZIP']
    if save_map:
        df.to_csv(output_file, index=False)
    return df


def extend_infousa(infousa, merged_name, zips, output_file, save_map=True):
    """ Returns the dataframe with a COUNTY_NAME column. Also saves csv of infousa file with the additional county column
    PARAMS:
    - <DataFrame> infousa: DataFrame object of infousa files
    - <String> merged_name: file name for csv output
    - <list> zips: list of zip codes
    - <String> output_file: file name to save the file mapping zip code to county
    """
    merged = infousa.merge(gen_match(zips, output_file, save_map), on=['ZIP'], how='left')
    merged.to_csv(merged_name, sep='\t', index=False)
    return merged


def myPivot(extended_infousa):
    """Runs the pivot function such that each row corresponds to a FAMILYID and the columns correspond to the counties resided across years
    PARAMS:
    - <DataFrame> extended_infousa: dataframe object of infousa data with the COUNTY_NAME column
    """
    return extended_infousa.pivot(values='COUNTY_NAME', index='FAMILYID', columns='YEAR')


def get_flows():
    """Calculate county to county flows between YEAR1 and YEAR2 from the reshaped dataset
    """
    #### CHANGE VALUES ACCORDINGLY ####
    RESHAPED_FILE = 'nc_reshaped.txt' #reshaped file (each column is a household's county)
    CENSUS_FILE = '' #county flows file from census, but contains only the 2 county columns, the estimated flows from county A to B, and margin of error (MOE) 
    YEAR1 = 2012 #lower year
    YEAR2 = 2016 #higher year
    ##################################
    df = pd.read_csv(RESHAPED_FILE, sep='\t')
    infousa = df.groupby([str(YEAR1), str(YEAR2)]).size().reset_index()
    infousa.columns=['A', 'B', 'N']
    census = pd.read_excel(CENSUS_FILE)
    census.columns=['A', 'B', 'Estimate', 'MOE']
    merged = census.merge(infousa, on=['A', 'B'], how='left')
    merged['N']=merged['N'].fillna(0)
    merged['N']=merged['N']*2.48
    merged['UPPER']= merged['Estimate'] + merged['MOE']
    merged['LOWER']= merged['Estimate']- merged['MOE']
    print('See the first number in the following tuples:')
    print('Number of flows in census: ' + str(merged.shape)) 
    print('Number of flows that fall within confidence interval: ' + str(merged[(merged['N'] <= merged['UPPER']) & (merged['N'] >= merged['LOWER'])].shape))
    

if __name__== "__main__":
    #### SET PARAMS ####
    MATCHFILE = 'empty_matchfile.txt'  #replace with desired csv file name
    INFOUSA_IN = 'empty.txt' #replace with infousa file name
    INFOUSA_OUT = 'empty_out.txt' # replace with name for infousa with additional county name column
    SAVE_MAP = False # set to True if you want to save the match file
    FLOWSOUT = 'reshaped.txt'
    ####################
    infousa = pd.read_csv(INFOUSA_IN, sep = '\t')
    zipcodes = list(infousa['ZIP'].unique())
    matching = gen_match(zipcodes, MATCHFILE, SAVE_MAP)
    myExtended = extend_infousa(infousa, INFOUSA_OUT, zipcodes, MATCHFILE, SAVE_MAP)
    reshaped_flows = myPivot(myExtended)
    reshaped_flows.to_csv(FLOWSOUT, sep='\t')
