import pandas as pd
import os
import re
import sys

#pd.set_option('display.max_columns', None)

def do_zip_filter(year):
    """
    Filters dataset for zipcode 11953 
    
    PARAMS;
    - <str/int> year: year of interest

    EXAMPLE:
    do_zip_filter('11953')
    """
    year = str(year)
    f = '../Household_Ethnicity_' + year + '.txt'
    #df = filter_zip('11953', f)
    output_file = 'zip_' + year + '.txt'
    df.to_csv(output_file, sep='\t', index=False) 

def get_NC(filename):
    to_keep = ['FAMILYID', 'LOCATION_TYPE', 'PRIMARY_FAMILY_IND', 'HOUSEHOLDSTATUS', 'HEAD_HH_AGE_CODE', 'LENGTH_OF_RESIDENCE', 'CHILDREN_IND', 'ADDRESSTYPE', 'WEALTH_FINDER_SCORE', 'FIND_DIV_1000', 'OWNER_RENTER_STATUS', 'ESTMTD_HOME_VAL_DIV_1000', 'MARITAL_STATUS','MSA2000_CODE', 'MSA2000_IDENTIFIER', 'CSA2000_CODE', 'CBSACODE', 'CBSATYPE', 'CSACODE', 'LOCATIONID',  'STATE', 'ZIP', 'ZIP4', 'VACANT', 'GE_CENSUS_LEVEL_2010', 'GE_CENSUS_STATE_2010', 'GE_CENSUS_COUNTY', 'GE_CENSUS_TRACT', 'GE_CENSUS_BG', 'GE_ALS_COUNTY_CODE_2010', 'GE_ALS_CENSUS_TRACT_2010', 'GE_ALS_CENSUS_BG_2010', 'Ethnicity_Code_1', 'Ethnicity_Code_2', 'Ethnicity_Code_3']
    #df = pd.read_csv(filename, sep='\t')
    #print(df.head())
    df = pd.read_csv(os.path.join('household_data', filename), sep='\t')
    df = df[to_keep]
    print(df.head())
    year = re.findall(r'[0-9]+', filename)[0] 
    df['YEAR'] = year
    df = df[df['STATE'] == 'NC']
    print(df.head())
    df.to_csv('NC/NC'+year+'.txt', sep='\t', index=False)


if __name__ == "__main__":
    input1 = sys.argv[1]
    get_NC(input1)
