"""Feb 24, 2021
Contains functions required to check infousa county flows

"""
import pandas as pd

def get_uniqueZIP(df):
    """Returns list of unique zip codes in the dataframe in the column df['ZIP']
    PARAMS:
    - <DataFrame> df: infousa dataframe
    RETURNS: list
    """
    return df.ZIP.unique().tolist()


def gen_match(zips, output_file=None):
    """Returns a dataframe where each row contains the corresponding county name for a zip code
    (Can also save csv file with save_map=True)
    PARAMS:
    - <list> zips: list of zip codes
    - <String> output_file: file name to save the file mapping zip code to county
    RETURNS: DataFrame
    """
    from uszipcode import SearchEngine
    search = SearchEngine(simple_zipcode=True)
    counties = list(map(lambda x: search.by_zipcode(str(x)).values()[5], zips))
    df = pd.DataFrame(zips, counties).reset_index()
    df.columns = ['COUNTY_NAME', 'ZIP']
    if output_file != None:
        df.to_csv(output_file, index=False)
    return df


def extend_infousa(countyMapping, infousa, merged_name=None):
    """Returns infousa dataframe with additional 'COUNTY_NAME' column
    PARAMS:
    - <DataFrame> countyMapping: dataframe with 'COUNTY_NAME' and 'ZIP' columns (generated from gen_match)
    - <DataFrame> infousa: infousa dataframe
    - <String> merged_name: filename to save extended infousa dataframe
    RETURNS: DataFrame
    """
    merged = infousa.merge(countyMapping, on=['ZIP'], how='left')
    merged['COUNTY_STATE'] = merged['COUNTY_NAME'] + "_" + merged['STATE']
    if merged_name != None:
        merged.to_csv(merged_name, sep='\t', index=False)
    return merged


def myPivot(extended_infousa):
    """Returns dataframe (long) where each row corresponds to a FAMILYID and the columns correspond to the counties resided across years
    PARAMS:
    - <DataFrame> extended_infousa: dataframe object of infousa data with the COUNTY_NAME column
    RETURNS: DataFrame
    """
    return extended_infousa.pivot(values='COUNTY_STATE', index='FAMILYID', columns='YEAR')


def read_censusFlows(fileName, stateNames):
    """
    PARAMS:
    - <String> fileName: name of .xlsx census flows, e.g., 'county-to-county-2014-2018-ins-outs-nets-gross.xlsx'
    - <String[]> list of strings: name of states whose flows are being considered, e.g., ['North Carolina', 'Illinois', 'Michigan']
    RETURNS: DataFrame
    """
    d = {'Alabama': 'AL','Alaska': 'AK','Arizona': 'AZ','Arkansas': 'AR','California': 'CA','Colorado': 'CO','Connecticut': 'CT','Delaware': 'DE','Florida': 'FL','Georgia': 'GA','Hawaii': 'HI','Idaho': 'ID','Illinois': 'IL','Indiana': 'IN','Iowa': 'IA','Kansas': 'KS','Kentucky': 'KY','Louisiana': 'LA','Maine': 'ME','Maryland': 'MD','Massachusetts': 'MA','Michigan': 'MI','Minnesota': 'MN','Mississippi': 'MS','Missouri': 'MO','Montana': 'MT','Nebraska': 'NE','Nevada': 'NV','New Hampshire': 'NH','New Jersey': 'NJ','New Mexico': 'NM','New York': 'NY','North Carolina': 'NC','North Dakota': 'ND','Ohio': 'OH','Oklahoma': 'OK','Oregon': 'OR','Pennsylvania': 'PA','Rhode Island': 'RI','South Carolina': 'SC','South Dakota': 'SD','Tennessee': 'TN','Texas': 'TX','Utah': 'UT','Vermont': 'VT','Virginia': 'VA','Washington': 'WA','West Virginia': 'WV','Wisconsin': 'WI','Wyoming': 'WY', 'American Samoa': 'AS', 'District of Columbia': 'DC', 'Federated States of Micronesia': 'FM', 'Guam': 'GU', 'Marshall Islands': 'MH', 'Northern Mariana Islands': 'MP', 'Palau': 'PW', 'Puerto Rico': 'PR', 'Virgin Islands': 'VI'}
    myDataFrame = pd.DataFrame()
    lst = list(map(lambda x: d[x], stateNames))
    for i in stateNames:
        df = pd.read_excel(fileName, sheet_name = i, skiprows=2, usecols=[4,5,6,7,10,11])
        df.columns = ['STATE_A', 'COUNTY_A', 'STATE_B', 'COUNTY_B', 'ESTIMATE', 'MOE']
        df = df.dropna()
        df['A'] = df['COUNTY_A'] + "_" + df['STATE_A'].map(d)
        df['B'] = df['COUNTY_B'] + "_" + df['STATE_B'].map(d)
        df = df[['A', 'B', 'ESTIMATE', "MOE"]]
        df = df[df['B'].str.contains('|'.join(lst))]
        myDataFrame = pd.concat([myDataFrame, df])
    return myDataFrame


def get_flows(infousaDF, censusDF, YEAR1, YEAR2):
    """
    PARAMS:
    - <DataFrame> infousaDF: infousa dataframe
    - <DataFrame> censusDF: census county flows dataframe
    - <int> year1, year2: flows between counties from year1 to year2
    RETURNS: DataFrame
    """
    infousa = infousaDF.groupby([str(YEAR1), str(YEAR2)]).size().reset_index()
    infousa.columns=['A', 'B', 'N']
    merged = censusDF.merge(infousa, on=['A', 'B'], how='left')
    merged['N']=merged['N'].fillna(0)
    merged['N']=merged['N']*2.48
    merged['UPPER']= merged['ESTIMATE'] + merged['MOE']
    merged['LOWER']= merged['ESTIMATE'] - merged['MOE']
    return merged









