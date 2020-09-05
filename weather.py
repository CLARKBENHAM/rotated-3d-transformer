import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import random
import timeit
import math
import re
import requests
import pickle
import os
from datetime import datetime, date, timedelta
import time
import pdb

abs_path = "C:\\Users\\student.DESKTOP-UT02KBN\\Desktop\\electric_price_preds\\data"

def save_struct(struct, name):
    "handler to pickle data"
    d = os.getcwd()
    os.chdir(abs_path)
    if isinstance(struct, pd.DataFrame) or isinstance(struct, pd.Series):
        struct.to_pickle(f'{name}.p')
    else:
        with open(f'{name}.p', 'wb') as file:
            pickle.dump(struct,  file)
    os.chdir(d)

def load_struct(name):
    d = os.getcwd()
    os.chdir(abs_path)
    try:
        with open(f'{name}.p', 'rb') as file:
            return pickle.load(file)
    except:
        return pd.read_pickle(f'{name}.p')
    os.chdir(d)

def reprocess_struct(name):
    "Deletes pickled data so data_handler will reload it"
    d = os.getcwd()
    os.chdir(abs_path)
    try:
        os.remove(f"{name}.p")
    except Exception as e:
        print(f"{name} alread removed", e)
    try:
        exec(f"del {name}")
    except:
        pass
    data_handler(data_structs = [name])
    os.chdir(d)
        
#%%
weather_token = "XUxckTkzjdLZvkPvtIpjVwRSawSPGETi"
baseurl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/'
today_dt = datetime.strftime(datetime.now(), "%Y-%m-%d")#gets date from yesturday to today, yyyy-mm-dd
yester_dt = datetime.strftime(datetime.now() - timedelta(days=4), "%Y-%m-%d")
todaystr = 'enddate=' + today_dt
yesterstr = 'startdate=' + yester_dt
headers = {'token': 'XUxckTkzjdLZvkPvtIpjVwRSawSPGETi'}
#stationid=COOP:310090
default_lmt_sz = 25
max_lmt_sz = 1000

def make_request(requrl):
    "Makes request w/ requrl added to end of url"
    if requrl[:5] != 'https':
        requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/' + requrl
    assert(requrl[:41] == 'https://www.ncdc.noaa.gov/cdo-web/api/v2/')
# &includeStationLocation=1
    return requests.get(requrl, headers = {'token': weather_token})

    
def explore_reqs(requrl, dates=False, limit=100):
    if limit != None and 'limit' not in requrl:
        requrl += f"?limit={limit}"
    if dates and 'date' not in requrl:
        requrl += f"&{yesterstr}&{todaystr}"
    out = make_request(requrl)
    try:
        return out.json()
    except Exception as e:
        print(e, out.status_code, out.content)
        return None

# datacategories = explore_reqs("datacategories", dates=False).json()

datacategories 
#%%
def write_info(completed_req, filename):
    "takes a request or a df and writes the info to a file"
    if isinstance(completed_req, pd.DataFrame):
        js_df = completed_req
    else:
        js_df = pd.DataFrame.from_dict(completed_req.json()['results'])
    f = open(filename, 'a')#append mode
    f.write("############################# \n===" + info + '===\n')
    f.write(js_df.to_csv(sep=',', index = False))
    f.write("##################################\n")
    f.close()


def write_pickle_file(filename, data, index = None):
    "both writes and pickles data, filename is in current directory"
    filename1 = r"Desktop\side_projects\\" + filename + '.p'
    with open(filename1, 'wb') as filehandler:
        pickle.dump(data, filehandler)
    
    if isinstance(data, pd.DataFrame):
        print(f"WARNING: the index in the csv is: {index}")
        data.to_csv(r'Desktop\side_projects\\' + filename + '.txt', index = index)
    else:
        f = open(r'Desktop\side_projects\\' + filename + '.txt', 'w')
        try:
            f.write(data)
        except:
            f.write(str(data))
        finally:
            f.close()

def get_date_stat_val(req):
    "given a request returns list of date, station, and value"
    adate = [datetime.strptime(i['date'], "%Y-%m-%dT00:00:00") for i in req.json()['results']]
    astation= [i['station'] for i in req.json()['results']]
    aval = pd.to_numeric([i['value'] for i in req.json()['results']])
    return adate, astation, aval
    # tup_dta = [(datetime.strptime(i['date'], "%Y-%m-%dT00:00:00"),
    #             i['station'],
    #             i['value'])
    #             for i in req.json()['results']]
    # return list(zip(tup_data))

def iter_thru_req(requrl, maxresults = 365*835, index_name = None, col_names = None ):
    """"gets all count values in requrl, returns a dataframe with those values
    index_name: the name of colum to use as index
    col_names: the names of the columns to keep from what's returned
    returns request results on index, columns are data categorizes
    """
    if requrl[:5] != 'https':
        requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/' + requrl
    assert(requrl[:41] == 'https://www.ncdc.noaa.gov/cdo-web/api/v2/')
    assert('offset' not in requrl)
    
    requrl = re.sub("limit=\d+", "limit=1", requrl)
    first_js = requests.get(requrl, headers = {'token': weather_token}).json()
    requrl = re.sub("limit=\d+", "limit=1000", requrl)
    
    if col_names is None:
        col_names = list(first_js['results'][0].keys())
    else:
        cols = [i for i in col_names if i in first_js['results'][0].keys()]
        assert cols == col_names, f"""the following cols weren't included,
                                    {[i for i in col_names if i not in cols]}"""
    if index_name is not None:
        assert index_name in col_names, f"""col for Index not in {col_names}"""
    
    size = min(first_js['metadata']['resultset']['count'], maxresults)
    offset = 1
    incremented_by = 0
    data = []
    while offset + incremented_by < size:
        js = requests.get(requrl, headers = {'token': weather_token}).json()
        data += js['results']
        
        offset = js['metadata']['resultset']['offset']
        try:
            incremented_by = js['metadata']['resultset']['limit']
        except:
            incremented_by = default_lmt_sz
        offset += incremented_by
        new_limit = min(1000, size - offset)#maximium value 
        
        if 'limit' in requrl:
            requrl = re.sub("limit=\d+", f"limit={new_limit}", requrl)
        else:
            requrl += '&limit=' + str(new_limit)
        if 'offset' in requrl:
            requrl = re.sub("offset=\d+", f"offset={offset}", requrl)
        else:
            requrl += '&offset=' + str(offset)
            
   
    out = pd.DataFrame(data, columns = col_names)
    for c in out.columns:
        if 'date' in c:
            out.loc[:,c] = pd.to_datetime(out.loc[:,c], format="%Y-%m-%d")
    if index_name is not None:
        out = out.set_index(index_name)
    return out, frst_req

requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?limit=1000' + '&' + yesterstr + '&' + todaystr + '&locationid=FIPS:37'
# station_df, req = iter_thru_req(requrl, maxresults = 1000)#currently 577 WA stations 
#station_df.to_csv(r'Desktop\side_projects\station_latlon.txt', index = None)
#save_struct(station_df, 'stations')

##%%
#requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?limit=1000' + '&' + yesterstr + '&' + todaystr + '&locationid=FIPS:37'
#a = make_request(requrl)
#wa_station_ids = [i['id'] for i in a.json()['results']]

#%%
#datatypes you want
f = open('Desktop\side_projects\search_catagories.txt','r')
search_catagories_txt = f.read()
indx = search_catagories_txt.rfind('===datatypes===') + len('===datatypes===\n')
from pandas.compat import StringIO
search_cat_df = pd.read_csv(StringIO(search_catagories_txt[indx:]), sep=',').dropna()[:-1]

datatype_names = search_cat_df.loc[search_cat_df['name'].apply(\
                  lambda rw: not re.search(r'wind', rw, re.IGNORECASE) is None), 'id'].values.astype('str')

datatype_reqs = [0]*len(datatype_names)
datatypes_named_wind = pd.Series(index = datatype_names, dtype = 'str')
for i, name in enumerate(datatype_names):
    req = make_request(f'datasets?datatypeid={name}')
    datatype_reqs[i] = req
    try:
        datatypes_named_wind[name] = req.json()['results'][0]['id']
    except:
        datatypes_named_wind[name] = None
print(datatypes_named_wind)
datatypes_named_wind = datatypes_named_wind.dropna()
#These are the datatypes that have 'wind' in their description
#%%
#These are the data_types that are in the wind category; all of them are in datatypes named wind
#this can be ignored
req2 = make_request('datatypes?datacategoryid=WIND&limit=1000')
wind_datatypes = pd.DataFrame(data = {'name': [i['name'] for i in req2.json()['results']], 
                                               'id': [i['id'] for i in req2.json()['results']]})
#diff = [i for i in datatypes_named_wind.index if i not in wind_datatypes['id'].values]
#print([i for i in search_cat_df.loc[search_cat_df.loc[:,'id'].apply(lambda nm: nm in diff), 'name']])
#only other interesting one is WT11, 'High or Damaging winds' not in WIND data categorys
wind_datatypes = wind_datatypes.append({'name':'High or damaging winds', 'id':'WT11'}, ignore_index = True)

#all of the values only in wind_datatypes are none
wind_dataset = pd.Series(index = [i for i in wind_datatypes['id'] if i not in datatypes_named_wind])
wind_reqs = [0]*wind_dataset.size
for i, name in enumerate(wind_dataset.values):
    req = make_request(f'datasets?datatypeid={name}')
    wind_reqs[i] = req
    try:
        wind_dataset[name] = req.json()['results'][0]['id']
    except:
        wind_dataset[name] = None
    time.sleep(0.21)#can't make more than 5 requests per second
#%%
def get_datatype_description(given_ix):
    "gets the datatype descriptions of given a set of datatype IDs as an index\
    It returns a dataframe"
    b = [list(re.findall("[^,]*,([^,]+),[^,]+,[^,]+,(.*)", j)[0])\
            for j in search_catagories_txt[indx+37:-35].split("\n")]
    ids,des = zip(*b)
    d = pd.DataFrame(pd.Series(des, index = ids), columns = ['Description'])
    both_idx = given_ix.intersection(d.index)
    return d.loc[both_idx]

datatypes_named_wind['Description'] = get_datatype_description(datatypes_named_wind.index)
good_datatypes = pd.DataFrame(
        {"Dataset": datatypes_named_wind, 
         "Description": get_datatype_description(datatypes_named_wind.index).Description},
         index = datatypes_named_wind.index)
good_datatypes.drop('Description', inplace = True)
write_pickle_file("wind_datasets", good_datatypes, index = True)


#%%
#gets all the current weather stations in WA
req_wa_stat = make_request('stations?locationid=FIPS:53&startdate=2016-01-01&limit=1000')
e = [[i['id'], i['name']] for i in req_wa_stat.json()['results']]
all_stations_in_wa = pd.Series(list(zip(*e))[1], index = list(zip(*e))[0], dtype='str')

#%%trying to actually pull the data

end_datetime = datetime.strptime("2019-06-14", "%Y-%m-%d")
end_date = end_datetime.strftime("%Y-%m-%d")
start_date = (end_datetime - timedelta(days=364)).strftime("%Y-%m-%d")
data_type = my_datatypes[0]
date_ix = pd.DatetimeIndex([end_datetime - timedelta(x) for x in range(364,-1,-1)])

all_req_dict = {i:[0] for i in good_datatypes.index}
for data_type in good_datatypes.index:
    print(data_type)
    dataset_id = good_datatypes.loc[data_type,'Dataset']#Am I sure I got the dataset right?
    data_url = str(f"https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid={dataset_id}&datatypeid={data_type}&locationid=FIPS:53&startdate={start_date}&enddate={end_date}&limit=1000")
    print(data_url)
    all_req_dict[data_type], frst_req = iter_thru_req(data_url, 
                                                maxresults=365*835, 
                                                index =date_ix, 
                                                columns = all_stations_in_wa.index)
#    print(data_url, "\n\n", all_req_dict[data_type])
    try:#am getting weird errors with empty data being returned?
        print(frst_req.json()['metadata'], "\n\n\n\n\n\n\n\n\n##############")
    except:
        pass
    if all_req_dict[data_type].isna().sum().sum() != 365*835:
        write_pickle_file(str(f"api_data\{str(data_type)}"), all_req_dict[data_type], index = True)
#write_pickle_file("pulled_data", [i for i in all_req_dict.values()])
print(all_req_dict)
x_values = pd.concat(all_req_dict.values)




#%%
#Don't Know what velocity datatype is
req3 = make_request('datatypes?datacategoryid=VELOCITY&limit=1000')
#req3.json()
vel_datatypes = pd.DataFrame(data = {'name': [i['name'] for i in req3.json()['results']], 
                                               'id': [i['id'] for i in req3.json()['results']]})



#%%
#convert station_df to actual types#grib
wa_stations = station_df.loc[:,['id', 'fips']]
wa_stations = wa_stations.dropna()
wa_stations = wa_stations.loc[wa_stations['fips'].apply(lambda rw: rw[:2] == '53'), :]
stat_id = wa_stations.iloc[0,0]
stat_id2 = wa_stations.iloc[1,0]
stat_id3 = wa_stations.iloc[2,0]

#do all stations have the same amount of info?
#wa_station_dict = {i:[0] for i in wa_stations}

#wa_weather_df = pd.DataFrame(index =ex_indx, columns = wa_stations)
#%%
#station_latlon =station_df.loc[:,['latitude','longitude']]
#station_df.to_csv(r'Desktop\side_projects\station_latlon.txt', index = None)

#frst_req = requests.get(requrl, headers = {'token': weather_token})
#frst_req = requests.get('https://www.ncdc.noaat.gov/cdo-web/api/v2/locationcategories?limit=5', headers = {'token': weather_token})
#iter_thru_req('https://www.ncdc.noaa.gov/cdo-web/api/v2/datasets')
        #%%
#need to map COOP to NOAA?
        #using the fcc API
url = "https://geo.fcc.gov/api/census/block/find?latitude=49.259&longitude=-122.8591&showall=false&format=json"
def fcc_fips_api(lat,lon):
    url = "https://geo.fcc.gov/api/census/block/find?latitude=" +\
    str(lat)+ "&longitude=" + str(lon)+ "&showall=false&format=json"#can't usefstrings for py2
    js =requests.get(url).json()
    return js['Block']['FIPS']

#below didn't work
def fcc_api(lat,lon):
    "takes FCC API calls to pd.Series"
    if isinstance(lat, float):
        url = "https://geo.fcc.gov/api/census/block/find?latitude=" +\
        str(lat)+ "&longitude=" + str(lon)+ "&showall=false&format=json"#can't usefstrings for py2
        js = requests.get(url).json()
        return pd.io.json.json_normalize(js).iloc[0]
    elif isinstance(lat, pd.Series):
        latlon_df = pd.DataFrame(index = range(lat.shape[0]), columns = ['datacoverage', 'elevation', 'elevationUnit', 'id', 'latitude',
       'longitude', 'maxdate', 'mindate', 'name'])
        for i in range(lat.shape[0]):
            latlon_df.iloc[i,:] = fcc_api(lat.iloc[i], lon.iloc[i])
        return latlon_df
    else:
        print("an Error")
fcc_df = fcc_api(station_df['latitude'], station_df['longitude'])#allnan?
write_pickle_file("fcc_df", fcc_df)
#[(j,k) for i in zip(tzt_df.loc[:,['latitude', 'longitude']].values) for j,k in i]
#fips = [fcc_fips_api(lat,lon) for lat,lon in zip(station_df['latitude'], station_df['longitude'])]



#%%
#%%
#getting what I think are the most relavent datatypes
my_datatypes = list(good_datatypes.drop(good_datatypes.index[2:24]).dropna().index)
my_datatypes = my_datatypes[:3] + my_datatypes[4:]
for i in my_datatypes:
    print(f"{i}: {good_datatypes.loc[i, 'Description']}")


#%%    
r = requests.get(url).json()
print(r['Block']['bbox'],
r['Block']['FIPS'],
r['County']['FIPS'],
r['County']['name'],
r['State']['FIPS'],
r['State']['code'],
r['State']['name'],
r['status'],
r['executionTime'])



#%%

