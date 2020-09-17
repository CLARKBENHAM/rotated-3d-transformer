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
import itertools

abs_path = "C:\\Users\\student.DESKTOP-UT02KBN\\Desktop\\electric_price_preds\\data"

def save_struct(struct, name):
    "handler to pickle data, will overwrite"
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
    
def regex_load_struct(regex):
    "Returns names, data files matching regex"
    d = os.getcwd()
    os.chdir(abs_path)
    def _get_name(name):
        try:
            with open(f'{name}', 'rb') as file:
                return pickle.load(file)
        except:
            return pd.read_pickle(f'{name}')
    files = [(i, _get_name(i)) for i in os.listdir() if re.match(regex, i)]
    os.chdir(d)
    if len(files) == 0:
        return [], []
    return (list(i) for i in zip(*files))

def delete_struct(name, verbal = True):
    "Deletes pickled data"
    d = os.getcwd()
    os.chdir(abs_path)
    try:
        os.remove(f"{name}.p")
    except Exception as e:
        if verbal:
            print(f"{name} alread removed", e)
    try:
        exec(f"del {name}")
    except:
        pass
    os.chdir(d)

def reprocess_struct(name):
    "calls data_handler to reload name"
    delete_struct(name)
    data = data_handler(data_structs = [name])
    save_struct(data, name)

#%%
baseurl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/'
today_dt = datetime.strftime(datetime.now(), "%Y-%m-%d")#gets date from yesturday to today, yyyy-mm-dd
yester_dt = datetime.strftime(datetime.now() - timedelta(days=4), "%Y-%m-%d")
todaystr = 'enddate=' + today_dt
yesterstr = 'startdate=' + yester_dt
weather_token = "XUxckTkzjdLZvkPvtIpjVwRSawSPGETi"
headers = {'token': 'XUxckTkzjdLZvkPvtIpjVwRSawSPGETi'}

default_lmt_sz = 25
max_lmt_sz = 1000
max_reqs_sec = 5#NOAA caps at 5 requests per second
max_reqs_day = 10000#10k per day
temp = 0

def make_request(requrl):
    """"Makes request w/ requrl added to end of url
    in API limit is number of results returned
    offset is what ix# they start at"""
    
    if requrl[:5] != 'https':
        requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/' + requrl
    assert(requrl[:41] == 'https://www.ncdc.noaa.gov/cdo-web/api/v2/')
# &includeStationLocation=1
    return requests.get(requrl, headers = {'token': weather_token})

    
def explore_reqs(requrl, dates=False, limit=None):
    "append requests w/ '?' if it's adding additional information after the first category split (i.e. 1 word after a '/' use ?, else '&')"
    if limit is not None and 'limit' not in requrl:
        if '?' in requrl:
            requrl += f"&limit={limit}"
        else:
            requrl += f"?limit={limit}"
    if (dates or 'data?' in requrl ) and 'date' not in requrl:#dates required for data requests
        if '?' in requrl:
            requrl += f"&{yesterstr}&{todaystr}"
        else:
            requrl += f"?{yesterstr}&{todaystr}"
    out = make_request(requrl)
    # pdb.set_trace()
    try:
        return out.json()
    except Exception as e:
        print(e, out.status_code, out.content, sep="\n")
        return -1

def make_header(counts = {}):
    #need to reset every 24hrs
    tokens = ["XUxckTkzjdLZvkPvtIpjVwRSawSPGETi",
              'dPOtwdMLrJOJygiBdZYBOhHvmsAwTbXD']
    counts = {i:counts[i] if i in counts else 0 for i in tokens}
    current = tokens[0]
    last_used = {i:time.time() for i in tokens}
    token_iter = itertools.cycle(tokens)
    while True:
        if counts[current] >= max_reqs_day:
            tokens.remove(current)
            print("Token counts: ", counts)
            token_iter = itertools.cycle(tokens)
            current = next(token_iter)            
        while time.time() - last_used[current] < 1/max_reqs_sec:
            current = next(token_iter)
        yield {'token': current}
        last_used[current] = time.time()
        counts[current] += 1
            
next_header = make_header()
# g = time.time()
# for i in range(30):
#     try:
#         print(next(next_header))
#     except Exception as e:         
#         if isinstance(e, StopIteration):
#             raise Exception("Have run out of token's for the day")
# print(time.time() - g)

def get_date_stat_val(req):
    "given a request in json, returns list of date, station, and value"
    tup_data = [(datetime.strptime(i['date'], "%Y-%m-%dT%H:%M:%S"),
                i['station'],
                i['value'])
                for i in req.json()['results']]
    return list(zip(*tup_data))

def iter_thru_req(requrl, maxresults = None,
                  col_names = None, index_name = None):
    """"gets all count values in requrl, returns a dataframe with those values
    index_name: the name of colum to use as index
    col_names: the names of the columns to keep from what's returned
    returns request results on index, columns are data categorizes
<<<<<<< HEAD
    rerunning if failed partway thru will preserve intermediate results
=======
>>>>>>> c880c80d812c609d6b16b0bd7171d148203479a6
    """
    if requrl[:5] != 'https':
        requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/' + requrl
    assert(requrl[:41] == 'https://www.ncdc.noaa.gov/cdo-web/api/v2/')
    assert('limit' not in requrl)
    
    save_name = f"_temp_data_save"
    def save_incomplete_req(data, get = False):
        try:
            prev_data = load_struct(save_name)
            data = prev_data + data #preserve order
        except:
            pass
        if get:
            return data
        else:
            save_struct(data, save_name) 
    
<<<<<<< HEAD
    data = save_incomplete_req([], get = True)#can rerun and get intermediate results
=======
    data = save_incomplete_req([], get = True)
>>>>>>> c880c80d812c609d6b16b0bd7171d148203479a6
    offset = len(data)
    if 'offset' not in requrl:
        requrl += '&offset=' + str(offset)
    else:
        requrl = re.sub("offset=\d+", f"offset={offset}", requrl)
    requrl += '&limit=' + str(max_lmt_sz) #NOAA only limits number of calls, not size
    first_js = requests.get(requrl, headers = next(next_header)).json()
    
    if col_names is None:
        col_names = list(first_js['results'][0].keys())
    else:
        cols = [i for i in col_names if i in first_js['results'][0].keys()]
        assert cols == col_names, f"""the following cols weren't included,
                                    {[i for i in col_names if i not in cols]}"""
    if index_name is not None:
        assert index_name in col_names, f"""col for Index not in {col_names}"""
    
    size = first_js['metadata']['resultset']['count']
    if maxresults:
        size = min(size, maxresults)
    data += first_js['results']
    offset += len(data) 
    requrl += '&includemetadata=false'#faster
    #what if size changes partway through requests? i.e. more data's added?
    num_fails = 0
    reps = 0
    while offset < size:
        try:
            js = requests.get(requrl, headers = next(next_header)).json()
            data += js['results']
            new_limit = min(max_lmt_sz, size - offset)#maximium value 
            offset += new_limit
            requrl = re.sub("limit=\d+", f"limit={new_limit}", requrl)
            requrl = re.sub("offset=\d+", f"offset={offset}", requrl)
            reps += 1
            if reps % 100 == 0:
                save_incomplete_req(data)
                data = []
        except Exception as e:
            num_fails += 1
            save_incomplete_req(data)
            if isinstance(e, StopIteration):
                raise Exception("Have run out of token's for the day")
            data = []
            print(f"{num_fails} Failed on {requrl}, @{offset}", e, '\n\n')
            time.sleep(3)
            if num_fails %5 == 0:
                time.sleep(30)
    data = save_incomplete_req(data, get = True)
    out = pd.DataFrame(data, columns = col_names)
    for c in out.columns:
        if 'date' in c:
            out.loc[:,c] = pd.to_datetime(out.loc[:,c], format="%Y-%m-%d")
    if index_name is not None:
        out = out.set_index(index_name)
    delete_struct(save_name, verbal = False)
    return out 

if 'dataset_dates' not in globals():
    dataset_dates = {i['id']: (i['mindate'], i['maxdate'])
                     for i in explore_reqs('datasets')['results']}

def get_entire_dataset(dataset_id, struct_name):
    """Gets all data for a dataset with id dataset_id and places in struct_name
    """
    try:
        return load_struct(struct_name)
    except:
        pass
    #started at 1970 per API, make 1 call that defines all dates for each data type?
    start, end = dataset_dates[dataset_id]
    start, end = int(start[:4]), int(end[:4])
<<<<<<< HEAD
    regex = f'{struct_name}_yr(\d+)$'
=======
    regex = f'{struct_name}_yr(\d+)'
>>>>>>> c880c80d812c609d6b16b0bd7171d148203479a6
    names, data = regex_load_struct(regex)
    start = 1 + max((int(re.search(regex, i).group(1)) for i in names), 
                    default = start - 1)
    for yr in range(start, end):
        start = f'{yr}-01-01'#dates must be within 1 yr of each other
        end = f'{yr+1}-01-01'
        requrl = f'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid={dataset_id}&startdate={start}&enddate={end}'
        temp = iter_thru_req(requrl)
        if 'dataset_id' == 'PRECIP_15':#since got partway thru dataset doing this
            temp = temp.pivot_table(
                                    index = ['date', 'station'], 
                                    columns = 'datatype', 
                                    values = 'value')
        save_struct(temp, f"{struct_name}_yr{yr}")
        data += [temp]
        #Only QPCP data till at least 1988
    
    data = pd.concat(data)
    save_struct(data, struct_name)
    return data

def get_station_locs():
    "grib!! currently returns None"
    struct_name = "station_locations"
    try:
        return load_struct(struct_name)
    except:
        pass
    regex = f'{struct_name}_st(\d+)'
    names, data = regex_load_struct(regex)
    for fips_id in ['01', '02', '04', '05', '06', '08', '09', '10', '12', '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56', '60', '66', '69', '72', '78']:
        requrl = f"https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?locationid=FIPS:{fips_id}"
        try:
            temp = load_struct(f"{struct_name}_st{fips_id}")
        except:
            temp = iter_thru_req(requrl)
        print(temp)
        save_struct(temp, f"{struct_name}_st{fips_id}")
        data += [temp]
    data = pd.concat(data)
    save_struct(data, struct_name)
    return data

def process_data():
    precipitation_data = get_entire_dataset(dataset_id = 'PRECIP_15', 
                                     struct_name = 'precipitation_data')
    precipitation_data[precipitation_data==99999] = np.nan
    #rearrange datatypes from being indentifier in 1 col, to each having individual col
    # out = out.pivot(index = 'date', columns = 'datatype')
    # out.columns = out.columns.map("_".join)
    # station_cols = [i for i in out if 'station' in i]
    # out['station'] = out[station_cols].fillna(axis=0, method = 'bfill').iloc[:,0]
    # out = out.drop(station_cols, axis=1)
    ###am removing 'attributes' column here; don't know what they do
    
    # precipitation_data = precipitation_data.pivot_table(
    #                                             index = ['date', 'station'], 
    #                                             columns = 'datatype', 
    #                                             values = 'value') #doesn't work w/ non numeric columns?
    
    print(precipitation_data)
    station_locs = get_station_locs()
    assert all(station_locs['elevationUnit'].isin(['METERS', np.nan])), "CHECK Units"
    #assumes don't care when station started/ended
    station_locs = station_locs.drop(['mindate', 'maxdate', 'elevationUnit', 'name'],
                                     axis=1)
<<<<<<< HEAD
    #could also drop datacoverage?
=======
>>>>>>> c880c80d812c609d6b16b0bd7171d148203479a6
    station_locs = station_locs.set_index('id')
    precipitation_data =  precipitation_data.join(station_locs, on=['station'])
    save_struct( precipitation_data, 'all_data')
    return precipitation_data

# all_data = process_data()
precipitation_data = get_entire_dataset(dataset_id = 'PRECIP_15', 
                                     struct_name = 'precipitation_data')
#%%
<<<<<<< HEAD


#%%
=======
>>>>>>> c880c80d812c609d6b16b0bd7171d148203479a6
# requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?limit=1000' + '&' + yesterstr + '&' + todaystr + '&locationid=FIPS:37'
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

