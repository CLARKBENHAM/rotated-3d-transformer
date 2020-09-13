datasets = explore_reqs('datasets')
#NDC uses 'Datasets' as primary groupings
[(i['name'], i['id'],i['uid']) for i in datasets['results']]#Interesting datasets
#use ID not uid
  # ('Weather Radar (Level II)', 'NEXRAD2', 'gov.noaa.ncdc:C00345'),
  #date range< 1yr
  # ('Weather Radar (Level III)', 'NEXRAD3', 'gov.noaa.ncdc:C00708'),
  #Following no longer supported
  # ('Normals Daily', 'NORMAL_DLY', 'gov.noaa.ncdc:C00823'),
  # ('Precipitation 15 Minute', 'PRECIP_15', 'gov.noaa.ncdc:C00505'),#only 25% data coverage
  # ('Precipitation Hourly', 'PRECIP_HLY', 'gov.noaa.ncdc:C00313')#100% coverage, longer time period

#Do I need all datypeid's? There's 1500 so won't worry about that
# datatypes = explore_reqs('https://www.ncdc.noaa.gov/cdo-web/api/v2/datatypes', dates=True, limit = 100)

#what are atributes? eg. of PRECIP_15
#%%
"""want all data for each station
https://www.ncdc.noaa.gov/data-access offers a bunch of other data sources: Radar, ballons, etc.
so could extend out to 4D LSTM, if include height data at loc
"""
a = explore_reqs('data?datasetid=NEXRAD2', dates=True)#requires Dates
#getting 500 error?
# a
# b = make_request('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=NORMAL_DLY')
# 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GSOM&limit=100&startdate=2000-09-03&enddate=2020-09-07'
#%%
out = []
for id in datasets['results'][2:-2]: 
    end = int(id['maxdate'][:4])
    start = end -1 
    requrl = f'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid={id["id"]}&startdate={start}-01-01&enddate={end}-01-01'
    a = requests.get(requrl, headers = {'token': weather_token})
    out += [a]
    print(id['name'], a.content)

bad_datasets = [0,1] + [ix+2 for ix,i in enumerate(out) if 200 != i.status_code]
good_datasets = [ix+2 for ix,i in enumerate(out) if 200 == i.status_code] + [9,10]
[datasets['results'][i] for i in good_datasets]
#good:
[{'uid': 'gov.noaa.ncdc:C00947',
  'mindate': '1763-01-01',
  'maxdate': '2020-01-01',
  'name': 'Global Summary of the Year',
  'datacoverage': 1,
  'id': 'GSOY'},
 {'uid': 'gov.noaa.ncdc:C00824',
  'mindate': '2010-01-01',
  'maxdate': '2010-12-31',
  'name': 'Normals Hourly',
  'datacoverage': 1,
  'id': 'NORMAL_HLY'},
 {'uid': 'gov.noaa.ncdc:C00822',
  'mindate': '2010-01-01',
  'maxdate': '2010-12-01',
  'name': 'Normals Monthly',
  'datacoverage': 1,
  'id': 'NORMAL_MLY'},
 {'uid': 'gov.noaa.ncdc:C00505',
  'mindate': '1970-05-12',
  'maxdate': '2014-01-01',
  'name': 'Precipitation 15 Minute',
  'datacoverage': 0.25,
  'id': 'PRECIP_15'},
 {'uid': 'gov.noaa.ncdc:C00313',
  'mindate': '1900-01-01',
  'maxdate': '2014-01-01',
  'name': 'Precipitation Hourly',
  'datacoverage': 1,
  'id': 'PRECIP_HLY'}]
#none of which is usefull!!!

#USe ftp://ftp.ncdc.noaa.gov/pub/data/normals/1981-2010/ for Normals hourly
#%%
requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=NEXRAD2&startdate=2020-02-03&enddate=2020-09-04'
a2 = requests.get(requrl, headers = {'token': weather_token})
a2.json()
#%%
requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=PRECIP_15&stationid=COOP:010008&units=metric&startdate=2010-05-01&enddate=2010-05-31&limit=100'
a3 = requests.get(requrl, headers = {'token': weather_token}).json()
a3['metadata']
col_names = list(a3['results'][0].keys())
target = pd.DataFrame(a3['results'], columns = col_names)
#%%
#to get all stations run for all fips ID's?
# for fips_id in ['01', '02', '04', '05', '06', '08', '09', '10', '12', '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56', '60', '66', '69', '72', '78']:
# f"https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?locationid=FIPS:{fips_id}"
e = requests.get(f"https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?locationid=FIPS:01",headers = {'token': weather_token})
#%%
data = e.json()['results']
col_names = list(e.json()['results'][0].keys())
out = pd.DataFrame(data, columns = col_names)
for c in out.columns:
    if 'date' in c:
        out.loc[:,c] = pd.to_datetime(out.loc[:,c], format="%Y-%m-%d")
out.pivot_table(index = ['date', 'station'], 
                           columns = 'datatype', 
                           values = 'value')
#%%

# datacategories = explore_reqs("datacategories", dates=False)
# datacategories 
