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
requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GSOM&limit=100&startdate=2012-09-03&enddate=2020-09-07'
a = requests.get(requrl, headers = {'token': weather_token})

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

