datatypes = explore_reqs('datasets')
#NDC uses 'Datasets' as primary groupings
[(i['name'], i['id'],i['uid']) for i in datatypes['results']]#Interesting datasets
#use ID not uid
  # ('Weather Radar (Level II)', 'NEXRAD2', 'gov.noaa.ncdc:C00345'),
  # ('Weather Radar (Level III)', 'NEXRAD3', 'gov.noaa.ncdc:C00708'),
  # ('Normals Daily', 'NORMAL_DLY', 'gov.noaa.ncdc:C00823'),
  # ('Precipitation 15 Minute', 'PRECIP_15', 'gov.noaa.ncdc:C00505'),#only 25% data coverage
  # ('Precipitation Hourly', 'PRECIP_HLY', 'gov.noaa.ncdc:C00313')#100% coverage, longer time period
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
requrl = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=PRECIP_15&stationid=COOP:010008&units=metric&startdate=2010-05-01&enddate=2010-05-31'
a2 = requests.get(requrl, headers = {'token': weather_token})
a2.json()