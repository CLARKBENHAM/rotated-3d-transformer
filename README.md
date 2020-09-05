Am trying to create new ML architecture using PyTorch for predicting weather patterns (and thus hopefully electricity prices).
As 3d LSTMs work for generating pixels, would have to have a 'rotated' 3d LStm that when given current/previous data from NOAA stations in an area can predict values at t+1, so 2d for spatial layout + 1d for time. (Hopefully correlated to $).

2 main challenges:
  expand 2d pixel generation for video (add time dim)
  account for geometry of station's location data not being grid aligned,
  	  want to predict for different locations than stations are?
  
Todo:
Get more clear on how bert was trained, time/spatial is conflated for text processing, how about for pixel/video generation?

get NOAA station data.
Preprocessing:
	segment station data by geographic area/match to price data
		or are there national trends that need to be picked up?
	upload to collab
	have dropout encoding for training? Would learn weights for predicting left out station data?
	     (But want to predict value at t+1, not geographically adjacent)
	     Best way to learn weights is dropout across both atributes?
Figure out archetecture
       ?Have some way to account for how geographic relationship varies between stations?
       ?Just use 2d/8 surrounding cells, as what's done for predicting the next pixel. network learn this automatically from how much weight to place on related cells?
       	?Shrink size of cells so that all stations within a single cell, and most cell inputs are null?

get electricity price data
Add CNN on stations to price data and see what happens.
Profit.
