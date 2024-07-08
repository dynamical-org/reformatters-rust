# NOAA GFS analysis, hourly

Historical weather data from the Global Forecast System (GFS) model operated by NOAA NCEP, transformed into zarr format by [dynamical.org](https://dynamical.org).

## Dataset Overview

- **Spatial domain**: Global
- **Spatial resolution**: 0.25 degrees (~20km)
- **Time domain**: 2015-01-15 00:00:00 UTC to 2024-07-01 00:00:00 UTC
- **Time resolution**: 1 hour

URL: `https://data.dynamical.org/noaa/gfs/analysis-hourly/latest.zarr?email=option@email.com`

_Email optional: registration not required. Providing your email as a query param helps us understand usage and impact to keep dynamical.org supported for the long-term. For catalog updates follow [here](https://dynamical.org/updates)._

## Description

The Global Forecast System (GFS) is a National Oceanic and Atmospheric Administration (NOAA) National Centers for Environmental Prediction (NCEP) weather forecast model that generates data for dozens of atmospheric and land-soil variables, including temperatures, winds, precipitation, soil moisture, and atmospheric ozone concentration. The system couples four separate models (atmosphere, ocean model, land/soil model, and sea ice) that work together to depict weather conditions.

This dataset is an "analysis" containing the model's best estimate of each value at each timestep. In other words, it does not contain a forecast dimension. GFS starts a new model run every 6 hours and dynamical.org has created this analysis by concatenating the first 6 hours of each forecast. Before 2021-02-27 GFS had a 3 hourly step at early forecast hours. In this reanalysis we have used linear interpolation in the time dimension to fill in the two timesteps between the three-hourly values prior to 2021-02-27.

The data values in this dataset have been rounded in their binary representation to improve compression. We round to retain 9 bits of the floating point number's base (mantissa) which creates a maximum of 0.2% difference between the original and rounded value. See [Klöwer et al. 2021](https://www.nature.com/articles/s43588-021-00156-2) for more information.

## Dimensions

| Dimension | Start                   | Stop                    | Units                             |
| --------- | ----------------------- | ----------------------- | --------------------------------- |
| latitude  | 90                      | -90                     | decimal degrees                   |
| longitude | -180                    | 180                     | decimal degrees                   |
| time      | 2015-01-15 00:00:00 UTC | 2024-07-01 00:00:00 UTC | seconds since 1970-01-01 00:00:00 |

## Variables

| Variable              | Units      | Dimensions                  | Description                                          |
| --------------------- | ---------- | --------------------------- | ---------------------------------------------------- |
| precipitation_surface | kg/(m^2 s) | time × latitude × longitude | Precipitation rate at earth surface                  |
| temperature_2m        | C          | time × latitude × longitude | Temperature 2 meters above earth surface             |
| wind_u_10m            | m/s        | time × latitude × longitude | Wind speed u-component 10 meters above earth surface |
| wind_v_10m            | m/s        | time × latitude × longitude | Wind speed v-component 10 meters above earth surface |

## Example

### Mean temperature for a single day

```python
import xarray as xr

ds = xr.open_zarr("https://data.dynamical.org/noaa/gfs/analysis-hourly/latest.json?email=optional@email.com")
ds["temperature_2m"].sel(time="2024-06-01T00:00").mean().compute()
```
