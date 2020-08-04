import pandas as pd
import io
import requests
import json

class OsloApi:
    
    base_url = "https://api.nilu.no"
    
    air_quality_index_url = "/aq/historical/{fromtime}/{totime}/{latitude}/{longitude}/{radius}?components={components}"
    
    def getAllPossibleComponents(self):
        url = "https://api.nilu.no/lookup/components"
        raw_data = requests.get(url).content
        raw_data_decoded = raw_data.decode('utf-8')
        res = json.loads(raw_data_decoded)
        res
        df = pd.DataFrame.from_dict(res)
        return df

    def getAllPossibleAreas(self):
        url = "https://api.nilu.no/lookup/areas"
        raw_data = requests.get(url).content
        raw_data_decoded = raw_data.decode('utf-8')
        res = json.loads(raw_data_decoded)
        res
        df = pd.DataFrame.from_dict(res)
        return df

    def getAllPossibleStations(self, area):
        url = "https://api.nilu.no/lookup/stations?area={}".format(area)
        raw_data = requests.get(url).content
        raw_data_decoded = raw_data.decode('utf-8')
        res = json.loads(raw_data_decoded)
        res
        df = pd.DataFrame.from_dict(res)
        return df

    def getAirQualityIndexPerComponent(self, component):
        url = "https://api.nilu.no/lookup/aqis?component={}&culture=en".format(component)
        raw_data = requests.get(url).content
        raw_data_decoded = raw_data.decode('utf-8')
        res = json.loads(raw_data_decoded)
        res
        df = pd.DataFrame.from_dict(res[0]['aqis'])
        return df

    def getAllPossibleAveragingsTypes(self):
        url = "https://api.nilu.no/lookup/meantypes?culture=en"
        raw_data = requests.get(url).content
        raw_data_decoded = raw_data.decode('utf-8')
        res = json.loads(raw_data_decoded)
        res
        df = pd.DataFrame.from_dict(res)
        return df

    def getAllTimeSeriesByStation(self, station):
        url = "https://api.nilu.no/lookup/timeseries?station={}".format(station)
        raw_data = requests.get(url).content
        raw_data_decoded = raw_data.decode('utf-8')
        res = json.loads(raw_data_decoded)
        res
        df = pd.DataFrame.from_dict(res)
        return df

    def getHistoricalObservationsByStationAndComponent(self, start_date, end_date, station, component):
        url = "https://api.nilu.no/obs/historical/{fromtime}/{totime}/{station}?components={component}".format(fromtime = start_date, totime = end_date, station = station, component = component)
        raw_data=requests.get(url).content
        raw_data_decoded = raw_data.decode('utf-8')
        res = json.loads(raw_data_decoded)
        #return res
        df = pd.DataFrame.from_dict(res[0])
        df['fromTime'] = df.apply(lambda r: r['values']['fromTime'], axis=1)
        df['toTime'] = df.apply(lambda r: r['values']['toTime'], axis=1)
        df['value'] = df.apply(lambda r: r['values']['value'], axis=1)
        df['qualityControlled'] = df.apply(lambda r: r['values']['qualityControlled'], axis=1)
        return df.drop('values', axis=1)
    
    def getHistoricalObservationsBypositionAndRadius(self, start_date, end_date, lat, lon, radius, component):
        url = "https://api.nilu.no/obs/historical/{fromtime}/{totime}/{lat}/{lon}/{radius}?components={component}".format(fromtime = start_date, totime = end_date, lat =lat, lon=lon, radius = radius, component = component)
        raw_data=requests.get(url).content
        raw_data_decoded = raw_data.decode('utf-8')
        res = json.loads(raw_data_decoded)
        #return res
        df = pd.DataFrame.from_dict(res[0])
        df['fromTime'] = df.apply(lambda r: r['values']['fromTime'], axis=1)
        df['toTime'] = df.apply(lambda r: r['values']['toTime'], axis=1)
        df['value'] = df.apply(lambda r: r['values']['value'], axis=1)
        df['qualityControlled'] = df.apply(lambda r: r['values']['qualityControlled'], axis=1)
        return df.drop('values', axis=1)