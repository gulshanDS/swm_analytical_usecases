import os
import sys
import json
import requests
import warnings
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime

warnings.filterwarnings('ignore')

time_now = datetime.now().strftime("%Y-%m-%d %H:%M")
today_date = datetime.now().strftime("%Y-%m-%d")

api_csv_col_name_mapping = {"entity_id": "entity_id", "sid": "identifier", "deviceid": "device_id",
                            "latitude": "latitude", "longitude": "longitude", "location": "location",
                            "circle": "circle", "bintype": "bin_type", "temperature": "temperature",
                            "battery": "battery", "signallevel": "signallevel", "fillelevel": "filledlevel",
                            "fillrate": "fill_rate", "iterationsarray": "iterations_array",
                            "pickedfilllevel": "picked_filllevel", "transactions": "transactions",
                            "filltime": "fill_time", "sourcetimestamp": "source_timestamp",
                            "last_updated": "last_updated", "pickedat": "picked_at",
                            "created_at": "created_at", "updated_at": "update_timestamp"}

date_columns = ['source_timestamp', 'last_updated', 'picked_at', 'created_at', 'update_timestamp']


class DataRetrivalApi():
    """
        This Class Fetches the data from the data from the API
        for the table swm_netbins_agg_data from NDMC 2.x db.
    """

    def __init__(self):
        self.auth_url = "https://dashboard.ndmc.com/ds/1.0.0/public/token"
        self.auth_headers = {'Content-Type': 'application/json'}
        self.auth_payload = json.dumps({"username": "kartheek@ndmc.com",
                                        "password": "Mullapudi@1",
                                        "grant_type": "password"})

        self.data_url = "https://dashboard.ndmc.com/abstraction/1.0.0/dashboard/getData/U8-eC3sBwOPaHTv4EWJP"
        self.data_payload = {"from": "2019-01-01", "to": today_date}
        self.data_headers = {'Accept': 'application/json, text/plain, */*',
                             'Authorization': 'Bearer {access_token}',
                             'Content-Type': 'application/json',
                             'Origin': 'https://dashboard.ndmc.com'}

    def authenticate(self):
        response = requests.request("POST",
                                    self.auth_url, headers=self.auth_headers,
                                    data=self.auth_payload, verify=False)
        if response.status_code == 200:
            access_token = json.loads(response.text)['access_token']
            return access_token
        else:
            print("Couldn't Authenticate API Credentials")
            print("Status Code: ", response.status_code)
            print("Status Text: ", response.text)
            raise Exception("SourceAPIAuthenticationError")

    def fetch_data(self, from_date=None, till_date=None):
        # print(self.data_payload)
        data_payload = self.data_payload
        if from_date:
            data_payload['from'] = from_date
        if till_date:
            data_payload['to'] = till_date

        # print(data_payload)
        data_payload = json.dumps(data_payload)
        # print(data_payload)

        access_token = self.authenticate()
        data_headers = self.data_headers
        data_headers['Authorization'] = data_headers['Authorization'].format(access_token=access_token)

        response = requests.request("POST", self.data_url, headers=self.data_headers,
                                    data=data_payload, verify=False)

        if response.status_code == 200:
            results = json.loads(response.text)
            if len(results['result']) > 0:
                df = pd.DataFrame(results['result'])
                return df
            else:
                print("No Records Found In the Given Time Period")
                return None
        else:
            print("Error While Fetching Data From Source API")
            print("Status Code: ", response.status_code)
            print("Status Code: ", response.text)
            raise Exception("SourceAPIDataFetchError")


def get_swm_api_df():
    d_ret = DataRetrivalApi()
    swm_netbins_df = d_ret.fetch_data()
    swm_netbins_df.rename(columns=api_csv_col_name_mapping, inplace=True)

    for d in date_columns:
        try:
            swm_netbins_df[d] = pd.to_datetime(swm_netbins_df[d], format='%Y-%m-%d %H:%M:%S')
            # .strftime("%Y-%m-%d")
            # .strftime("%Y-%m-%d %H:%M:%S")
            # ,format='%Y-%m-%d %H:%M:%S'
        except:
            pass
    swm_netbins_df.sort_values(by=['source_timestamp', 'location', 'identifier'], inplace=True)
    swm_netbins_df.drop_duplicates(subset=['source_timestamp', 'location', 'identifier'], inplace=True,
                                   ignore_index=True)
    return swm_netbins_df


def get_daterange_helper(df):
    min_date_value = pd.to_datetime(df.source_timestamp.dt.date.min().strftime('%Y-%m-%d 00:00:00'))
    max_date_value = pd.to_datetime(df.source_timestamp.dt.date.max().strftime('%Y-%m-%d 23:59:59'))
    each_hour_date_list = pd.date_range(min_date_value, max_date_value, freq='12H')
    return each_hour_date_list


def process_bin_fill_frequency_for_a_location(df):
    temp_binfill_frequency_list = []
    if len(df) > 0:
        every_halfday_list = get_daterange_helper(df)
        bin_location = df.location.unique().tolist()[0]
        # print(every_halfday_list)

        for a_half_day in every_halfday_list:
            # print(type(a_half_day))
            filtered_df = df.loc[
                ((df.source_timestamp <= a_half_day.strftime("%Y-%m-%d %H:%M:%S")) & (~df.fill_rate.isna())),]
            if len(filtered_df) > 0:
                fill_rate_value = \
                filtered_df.loc[filtered_df.source_timestamp == filtered_df.source_timestamp.max(), 'fill_rate'].values[
                    -1]
                binfill_frequency_dict = {'location': bin_location,
                                          'identifier': location_identifier_mapping[bin_location],
                                          'source_timestamp': a_half_day,
                                          'fill_rate': fill_rate_value,
                                          'fillrate_last_updated': filtered_df.source_timestamp.max()}
                temp_binfill_frequency_list.append(binfill_frequency_dict)

    return temp_binfill_frequency_list


def hours_to_fill_helper(x):
    try:
        return round(100 / x, 2)
    except ZeroDivisionError:
        return 0


def bin_fill_frequency_data(source_data=None):
    if not source_data:
        source_data = get_swm_api_df()

    source_mapping = source_data.set_index('location').identifier.to_dict()
    global location_identifier_mapping
    location_identifier_mapping = source_data.set_index('location').identifier.to_dict()

    binfill_frequency_list = []
    for bin_location in source_mapping.keys():
        # print("Processing: ",bin_location)
        source_location_data = source_data.loc[source_data.location == bin_location]
        bin_fills = process_bin_fill_frequency_for_a_location(source_location_data)
        binfill_frequency_list.extend(bin_fills)

    if len(binfill_frequency_list) > 0:
        df = pd.DataFrame(binfill_frequency_list)
        df['fill_rate'].fillna(value=0, inplace=True)
        df['fill_rate'] = df['fill_rate'].astype(float).astype(int)
        df['hours_to_fill'] = df['fill_rate'].apply(lambda x: hours_to_fill_helper(x))
    else:
        df = pd.DataFrame()

    return df


def main():
    source_data = get_swm_api_df()
    source_data.sort_values(by=['location', 'source_timestamp'], inplace=True)
    cols_list = ['location','identifier', 'source_timestamp', 'filledlevel', 'fill_rate']
    latest_fill_data = source_data.drop_duplicates(subset=['location'], keep='last')[cols_list].reset_index(drop=True)

    latest_fill_data['fill_rate'].fillna(value=0, inplace=True)
    latest_fill_data['fill_rate'] = latest_fill_data['fill_rate'].astype(float).astype(int)
    latest_fill_data['filledlevel'] = latest_fill_data['filledlevel'].astype(float).astype(int)
    latest_fill_data['hours_to_fill'] = latest_fill_data['fill_rate'].apply(lambda x: hours_to_fill_helper(x))

    latest_fill_data['source_timestamp'] = latest_fill_data['source_timestamp'].dt.tz_localize(None)
    latest_fill_data['source_timestamp_rounded'] = latest_fill_data['source_timestamp'].dt.round('H')

    max_date_value = pd.to_datetime('now').round('H') + pd.Timedelta(hours=24)

    new_list = []
    alfa_list = []

    for row_index, col_dict in latest_fill_data.iterrows():

        location = col_dict['location']
        fill_rate = col_dict['fill_rate']
        hours_to_fill = col_dict['hours_to_fill']
        present_filledlevel = col_dict['filledlevel']

        min_date_value = pd.to_datetime(col_dict['source_timestamp_rounded'])
        each_hour_date_list = pd.date_range(min_date_value, max_date_value, freq='H')

        hrs_since_update = int(
            (pd.to_datetime('now').round('H') - col_dict['source_timestamp_rounded']) / pd.Timedelta(hours=1))

        alfa_one_list = []
        if fill_rate != 0:
            f_level = present_filledlevel
            for an_hour in each_hour_date_list[1:]:
                f_level += fill_rate
                temp_dict = {
                    'location': location,
                    'last_update_timestamp': min_date_value.strftime("%Y-%m-%d %H:%M:%S"),
                    'last_update_fillevel': present_filledlevel,
                    'last_update_fillrate': fill_rate,
                    'hours_since_last_update': hrs_since_update,
                    'forecast_timestamp': an_hour,
                    'forecast_fillevel': min(100, f_level)}
                new_list.append(temp_dict)

                if an_hour.round("D") >= pd.to_datetime('now').round('D'):
                    alfa_one_dict = {'forecast_timestamp': an_hour.strftime("%Y-%m-%d %H:%M:%S"),
                                     'forecast_fillevel': min(100, f_level)}
                    alfa_one_list.append(alfa_one_dict)
        else:
            for an_hour in each_hour_date_list[1:]:
                temp_dict = {
                    'location': location,
                    'last_update_timestamp': min_date_value.strftime("%Y-%m-%d %H:%M:%S"),
                    'last_update_fillevel': present_filledlevel,
                    'last_update_fillrate': fill_rate,
                    'hours_since_last_update': hrs_since_update,
                    'forecast_timestamp': an_hour,
                    'forecast_fillevel': present_filledlevel}
                new_list.append(temp_dict)

                if an_hour.round("D") >= pd.to_datetime('now').round('D'):
                    alfa_one_dict = {'forecast_timestamp': an_hour.strftime("%Y-%m-%d %H:%M:%S"),
                                     'forecast_fillevel': present_filledlevel}
                    alfa_one_list.append(alfa_one_dict)

        alfa_two_dict = {'location': location,
                         'last_update_timestamp': min_date_value.strftime("%Y-%m-%d %H:%M:%S"),
                         'last_update_fillevel': present_filledlevel,
                         'last_update_fillrate': fill_rate,
                         'hours_since_last_update': hrs_since_update,
                         'forecasts': alfa_one_list}
        alfa_list.append(alfa_two_dict)

    new_df = pd.DataFrame(new_list)
    new_df = new_df[new_df.forecast_timestamp.dt.date >= pd.to_datetime('now').round('D')]

    return json.dumps(alfa_list)


if __name__ == "__main__":
    response = main()
    print(response)
