import pandas as pd
import json
import numpy as np

from onboard.client import RtemClient
from onboard.client.dataframes import points_df_from_streaming_timeseries
from onboard.client.models import PointSelector, TimeseriesQuery, PointData
from datetime import datetime, timezone, timedelta
from typing import List
import pytz

def main():
    try:
        client = load_client()
    except:
        print("Client could not be loaded. Is the secret entered correctly?")
        return
    print_building_names(client)
    sensor_data = get_sensor_data(client)
    print(sensor_data.to_string())

def load_client():
    """
    Loads the client by using the secret file

    :return: OnboardClient of the user
    """
    # Read secret from /secrets/ folder
    with open('secrets/secrets.txt') as f:
        secret = f.readlines()[0]

    # Setup client
    client = RtemClient(api_key=secret)
    return client

def print_building_names(client):
    """
    Prints a list of all the buildings accessed by the OnboardClient
    """

    building_names = list(pd.DataFrame(client.get_all_buildings())['name'])
    print(building_names)

def get_sensor_data(
    client,
    point_types=['Zone Temperature'],
    equipment_types=['fcu'],
    buildings=['Laboratory'],
    start = datetime.now(pytz.utc) - timedelta(days=7),
    end   = datetime.now(pytz.utc)
):
    """
    Retrieves data for the specified sensors and buildings
    
    :param client: OnboardClient of the user
    :param point_types: list of str, representing the point types to get data from, defaults to ['Zone Temperature']
    :param equipment_types: list of str, representing the point types to get data from, defaults to ['fcu']
    :param buildings: list of str, representing the point types to get data from, defaults to ['Laboratory']
    :param start: datetime, start time of collecting data, defaults to one week ago
    :param end: datetime, end time of collecting data, defaults to now
    :return sensor_data: dataframe with sensor data for the selected points, equipments, buildings, and times
    """
    query = PointSelector()
    query.point_types     = point_types         # can list multiple point
    query.equipment_types = equipment_types     # types, equipment types,
    query.buildings       = buildings           # buildings, etc.
    selection = client.select_points(query)    

    timeseries_query = TimeseriesQuery(point_ids = selection['points'], start = start, end = end)
    sensor_data = points_df_from_streaming_timeseries(client.stream_point_timeseries(timeseries_query))
    return sensor_data

if __name__ == "__main__":
    main()