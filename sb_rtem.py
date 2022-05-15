# Standard python libraries
import pandas as pd
import json
import numpy as np

# Import plottting libraries
import plotly
import seaborn as sns

# RTEM libraries
from onboard.client import RtemClient
from onboard.client.dataframes import points_df_from_streaming_timeseries
from onboard.client.models import PointSelector, TimeseriesQuery, PointData
from datetime import datetime, timezone, timedelta
import pytz


def sb_buildings_by_function(df,function_string):
    """Function to get all buildings from the all buildings dataframe and filter them by function
    params:
    df: A pandas dataframe
    function_string : A list of string functions
    return: Filtered pandas dataframe"""
    filtered_df = df[df["customerType"]==function_string]
    return filtered_df

def get_building_info(building_id,df_buildings_flt):
    """_summary_

    Args:
        building_id (_type_): _description_
        df_buildings_flt (_type_): _description_

    Returns:
        _type_: _description_
    """
    building_info = df_buildings_flt[df_buildings_flt["id"]==building_id].to_dict()
    return building_info

def dump_csv(sensors,metadata):
    """_summary_

    Args:
        sensors (_type_): _description_
    """
    i = 0
    for df in sensors:
        path_str = "sensor_dump/"+str(i)+"_sensor.csv"
        df.to_csv(path_str)
        i+=1
    i = 0
    for df in metadata:
        path_str = "sensor_dump/"+str(i)+"_meta.csv"
        df.to_csv(path_str)
        i+=1

def dump_excel(sensors,metadata,building_info):
    with pd.ExcelWriter("excel_dump.xlsx") as writer:
        i = 0
        for sensor in sensors:
            sheet_name = "sensor_"+str(i)
            sensor.to_excel(writer,sheet_name=sheet_name)
            i+=1
        i = 0
        for meta in metadata:
            sheet_name = "meta_"+str(i)
            meta.to_excel(writer,sheet_name = sheet_name)
            i+=1
        building_info.to_excel(writer,sheet_name = "building_info")


def doctor_my_building(client,building_id,df_buildings_flt,year):
    """_summary_

    Args:
        building_id (_type_): _description_
        df_buildings_flt (_type_): _description_
        year (_type_): _description_

    Returns:
        _type_: _description_
    """

    csv_eq_types = pd.json_normalize(client.get_equipment_types())
    csv_point_types = pd.json_normalize(client.get_all_point_types())
    dflist_equipments = []
    dflist_metadata = []
    from matplotlib import pyplot as plt
    building_info = get_building_info(building_id,df_buildings_flt)
    id = list(building_info["id"].values())[0]
    building_area = list(building_info["sq_ft"].values())[0]
    building_city = list(building_info["geoCity"].values())[0]
    building_use = list(building_info["customerType"].values())[0]
    print(f"You have selected building {id}\nBuilding {id} is located in the {building_city} area of NY and has an area of {building_area} square feet.\nBuilding {building_id} belongs to the category {building_use}.")

    #Select the category
    df_buildingUse = sb_buildings_by_function(df_buildings_flt,building_use)
    # Lets start with this building
    all_buildings = df_buildingUse["id"].unique().tolist()
    print(f'There are {len(all_buildings)} other buildings available in this category')
    
    # Going into equipments
    all_equipment = pd.DataFrame(client.get_building_equipment(id))
    eq_types = all_equipment["equip_type_tag"].unique().tolist()
    print(f"The following equipment types are available {eq_types}")
    
    # Maybe this helps somewhere?
    #null_equipment = csv_eq_types[csv_eq_types["critical_point_types"] == '[]']["tag_name"].to_list()
    
    # Fixing the date/time format
    tz = pytz.timezone('UTC')
    start_date = datetime(year,1,1,0,0,0).replace(tzinfo=tz)
    end_date = datetime(year,12,31,0,0,0).replace(tzinfo=tz)
    
    # For each equipment type in this building
    for i in eq_types:
        print(f"    Fetching sensors for {i}...")
        # Look for ValueError: invalid literal for int() with base 10: '' 
        # and add the equipment that caused this error in the list below
        if i not in ['site','virtual','meter','panel','lighting','elevator','battery']:
            point_types = []
            for j in [x for x in csv_eq_types[csv_eq_types['tag_name']==i]["critical_point_types"].to_list()[0]]:
                point_type = csv_point_types[csv_point_types['id'] == j]["tag_name"].to_list()[0]
                point_types.append(point_type)
            eq_types = [i]
            query = PointSelector()
            query.point_types = point_types
            query.equipment_types = [i]
            query.buildings = [id]
            selection = client.select_points(query)
            print(f"        Following sensors are found: {point_types}")
            pd.options.plotting.backend = "plotly" 
            if len(selection['points'])>0:
                timeseries_query = TimeseriesQuery(point_ids = selection['points'], start = start_date, end = end_date)
                points = selection["points"]
                sensor_metadata = pd.DataFrame(client.get_points_by_ids(points))
                dflist_metadata.append(sensor_metadata)
                sensor_data = points_df_from_streaming_timeseries(client.stream_point_timeseries(timeseries_query))
                if "timestamp" in sensor_data.columns:
                    sensor_data =  sensor_data.set_index("timestamp")
                    print(f"            Appending sensor data for {i}")
                    dflist_equipments.append(sensor_data)
                else:
                    print(f"¯\_(ツ)_/¯ Ahhh.... I didn't find any timeseries data for {i}")
            else:
                print(f"¯\_(ツ)_/¯ Ahhh.... I didn't find any data for {i}")
        else:
            print("¯\_(ツ)_/¯ Ahh.... No data found")
    print(f"There are {len(dflist_equipments)} equipment(s) in the dataframe")
    dflist_equipments = rename_sensors(dflist_equipments, dflist_metadata)
    return dflist_equipments,dflist_metadata,pd.DataFrame.from_dict(building_info)

def rename_sensors(sensors,metadata):
    """_summary_

    Args:
        sensors (_type_): _description_
        metadata (_type_): _description_
    """
    new_sensors = []
    id_list = []
    description_list = []
    for df in metadata:
        sublist_id = df["id"].to_list()
        for item in sublist_id:
            id_list.append(item)
        sublist_description = df["description"].to_list()
        for item in sublist_description:
            description_list.append(item)
    for df in sensors:
        key = df.columns.to_list()
        val = []
        for col in key:
            if col in id_list:
                index = id_list.index(col)
                val.append(description_list[index])
        column_rename = {key[i]: val[i] for i in range(len(key))}
        df = df.rename(column_rename,axis='columns')
        new_sensors.append(df)
    return(new_sensors)