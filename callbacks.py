import dash
import dash_table as dt
from dash.dependencies import Input, Output, State

from app import app
import pandas as pd
import requests
import json
import urllib.request
import dash_leaflet as dl
import dash_leaflet.express as dlx
import plotly.express as px
from dash_extensions.javascript import assign
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, concat_ws, when, lit, sum
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import to_date, col, date_format, year, month, dayofmonth

spark = SparkSession.builder.master("local").appName("HDBSale").getOrCreate()


def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    return text


parameters = {
    "resource_id": "f1765b54-a209-4718-8d38-a39237f502b3",
    "sort": "month desc",
    "limit": "1000",
    # "offset": "5"
}

response = requests.get("https://data.gov.sg/api/action/datastore_search", params=parameters)
# see if able to call API successfully
# print(response)
# print(jprint(response.json()))
# want to sieve out particular values , but when do this data type is a list
# print(jprint(response.json()["result"]["records"]))
x = response.json()["result"]["records"]
# x = ["1","0","ABCDEFU"]
# create df from list
# didnt use spark.read.json(x) doesnt work maybe cos file not JSON but list
df = spark.createDataFrame(x)
# month is in string need to chagne to datetime
df = df.withColumn("date", to_date("month"))
df.printSchema()
print(f"Total number of rows retrieved {df.count()}")

# collect to retrieve a cell, if use head or first all give back row not cell
today = df.collect()[0][12]


############################### TAB 1 CALLBACKS ###############################

# to store town selected based on dropdown into mtown
@app.callback(
    Output('mtown', 'data'),
    Input('town', 'value'))
def display_value(value):
    return value


# to display flat models in dropdown based on Town selected
@app.callback(
    Output('model', 'options'),
    Input('mtown', 'data'))
def display_value(value):
    if value is None:
        # PreventUpdate prevents ALL outputs updating
        raise dash.exceptions.PreventUpdate
    else:
        print(f"Town selected is {value}")
        flat_model = []
        # filter by today
        df1 = df.filter(df["month"] == today)
        # filter by town
        df1 = df1.filter(df1["town"] == value)
        # number of unique flat types
        for n in range(df1.select("flat_model").distinct().count()):
            # append unique flat types to empty list flat_type
            flat_model.append(df1.select("flat_model").distinct().collect()[n][0])
        print(f"the flat model present in {value} are {flat_model}")
        return [
            {'label': i, 'value': i} for i in flat_model  # these list needs to be based on the API dataset
        ]


# to store model selected based on model dropdown
@app.callback(
    Output('mfm', 'data'),
    Input('model', 'value'))
def display_value(value):
    return value


# to display flat types in dropdown based on model selected
@app.callback(
    Output('flat-type', 'options'),
    Input('mfm', 'data'),
    State('mtown', 'data'))
def display_value(value, mtown):
    if value is None:
        # PreventUpdate prevents ALL outputs updating
        raise dash.exceptions.PreventUpdate
    else:
        print(f"flat model selected is {value}")
        flat_type = []
        # filter by today
        df1 = df.filter(df["month"] == today)
        # filter by town
        df1 = df1.filter(df1["town"] == mtown)
        # filter by model
        df1 = df1.filter(df1["flat_model"] == value)
        # df1.show(100)
        # number of unique flat types
        for n in range(df1.select("flat_type").distinct().count()):
            # append unique flat types to empty list flat_type
            flat_type.append(df1.select("flat_type").distinct().collect()[n][0])
        print(f"the flat types present in {value} are {flat_type}")
        return [
            {'label': i, 'value': i} for i in flat_type  # these list needs to be based on the API dataset
        ]


# to store flat type based on dropdown selected in flat type
@app.callback(
    Output('mft', 'data'),
    Input('flat-type', 'value'))
def display_value(value):
    return value


# to output price range based on flat type selected
@app.callback(
    Output('price-range', 'options'),
    Input('mft', 'data'),
    State('mtown', 'data'),
    State('mfm', 'data'))
def display_value(value, mtown, mfm):
    if value is None:
        # PreventUpdate prevents ALL outputs updating
        raise dash.exceptions.PreventUpdate
    else:
        print(f"flat type selected is {value}")
        # filter by today
        df1 = df.filter(df["month"] == today)
        # filter by town
        df1 = df1.filter(df1["town"] == mtown)
        # filter by model
        df1 = df1.filter(df1["flat_model"] == mfm)
        # filter by flat-type
        df1 = df1.filter(df1["flat_type"] == value)
        # df1.show(100)
        # change column type so can divide later
        df1 = df1.withColumn("resale_price", col("resale_price").cast(IntegerType()))
        if df1.select(df1["resale_price"]).count() < 5:
            resale_price = []
            for n in range(df1.select("resale_price").distinct().count()):
                # append all prices to empty list resale_price
                resale_price.append(df1.select("resale_price").distinct().collect()[n][0])
            return [
                {'label': i, 'value': i} for i in resale_price  # these list needs to be based on the API dataset
            ]
        else:
            price_range = []
            # find max and min
            maxx = df1.select(df1["resale_price"]).sort(df1["resale_price"], ascending=False).collect()[0][0]
            minn = df1.select(df1["resale_price"]).sort(df1["resale_price"], ascending=True).collect()[0][0]
            quarter = (maxx - minn) / 4
            highmed = maxx - quarter
            med = maxx - 2 * quarter
            medlow = maxx - 3 * quarter
            # break it down into 4 price categories
            print(maxx, highmed, med, medlow, minn)
            # use when function to categorize new column price range
            df1 = df1.withColumn("Price Range",
                                 when((minn <= df1["resale_price"]) & (medlow >= df1["resale_price"]), lit("Low")).when(
                                     (medlow <= df1["resale_price"]) & (med >= df1["resale_price"]),
                                     lit("Med-Low")).when(
                                     (med <= df1["resale_price"]) & (highmed >= df1["resale_price"]),
                                     lit("Med-High")).when(
                                     (highmed <= df1["resale_price"]) & (maxx >= df1["resale_price"]), lit("High")))
            # need to output a list of price range as options
            for n in range(df1.select("Price Range").distinct().count()):
                # append all prices to empty list resale_price
                price_range.append(df1.select("Price Range").distinct().collect()[n][0])

            return [
                {'label': i, 'value': i} for i in price_range  # these list needs to be based on the API dataset
            ]


# to store price range based on dropdown selected in price range
@app.callback(
    Output('mpr', 'data'),
    Input('price-range', 'value'))
def display_value(value):
    return value


# to output final table based on town, model, flat type and price range
# output only when selected once the last dropdown price range is selected
@app.callback(
    Output('memory-table', 'data'),
    Output('Pie', 'figure'),
    Input('mpr', 'data'),
    State('mft', 'data'),
    State('mtown', 'data'),
    State('mfm', 'data'))
def display_value(mpr, mft, mtown, mfm):
    print(f"Final price/price range is {mpr}, Flat type is  {mft}, Town is  {mtown}, Flat mode is {mfm}")
    df2 = df.filter(df["month"] == today).filter(df["town"] == mtown).filter(df["flat_model"] == mfm)
    df2 = df2.groupBy("town", "flat_model", "flat_type").count()
    total = df2.select(sum('count')).collect()[0][0]
    df2 = df2.withColumn("% Availability", df2["count"] / total * 100)
    df2.show()
    fig = px.pie(df2, values= "% Availability", names="flat_type")
    if mpr is None:
        # PreventUpdate prevents ALL outputs updating
        raise dash.exceptions.PreventUpdate
    else:
        # filter by today,town,mode, flat_type one shot and sort resale price desc
        df1 = df.filter(df["month"] == today).filter(df["town"] == mtown).filter(df["flat_model"] == mfm).filter(
            df["flat_type"] == mft)
        if df1.select(df1["resale_price"]).count() < 5:
            df1 = df1.filter(df1["resale_price"] == mpr)
            # df1.show()
            # convert to pandas df
            df1 = df1.toPandas()
            # need be dictionary format so that it can be outputted
            return df1.to_dict('records'), fig
        else:
            print(f"price range selected is {mpr} ")
            # change column type from str to int
            df1 = df1.withColumn("resale_price", col("resale_price").cast(IntegerType()))
            # RECREATE DF with PRICE RANGE COLUMN
            maxx = df1.select(df1["resale_price"]).sort(df1["resale_price"], ascending=False).collect()[0][0]
            minn = df1.select(df1["resale_price"]).sort(df1["resale_price"], ascending=True).collect()[0][0]
            quarter = (maxx - minn) / 4
            highmed = maxx - quarter
            med = maxx - 2 * quarter
            medlow = maxx - 3 * quarter
            # break it down into 4 price categories
            print(maxx, highmed, med, medlow, minn)
            # use when function to categorize new column price range
            df1 = df1.withColumn("Price Range",
                                 when((minn <= df1["resale_price"]) & (medlow >= df1["resale_price"]), lit("Low")).when(
                                     (medlow <= df1["resale_price"]) & (med >= df1["resale_price"]),
                                     lit("Med-Low")).when(
                                     (med <= df1["resale_price"]) & (highmed >= df1["resale_price"]),
                                     lit("Med-High")).when(
                                     (highmed <= df1["resale_price"]) & (maxx >= df1["resale_price"]), lit("High")))
            df1 = df1.filter(df1["Price Range"] == mpr)

            # df1.show()
            # convert to pandas df
            df1 = df1.toPandas()
            # need be dictionary format so that it can be outputted
            return df1.to_dict('records'), fig


# to output address based on cell clicked (any column)
@app.callback(Output('table_cell_data', 'data'),
              Input('memory-table', 'active_cell'),
              Input('memory-table', 'data')
              )
def table_output(active_cell, tabledata):
    # print(tabledata)
    # convert from stored dictionary to pandas df
    df1 = pd.DataFrame.from_dict(tabledata)
    if active_cell:
        # filter by today,town,flat_type one shot and sort resale price desc
        # df1 = df.filter(df["month"] == today).filter(df["town"] == mtown).filter(df["flat_model"] == mfm).filter(
        # df["flat_type"] == mft).sort(
        # df["resale_price"], ascending=False)
        # print(str(active_cell))
        # get address from selected cell
        address = df1.iloc[active_cell['row']][10]
        # active cell outputs a dict
        print(f"Address of selected cell is {address}")
        return address
    else:
        return None


# to mark location on map based address data
@app.callback(Output('geojson', 'data'),
              Input('table_cell_data', 'data')
              )
def table_output(address):
    if not address:
        return None
    else:
        # Onemap API
        # provide address as one of input

        parameters2 = {
            "searchVal": str(address),
            "returnGeom": "Y",
            "getAddrDetails": "Y",
        }

        response2 = requests.get("https://developers.onemap.sg/commonapi/search", params=parameters2)
        # see if able to call API successfully
        print(response2)
        # print Latitude and Longitude from API GET
        x = float(response2.json()["results"][0]["LATITUDE"])
        y = float(response2.json()["results"][0]["LONGITUDE"])

        # input values to points dictionary and return data to geojson
        points = [dict(lat=x, lon=y)]
        data = dlx.dicts_to_geojson(points)

        # update map based on x and y
        return data


############################### TAB 2 CALLBACKS ###############################

# to store town selected based on dropdown into mtown
# to out put flat models based on town selected
@app.callback(
    Output('m2town', 'data'),
    Output('2fm', 'options'),
    Input('2town', 'value'))
def display_value(value):
    # town gives a list
    print(f"Town selected for analysis is {value}")
    flat_model = []
    # new isin expression to see if the values in argument inside the column
    dft = df.filter(df["town"].isin(value))
    for n in range(dft.select("flat_model").distinct().count()):
        # append unique flat types to empty list flat_type
        flat_model.append(dft.select("flat_model").distinct().collect()[n][0])
    print(f"the flat types present in {value} are {flat_model}")
    return value, [
        {'label': i, 'value': i} for i in flat_model  # these list needs to be based on the API dataset
    ]


# to store flat model selected based on flat model dropdown
# to output flat types options based on flat model selected
@app.callback(
    Output('m2fm', 'data'),
    Output('2ft', 'options'),
    Input('2fm', 'value'))
def display_value(value):
    # flat model gives a list
    print(f"Flat model selected for analysis is {value}")
    flat_type = []
    # new isin expression to see if the values in argument inside the column
    dft = df.filter(df["flat_model"].isin(value))
    for n in range(dft.select("flat_type").distinct().count()):
        # append unique flat types to empty list flat_type
        flat_type.append(dft.select("flat_type").distinct().collect()[n][0])
    print(f"the flat types present in {value} are {flat_type}")
    return value, [
        {'label': i, 'value': i} for i in flat_type  # these list needs to be based on the API dataset
    ]


# just store flat types selected in the drop down
@app.callback(
    Output('m2ft', 'data'),
    Input('2ft', 'value'))
def display_value(value):
    return value
    # convert resale price into integer using cast method on col while keeping same column name
    # dft = dft.withColumn("resale_price", col("resale_price").cast(IntegerType()))
    # dft.printSchema() to check if got change to int type
    # can multiple group by, take note resale_price changes to a new column name
    # dft = dft.groupBy("flat_type", "date").avg("resale_price")
    # convert to pandas df
    # dft = dft.toPandas()
    # sort date value cos px.line plots as it is without ordering so will have zig zag lines
    # dft = dft.sort_values(by="date")
    # plot based on the 3 columns in PANDAS DATAFRAME

    # need parition!!!!!
    # fig = px.line(dft, x="date", y="avg(resale_price)", color='flat_type')
    # Never return None here it'll hang, try return {'data': [
    #             {'x': [0], 'y': [0]} for example


# to out put graph once final flat type selected
@app.callback(
    Output('Graph1', 'figure'),
    Input('m2ft', 'data'),
    Input('m2fm', 'data'),
    Input('m2town', 'data')
)
def display_value(flattype, flatmodel, town):
    print(town, flatmodel, flattype)
    # convert resale price into integer using cast method on col while keeping same column name
    dft = df.withColumn("resale_price", col("resale_price").cast(IntegerType()))
    if town and not flatmodel and not flattype:
        print("town")
        dft = dft.filter(df["town"].isin(town))
        # dft.show(100)
        # can multiple group by, take note resale_price changes to a new column name
        dft = dft.groupBy("town", "date").avg("resale_price")
        # convert to pandas df
        dft = dft.toPandas()
        # sort date value cos px.line plots as it is without ordering so will have zig zag lines
        dft = dft.sort_values(by="date")
        # plot based on the 3 columns in PANDAS DATAFRAME
        fig = px.line(dft, x="date", y="avg(resale_price)", color='town')
        return fig
    elif town and flatmodel and not flattype:
        print("town and flatmodel")
        dft = dft.filter(df["town"].isin(town))
        dft = dft.filter(df["flat_model"].isin(flatmodel))
        # dft.show(100)
        # can multiple group by, take note resale_price changes to a new column name
        dft = dft.groupBy("town", "flat_model", "date").avg("resale_price")
        # merge town, flat model into 1 column with _ seperator for use in color in the graph
        # inside concat , need input the other 2 columns avg sale ... and date so that final dft has those columns as well
        dft = dft.select(concat_ws('_', "town", "flat_model"), "avg(resale_price)", "date")
        # convert to pandas df
        dft = dft.toPandas()
        # sort date value cos px.line plots as it is without ordering so will have zig zag lines
        dft = dft.sort_values(by="date")
        # plot based on the 3 columns in PANDAS DATAFRAME
        fig = px.line(dft, x="date", y="avg(resale_price)", color='concat_ws(_, town, flat_model)')
        return fig
    elif town and flatmodel and flattype:
        print("town and flatmodel and flattype")
        dft = dft.filter(df["town"].isin(town))
        dft = dft.filter(df["flat_model"].isin(flatmodel))
        dft = dft.filter(df["flat_type"].isin(flattype))
        # dft.show(100)
        # can multiple group by, take note resale_price changes to a new column name
        dft = dft.groupBy("town", "flat_model", "flat_type", "date").avg("resale_price")
        # merge town, flat model into 1 column with _ seperator for use in color in the graph
        # inside concat , need input the other 2 columns avg sale ... and date so that final dft has those columns as well
        dft = dft.select(concat_ws('_', "town", "flat_model", "flat_type"), "avg(resale_price)", "date")
        # convert to pandas df
        dft = dft.toPandas()
        # sort date value cos px.line plots as it is without ordering so will have zig zag lines
        dft = dft.sort_values(by="date")
        # plot based on the 3 columns in PANDAS DATAFRAME
        fig = px.line(dft, x="date", y="avg(resale_price)", color='concat_ws(_, town, flat_model, flat_type)')
        return fig
    else:
        return {}  # return empty graph at the start
