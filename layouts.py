import dash_table
import dash_core_components as dcc
import dash_html_components as html
from callbacks import *
import dash_leaflet as dl
import dash_leaflet.express as dlx
from dash_extensions.javascript import assign

# Cool, dark tiles by Stadia Maps.
url = 'https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png'
attribution = '&copy; <a href="https://stadiamaps.com/">Stadia Maps</a> '

print(f"hello today is {today}")
# filter df for latest date(today) df
# df1 new
df1 = df.filter(df["month"] == today)

# append list the towns available in today
town = []
for n in range(df1.select("town").distinct().count()):
    town.append(df1.select("town").distinct().collect()[n][0])
print(f"All towns available this month {town}")

# df is the entire dataframe no filter
# append list to get all the flat models
flat_model_all = []
for n in range(df.select("flat_model").distinct().count()):
    flat_model_all.append(df.select("flat_model").distinct().collect()[n][0])
print(f"All flat model available in whole df {flat_model_all}")

# append list to get all the flat types
flat_type_all = []
for n in range(df.select("flat_type").distinct().count()):
    flat_type_all.append(df.select("flat_type").distinct().collect()[n][0])
print(f"All flat types available in whole df {flat_type_all}")

# append list to get all the town
town_all = []
for n in range(df.select("town").distinct().count()):
    town_all.append(df.select("town").distinct().collect()[n][0])
print(f"All the towns available in whole df {town_all}")

# for leaflet,like default
point_to_layer = assign("function(feature, latlng, context) {return L.circleMarker(latlng);}")

layout1 = html.Div([
    html.H1('WELCOME TO HDB RESALE INFO WEBSITE'),
    dcc.Tabs([
        dcc.Tab(label='Available flats for this month', children=[
            html.Div([
                dcc.Store(id='mtown'),
                dcc.Dropdown(
                    id='town',
                    options=[
                        {'label': i, 'value': i} for i in town  # these list needs to be based on the API dataset
                    ],
                    placeholder="Please select Area of interest",
                )]),
            html.Div([
                dcc.Store(id='mfm'),
                dcc.Dropdown(
                    id='model',
                    # options to be updated via callback
                    placeholder="Please select Flat Model",
                )]),
            html.Div([
                dcc.Store(id='mft'),
                dcc.Dropdown(
                    id='flat-type'
                    # options to be updated via callback
                    ,
                    placeholder="Please select Flat type"
                )]),
            html.Div([
                dcc.Store(id='mpr'),
                dcc.Dropdown(
                    id='price-range'
                    # options to be updated via callback
                    ,
                    placeholder="Please select price/price range (SGD)"
                )]),

            html.H1(""),
            # for outputting a table use this
            html.Div([
                dcc.Store(id='table_cell_data'),
                dash_table.DataTable(
                    id='memory-table',
                    # defualt columns
                    columns=[{'name': i, 'id': i} for i in
                             ["block", "floor_area_sqm", "remaining_lease", "resale_price",
                              "storey_range", "street_name"]]
                )]),

            html.H1(""),

            html.H3("Block Vicinity"),
            # map component
            html.Div([
                dl.Map(children=[dl.TileLayer(), dl.GeoJSON(data=None, id="geojson", zoomToBounds=True,
                                                            options=dict(pointToLayer=point_to_layer))
                                 ])
            ], style={'width': '100%', 'height': '50vh', 'margin': "auto", "display": "block", "position": "relative"}),

            # pie chart
            html.Div(
                [dcc.Graph(id="Pie")]),

        ])
        ,


        dcc.Tab(label='Resale Flat Trends', children=[
            # drop down to select which town and store in m2town
            html.Div([
                dcc.Store(id='m2town'),
                dcc.Dropdown(
                    id='2town',
                    options=[
                        {'label': i, 'value': i} for i in town_all  # these list needs to be based on the API dataset
                    ],
                    placeholder="Please select Area of interest",
                    multi=True,
                )]),
            # drop down to select respective flat models based on m2town
            html.Div([
                dcc.Store(id='m2fm'),
                dcc.Dropdown(
                    id='2fm',
                    # options to be updated via callback
                    placeholder="Please select flat model",
                    multi=True,
                )]),
            # drop down to select respective flat types based on m2town
            html.Div([
                dcc.Store(id='m2ft'),
                dcc.Dropdown(
                    id='2ft',
                    # options to be updated via callback
                    placeholder="Please select flat type",
                    multi=True,
                )]),


            # 1. Resale price vs time trend by flat type
            html.Div(
                [dcc.Graph(id="Graph1")])
            # 3. Resale price vs Lease remaining

            # 4. Resale price vs floor area square meter

        ])

    ])

])
