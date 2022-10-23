# SGP HDB Resale Web App
 ## Webapp created mainly as a one stop for users to see current resale flat info 
 
 - Tab 1 focuses on what type of flats available in current month and shows where the location is 
 - User is able to select the Area of interest , Flat types and Price range at one go to filter
 - Finalized filtered available flats will be output in table where user is able to select interested flat 
 - Flat location vincity will be output in the map below seamlessly
![image](https://user-images.githubusercontent.com/63183714/197386030-a91f05a4-4565-4a04-9773-13e42342e465.png)

 - Tab 2 is additional tab with easy to use analysis tools to see general trend for past 5 months 
![image](https://user-images.githubusercontent.com/63183714/197385840-28a1d7be-3fab-44ce-92e9-4c0d72c5b8e9.png)
 
APIs used 
- "https://data.gov.sg/api/action/datastore_search" to get Resale flat info 
- "https://developers.onemap.sg/commonapi/search" to get exact longitude and latitude info from resale flat address

Languages/Framework
- Python with Pyspark heavily utilized to engineer data pulled from APIs
- Dash leaflet python library used to generate map location of the flats 
- Python Plotly library used to generate plots as seen in Tab 2 
