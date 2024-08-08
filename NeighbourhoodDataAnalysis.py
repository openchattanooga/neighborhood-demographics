import json
from pyspark.sql.functions import col, lit
import requests
import geopandas as gpd
from shapely.geometry import Polygon
from pyspark.sql import SparkSession
import pandas as pd

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Read API Data") \
    .getOrCreate()

def create_gdf(coords):
    polygon = Polygon(coords)
    gdf_polygon = gpd.GeoDataFrame({'geometry': [polygon]}, crs='EPSG:4269')
    census_blocks = gpd.read_file('tl_2023_47_tabblock20.shp')

    # Ensure the coordinate reference system (CRS) matches for both datasets
    # census_blocks = census_blocks.to_crs(epsg=4269)  # Example CRS; adjust as necessary

    # Perform the spatial query
    blocks_within_polygon = census_blocks[census_blocks.geometry.within(gdf_polygon.geometry.iloc[0])]

    pandas_df = blocks_within_polygon.reset_index()
    if 'geometry' in pandas_df.columns:
        pandas_df = pandas_df.drop(columns=['geometry'])

    return pandas_df

def getNeighbourhoodBoundaries():
    # URL of the API
    url = "https://www.chattadata.org/resource/dxzz-idjy.json"
    params = {
        '$$app_token': 'A3oVhOypNkb3tNozBUlaCZGIy'
    }

    # Make a request to the API
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Convert JSON data to a Spark DataFrame
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))

        # Flatten and select the `coordinates` column
        df2 = df.select("name", "description", "boundary.coordinates").withColumn("b2", col("coordinates")[0][0])
        df2.show(2, truncate=False)

        # Collect the data to the driver
        coordinates_nb = df2.select("name", "b2").rdd.collectAsMap()

        # Process each neighbourhood
        results = []
        for name, coords in coordinates_nb.items():
            pandas_df = create_gdf(coords)
            pandas_df['Neighbourhood'] = name
            results.append(pandas_df)

        # Concatenate all the results into a single Pandas DataFrame
        final_pandas_df = pd.concat(results, ignore_index=True)

        # Convert the Pandas DataFrame back to a Spark DataFrame
        final_spark_df = spark.createDataFrame(final_pandas_df)

        # Show the final Spark DataFrame
        final_spark_df.show(2, truncate=False)
        final_spark_df.write.format('csv') \
            .mode('overwrite') \
            .option('header', 'true') \
            .save('neighbourhood_block_data')


    else:
        print(f"Failed to retrieve data: {response.status_code}")

def getCensusDatabyBlock():

    # Replace 'YOUR_API_KEY' with your actual API key
    API_KEY = '9fb6010b0d49766a5136431d44aa18e259a51dd9'

    # Define the base URL for the API
    BASE_URL = 'https://api.census.gov/data/2020/dec/pl'

    # Specify the parameters for your query
    params = {
        'get': 'GEO_ID,P1_001N,P1_002N,P1_003N,P1_004N,P1_005N,P1_006N,P1_007N,P1_008N,P1_009N,H1_001N,H1_002N,H1_003N,P5_003N,P5_004N,P5_005N,P5_008N,P5_009N,P5_010N,P4_002N',  # Replace with the data you need, e.g., population
        'for': 'block:*',
        'in': 'state:47 county:065',  # Replace with the state and county FIPS codes
        'key': API_KEY
    }

    # Make the API request
    response = requests.get(BASE_URL, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Convert the response to JSON
        data = response.json()

        # Convert the JSON data to a pandas DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])

        spark_df_census = spark.createDataFrame(df)
        spark_df_census.write.format('csv') \
            .mode('overwrite') \
            .option('header', 'true') \
            .save('CensusDatabyBlock')

        # Show the PySpark DataFrame
        spark_df_census.show(truncate=False)

        # Display the first few rows of the DataFrame
        #print(df.head())
    else:
        print(f'Error: {response.status_code}')
        print(response.text)

getCensusDatabyBlock()
getNeighbourhoodBoundaries()


#############################################################

#Read Neighborhood Block data
read_df = spark.read.format('csv').option('header',True).load('neighbourhood_block_data')
read_df.createOrReplaceTempView("spark_df_blocks_nbs")


#Read Census Block data
read_df2 = spark.read.format('csv').option('header',True).load('CensusDatabyBlock')
read_df2.createOrReplaceTempView("spark_df_census_data")

query = """
        SELECT 
    Neighbourhood, 
    sum(P1_001N) as Total_Population,
    sum(P1_002N) as Total_Population_of_one_race,
    sum(P1_009N) as Total_Population_of_two_or_more_races,
    sum(P1_003N) as Total_Population_White_alone,
    sum(P1_004N) as Total_Population_Black_or_African_American_alone,
    sum(P1_005N) as Total_Population_American_Indian_and_Alaska_Native_alone,
    sum(P1_006N) as Total_Population_Asian_alone,
    sum(P1_007N) as Total_Population_Native_Hawaiian_and_Other_Pacific_Islander_alone,
    sum(P4_002N) as Total_PopulationHispanic_or_Latino,
    sum(P1_008N) as Total_Population_Some_Other_Race_alone,
    sum(H1_001N) as Total_Housing_Units,
    sum(H1_002N) as Total_Occupied_Housing_Units,
    sum(H1_003N) as Total_Vacant_Housing_Units,
    sum(P5_003N) as Correctional_facilities_for_adults,
    sum(P5_004N) as Juvenile_facilities,
    sum(P5_005N) as Nursing_facilities,
    sum(P5_008N) as College_University_student_housing,
    sum(P5_009N) as Military_quarters,
    sum(ALAND20) as Total_Land_Area,
    sum(AWATER20) as Total_Water_Area
FROM 
    spark_df_census_data c 
JOIN 
    spark_df_blocks_nbs n 
ON 
    c.block = n.BLOCKCE20 
    AND c.tract = n.TRACTCE20
GROUP BY 
    Neighbourhood;

        """

result_df = spark.sql(query)
result_df.show(truncate=False)
result_df.write.format('csv') \
    .mode('overwrite') \
    .option('header', 'true') \
    .save('analysis_nb')