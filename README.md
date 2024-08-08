Overview
The NeighborhoodDataAnalysis code fetches the demographic information of Chattanooga neighborhood populations.

Pre-requisites:
pip install pyspark requests geopandas shapely pandas
Download the TigerLine Shape File - tl_2023_47_tabblock20.shp - from census.gov. Replace the file path on line 17.

Run Instructions:
Run the Python file: NeighborhoodDataAnalysis.py
Generates a CSV data file in the folder path: analysis_nb

Resources Used:
Neighborhood Association Geographic Boundaries from API: chattadata.org
TIGER/Line Shapefiles from census.gov
Decennial Census 2020 Redistricting Data (PL 94-171) from Census API: census.gov/data

Code Logic:
Fetching neighborhood boundaries data from Neighborhood Association Boundaries API - chattadata.org

Fetching census block data from the U.S. Census Bureau API - Decennial Census 2020 Redistricting Data (PL 94-171) - census.gov/data

Performing spatial queries to match census blocks within neighborhood boundaries using TIGER/Line Shapefiles from census.gov

Aggregating and analyzing data to produce a summary of population and housing statistics by neighborhood.

The results are saved to a CSV file under the analysis_nb folder. The CSV file contains the following output columns for analysis for each neighborhood:

Total_Population
Total_Population_of_one_race
Total_Population_of_two_or_more_races
Total_Population_White_alone
Total_Population_Black_or_African_American_alone
Total_Population_American_Indian_and_Alaska_Native_alone
Total_Population_Asian_alone
Total_Population_Native_Hawaiian_and_Other_Pacific_Islander_alone
Total_PopulationHispanic_or_Latino
Total_Population_Some_Other_Race_alone
Total_Housing_Units
Total_Occupied_Housing_Units
Total_Vacant_Housing_Units
Correctional_facilities_for_adults
Juvenile_facilities
Nursing_facilities
College_University_student_housing
Military_quarters
Total_Land_Area
Total_Water_Area