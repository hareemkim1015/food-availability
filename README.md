## 405 Final Project: Food Manufacturer Crop Supply Analysis

### Project Overview

This project analyzes crop production data and food ingredient data to provide insights into food manufacturer crop supply trends. The pipeline transforms and aggregates data using Spark, and stores the results in a DuckDB database for querying and analysis.

### Data Sources

1. **crop_production_data.csv** : Contains crop production statistics from the USDA.
   * This dataset was preprocessed by:
        * Filtering to include data after February 23, 2020.
        * Removing rows containing '%other%', '%total%', '%hay%', '%cotton%', and '%tobacco%' in `COMMODITY_DESC`.
        * Restricting `STATISTICCAT_DESC` to production-related metrics: AREA HARVESTED, PRODUCTION, YIELD, SALES, and AREA PLANTED.
2. **ingredient_data.csv** : Contains food product ingredient data from the USDA.
   * This dataset was filtered to include data from the 12 manufacturers with the highest number of products. The "highest number of products" was determined by counting the number of rows associated with each manufacturer.
   * The manufacturers included are: Wal-Mart Stores, Target Stores, Meijer, Safeway, General Mills Sales Inc., Topco Associates, The Kroger Co., Hy-Vee, Supervalu, Whole Foods Market, Wakefern Food Corporation, and Wegmans Food Markets.
   
Both datasets can be downloaded via Google Drive (https://drive.google.com/drive/folders/12WfjnOqM5fDXA7AgmeOR3_BnaCrt1n6G?usp=sharing).
After downloading both files, please store them in your local `/Users/yourusername/Documents/` folder and ensure they are named exactly as follows:

* `crop_production_data.csv`
* `ingredient_data.csv`
  
### Pipeline Steps

1.  **Data Cleaning and Transformation (Spark):** Cleans and transforms the CSV data.
2.  **Data Aggregation and Analysis (Spark):** Aggregates the cleaned data.
3.  **Data Storage (DuckDB):** Stores the processed data in a DuckDB database.
4.  **Data Analysis and Querying (DuckDB):** Allows querying and analysis of the data.

### Executing the Pipeline

1.  Clone the github repository.
2.  From the directory in which the 'Team13' Github repo is stored, run the following command:
   
  ```bash
  cd team13/bash && ./pipeline.sh
  ```
(Before running the above command, use `chmod +x pipeline.sh` if the file needs to be made executable.)

### Tableau Visualization
A dashboard visualization of this data can be found at this link: https://public.tableau.com/views/MGMTMSA405_FinalProject_Viz/FinalDashboard?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

### Repository Structure

* `/bash`: Contains the `pipeline.sh` script.
* `/python`: Contains the Spark Python script.
* `/duckdb_sql`: Contains the DuckDB SQL scripts.
