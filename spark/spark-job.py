import os

from pyspark import SparkContext
sc = SparkContext("local[*]", "FoodProduction")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, first, asc,
    sum as _sum, when, to_date, date_trunc, upper, lower, trim, monotonically_increasing_id, explode, split, row_number)
from pyspark.sql.window import Window

def main():
    crop_prod_path = os.path.expanduser("~/Documents/crop_production_data.csv")
    ingredient_path = os.path.expanduser("~/Documents/ingredient_data.csv")
    
    spark = SparkSession.builder.getOrCreate()
 
    # Load the crops data
    crop_prod = spark.read.csv(crop_prod_path, header=True)

    # The USDA dataset contains multiple fields that are irrelevant to our project.
    # Therefore, we will only keep the following columns: Crop, Status, Load Time, Value, Unit
    crop_prod = crop_prod.select(
        lower(col("COMMODITY_DESC")).alias("crop"),
        lower(col("STATISTICCAT_DESC")).alias("status"),
        col("LOAD_TIME").alias("load_time"),
        col("VALUE").alias("value"),
        lower(col("UNIT_DESC")).alias("unit")
    )

    # Cleaning the data
    
    # 1. Some of our values inside the VALUE column are marked with non-numerical values. 
    # As non-numerical values cannot be aggregated or be converted to a numerical data type, we will remove these rows. 
    crop_prod = crop_prod.filter(col("value").rlike("^[0-9.,]+$"))

    # 2. Some values are also marked as zero, which we will remove. 
    crop_prod = crop_prod.filter(col("value") != "0")
    
    # 3. Some of the values in the VALUE column contain commas. 
    # We need to remove the commas in order to convert the data into a numerical data type.
    crop_prod = crop_prod.withColumn(
        "value", 
        regexp_replace(col("value"), "[,]", "")
    )

    # 4. Some of our data contain rows where the UNIT column is simply marked as 'OPERATIONS'. 
    # As we cannot identify what these rows indicate, we will remove them. 
    crop_prod = crop_prod.filter(col("unit") != "operations")
    
    # 5. Since our dataset contains various units (e.g., CWT, LB, TONS), we need to standardize them into a single unit for easier aggregation. 
    # We will choose LB (pounds) as the base unit, as it is the most fundamental and will allow us to easily convert to CWT or TONS later if necessary.
    
    # First, to simplify the aggregation process, we will clean up the unit column. 
    # Some crops are recorded with different notations based on the basis (e.g., 'Tons, dry basis' vs 'Tons, fresh basis').
    # We will standardize these units by removing the basis information, so that they are treated as the same unit (e.g., 'Tons')."
    crop_prod = crop_prod.withColumn(
        "unit", 
        regexp_replace("unit", r",\s*in\s.*|,\s*.*basis", "")
    )
    
    # Next, we will convert CWT and TONS into LB (pounds).
    # Apply unit conversion (multiplying value by respective conversion rate)
    crop_prod = crop_prod.withColumn(
        "value",
        when((col("status") == "production") & (col("unit"). isin(["ton", "tons"])), col("value") * 2000)
        .when((col("status")== "production") & (col("unit") == "cwt"), col("value") * 100)
        .when((col("status") == "yield") & (col("unit") == "tons / acre"), col("value") * 2000)
        .when((col("status") == "yield") & (col("unit") == "cwt / acre"), col("value") * 100)
        .otherwise(col("value"))
    )
    
    # Update the unit column
    crop_prod = crop_prod.withColumn(
        "unit",
        when((col("unit") == "tons") | (col("unit") == "cwt"), "lb")
        .when((col("unit") == "tons / acre") | (col("unit") == "cwt / acre"), "lb / acre")
        .otherwise(col("unit"))
    )
    
    # Some of our area units are marked as 'Cuerdas'. We will convert these into acres. 
    # Apply unit conversion on 'total_value' for cuerdas → acres
    crop_prod = crop_prod.withColumn(
        "value",
        when((col("status") == "area harvested") & (col("unit") == "cuerdas"), col("value") * 0.97)
        .otherwise(col("value"))
    )
    
    # Update the unit column only if it was in CUERDAS
    crop_prod = crop_prod.withColumn(
        "unit",
        when(col("unit") == "cuerdas", "acres")
        .otherwise(col("unit"))
    )

    # Some of our area units are also marked as 'SQ FT'. We will convert these into acres. 
    # Apply unit conversion on 'total_value' for cuerdas → acres
    crop_prod = crop_prod.withColumn(
        "value",
        when((col("status") == "area harvested") & (col("unit") == "sq ft"), col("value") * 0.000023)
        .otherwise(col("value"))
    )
    
    # Update the unit column only if it was in 'SQ FT'
    crop_prod = crop_prod.withColumn(
        "unit",
        when(col("unit") == "sq ft", "acres")
        .otherwise(col("unit"))
    )

    # We will drop any rows that do not have acres as their area unit. 
    crop_prod = crop_prod.filter(
    (~(col("status") == "area harvested")) | 
    ((col("status") == "area harvested") & (col("unit") == "acres"))
    )
    
    crop_prod = crop_prod.filter(
    (~(col("status") == "area planted")) |  
    (~((col("unit").rlike("pct")) & (col("status") == "area planted")))
    )

    # Some ingredients have multiple units in their production column. 

    # Delete rows with '$', 'PCT', 'PCT BY TYPE', '$, PHD EQUIV', 'HUNDREDS', 'THOUSANDS' in production column
    crop_prod = crop_prod.filter(
    (~(col("status") == "production")) |
    (~(col("unit").isin(["pct", "$", "hundreds", "thousands", "phd equiv", "pct by type"])))
    ) 
    
    # We will clean the data by only keeping the units that align with the crop's yield column.
    # Define the crops to filter
    crops_to_filter = ['wheat', 'oranges', 'peas', 'sorghum', 'avocados', 'grapefruit', 'lemons']
    
    # Apply the filter only to the specified crops
    crop_prod = crop_prod.filter(
        (~col("crop").isin(crops_to_filter)) |  
        (~((col("crop").isin(crops_to_filter)) & (col("status") == "production"))) | 
        (
            ((col("crop") == 'wheat') & (col("status") == "production") & (col("unit") == "bu")) |
            ((col("crop") == 'oranges') & (col("status") == "production") & (col("unit") == "boxes")) |
            ((col("crop") == 'peas') & (col("status") == "production") & (col("unit") == "lb")) |
            ((col("crop") == 'sorghum') & (col("status") == "production") & (col("unit") == "bu")) |
            ((col("crop") == 'avocados') & (col("status") == "production") & (col("unit") == "lb")) |
            ((col("crop") == 'grapefruit') & (col("status") == "production") & (col("unit") == "boxes")) |
            ((col("crop") == 'lemons') & (col("status") == "production") & (col("unit") == "boxes"))
        )
    )

    # Corns are both measured in pounds and bushels. We will convert all data into bushels. 
    # 1 bushel of corn = 56 pounds
    crop_prod = crop_prod.withColumn(
        "value",
        when((col("crop") == "corn") & (col("status") == "production") & (col("unit") == "lb"), col("value") * 0.017857)
        .when((col("crop") == "corn") & (col("status") == "yield") & (col("unit") == "lb / acre"), col("value") * 0.017857)
        .otherwise(col("value"))
    )

    # Update the unit column
    crop_prod = crop_prod.withColumn(
        "unit",
        when((col("crop") == "corn") & (col("unit") == "lb") , "bu")
        .when((col("crop") == "corn") & (col("unit") == "lb / acre"), "bu / acre")
        .otherwise(col("unit"))
    )

    # Cranberries are both measured in pounds and barrels. We will convert all data into barrels. 
    # 1 barrel of cranberries = 100 pounds
    crop_prod = crop_prod.withColumn(
        "value",
        when((col("crop") == "cranberries") & (col("status") == "production") & (col("unit") == "lb"), col("value") * 0.01)
        .when((col("crop") == "cranberries") & (col("status") == "yield") & (col("unit") == "lb / acre"), col("value") * 0.01)
        .otherwise(col("value"))
    )

    # Update the unit column
    crop_prod = crop_prod.withColumn(
        "unit",
        when((col("crop") == "cranberries") & (col("unit") == "lb") , "barrels")
        .when((col("crop") == "cranberries") & (col("unit") == "lb / acre"), "barrels / acre")
        .otherwise(col("unit"))
    )
    
    # We will drop any rows that do not have $ as their sales unit. 
    crop_prod = crop_prod.filter(
    (~(col("status") == "sales")) | 
    ((col("status") == "sales") & (col("unit") == "$")) 
    )

    # Now that we have cleaned the data, we will begin the data aggregation process on the crops dataset. 
    # 1. In order to start the data aggregation process, we need to change the data type of LOAD_TIME & VALUE
    # We will also drop the 'DD' from our date data. 
    crop_prod = crop_prod.withColumn("date", to_date(date_trunc("MM", col("load_time")))) \
                       .withColumn("value", col("value").cast("double"))

    # Since we would like to see the products in the order of the actual production cycle, we need to create a custom
    # order for the status. The production cycle is as follows:
    # Area Planted -> Area Harvested -> Yield (Total amount produced / Total amount of land harvested) -> Production -> Sales
    # Create a custom order for the 'status'
    crop_prod = crop_prod.withColumn(
        "status_order",
        when(col("status") == "area planted", 1)
        .when(col("status") == "area harvested", 2)
        .when(col("status") == "yield", 3)
        .when(col("status") == "production", 4)
        .when(col("status") == "sales", 5)
        .otherwise(6)
    ) 

    # 2. Group by crop, date, and status, then aggregate
    crop_prod_agg = crop_prod.groupBy("crop", "date", "status", "status_order", "unit")
    crop_prod_agg = crop_prod_agg.agg(_sum("value").alias("value"))
  
    # 3. Sort by crop (alphabetical) and date
    crop_output = crop_prod_agg.orderBy("crop", "date", "status_order")
    
    # Second Dataset:
    
    # Import food product data
    brand_crop_prod = spark.read.csv(ingredient_path, header=True)
    
    # Remove discontinued products and only select relevant columns
    brand_crop_prod = brand_crop_prod.filter(col("discontinued_date").isNull()).select(
        upper(col("brand_owner")).alias("brand"), # brand owner and make everything capital 
        col("fdc_id").alias("id"), # product id
        trim(lower(col("ingredients"))).alias("ingredients") # ingredients list
    )
    
    # Clean ingredients column by removing non-ingredient words like 'made with'
    brand_crop_prod = brand_crop_prod.withColumn(
        "ingredients", regexp_replace(col("ingredients"), r"(?i)\b(ingredients:|made with|made from|filling|prepared|natural)\b[:\s]*", "")
    )
    # Define crop list which comes from other data set - not including this makes majority of ingredients non-crops (i.e niacin or riboflavin)
    crop_list = [
        "alcohol coproducts", "almonds", "apples", "apricots", "artichokes", "asparagus", "avocados", "bananas", "barley", "beans",
        "beets", "blackberries", "blueberries", "boysenberries", "breadfruit", "broccoli", "brussels sprouts", "buckwheat", "cabbage", "cake & meal",
        "camelina", "canola", "carrots", "cassava", "cauliflower", "celery", "cherries", "chickpeas", "chicory", "chironjas",
        "citrons", "coconuts", "coffee", "coriander", "corn", "cranberries", "cucumbers", "daikon", "dasheens", "dates",
        "dill", "eggplant", "emmer & spelt", "escarole & endive", "figs", "flaxseed", "flour", "garlic", "ginger root", "ginseng",
        "gourds", "grain", "grapefruit", "grapes", "grasses", "greens", "guar", "hazelnuts", "hemp", "herbs",
        "herbs & spices", "hops", "horseradish", "jojoba", "kiwifruit", "legumes", "lemons", "lemons & limes", "lentils", "lettuce",
        "macadamias", "mangoes", "maple syrup", "melons", "millet", "millfeed", "mint", "miscanthus", "mustard", "nectarines",
        "oats", "oil", "okra", "olives", "onions", "oranges", "papayas", "parsley", "parsnips", "passion fruit",
        "peaches", "peanuts", "pears", "peas", "pecans", "peppers", "pineapples", "pistachios", "plantains", "plums",
        "popcorn", "potatoes", "prunes", "pumpkins", "quenepas", "radishes", "rambutan", "rapeseed", "raspberries", "rhubarb",
        "rice", "root celery", "roots & tubers", "rye", "safflower", "sesame", "silage", "small grains", "sorghum", "soursops",
        "soybeans", "spinach", "squash", "starfruit", "strawberries", "sugarbeets", "sugarcane", "sunflower", "sweet corn", "sweet potatoes",
        "switchgrass", "tangerines", "taniers", "taro", "tomatoes", "triticale", "turnips", "walnuts", "watercress", "wheat",
        "wild rice", "yams"
    ]
    # Split ingredients by spaces, explode, and filter only to include ingredients in crop list to reveal ingredients in each product and make easily joins easier later
    brand_crop_prod = brand_crop_prod.withColumn("ingredient_index", monotonically_increasing_id()) \
                                    .withColumn("ingredients_array", split(col("ingredients"), r",\s*|and\s*")) \
                                    .withColumn("ingredient", explode("ingredients_array")) \
                                    .filter(col("ingredient").isin(crop_list)) \
                                    .drop("ingredients_array")

    # Trim spaces
    brand_crop_prod = brand_crop_prod.withColumn("ingredient", trim(col("ingredient")))

    # Limit to 5 ingredients per product
    window_spec = Window.partitionBy("id").orderBy("ingredient_index")
    brand_crop_prod = brand_crop_prod.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") <= 5)
    brand_crop_prod = brand_crop_prod.drop("row_num", "ingredient_index")
    
    # Join crops data with ingredients data 
    final = (
        crop_output.join(brand_crop_prod, crop_output.crop == brand_crop_prod.ingredient)
        .select(
            brand_crop_prod["brand"], 
            brand_crop_prod["id"].cast("int"),
            brand_crop_prod["ingredient"], 
            crop_output["date"].cast("date"),
            crop_output["status"],
            crop_output["status_order"].cast("int"), #leaving the order variable in for DuckDB queries
            crop_output["value"].cast("double"),
            crop_output["unit"]
        )
    )
        
    # Order results
    final = final.orderBy('brand', 'id', 'ingredient', 'status_order', 'date')

    # Write output to parquet, 100000 files per file.
    final.write \
       .option("header", "true") \
       .option("maxRecordsPerFile", 100000) \
       .mode("overwrite") \
       .parquet("../output")

if __name__ == "__main__":
    main()
