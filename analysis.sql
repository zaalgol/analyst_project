-- Create table pointing to the S3 data with correct partitioning

-- These tables are created in AWS Athena that doesn't soppurt indexing. But arraging commonly 
-- queried fields (e.g., Nationality, Club, Continent) are near the start of the dataset can help reduce the query scanning time.
-- When using other platforms like AWS-Glue, consider using indexes.


-- Partittion by club or Nationality is more efftient for update large data, but the etl project will take longer to run. 
CREATE EXTERNAL TABLE fifa_players (
    Nationality string,
    Club string,
    Name string,
    Age int,
    `Fifa Score` int,
    Value DOUBLE,
    Salary DOUBLE,
    updated_at TIMESTAMP
)
PARTITIONED BY (Continent string)
STORED AS PARQUET
LOCATION 's3://zaalgol-players-data/output/';


ALTER TABLE fifa_players ADD
PARTITION (Continent='Europe') LOCATION 's3://zaalgol-players-data/output/Continent=Europe/'
PARTITION (Continent='South America') LOCATION 's3://zaalgol-players-data/output/Continent=South America/'
PARTITION (Continent='Unknown') LOCATION 's3://zaalgol-players-data/output/Continent=Unknown/'
PARTITION (Continent='Africa') LOCATION 's3://zaalgol-players-data/output/Continent=Africa/'
PARTITION (Continent='North America') LOCATION 's3://zaalgol-players-data/output/Continent=North America/'
PARTITION (Continent='Asia') LOCATION 's3://zaalgol-players-data/output/Continent=Asia/'
PARTITION (Continent='Oceania') LOCATION 's3://zaalgol-players-data/output/Continent=Oceania/';



-- Top 3 countries with highest income through players:
SELECT 
    Nationality,
    SUM(Salary) AS Total_Income
FROM 
    fifa_players
GROUP BY 
    Nationality
ORDER BY 
    Total_Income DESC
LIMIT 3;

-- Club with the most valuable players (based on average player value)
SELECT 
    Club,
    COUNT(*) AS Player_Count,
    AVG(Value) AS Avg_Player_Value
FROM 
    fifa_players
WHERE 
    Value > 0 -- Exclude players with no value
GROUP BY 
    Club
HAVING 
    COUNT(*) > 1 -- Only include clubs with more than one player
ORDER BY 
    Avg_Player_Value DESC
LIMIT 1;


-- Top 5 clubs with highest salary expenditure:
SELECT 
    Club,
    SUM(Salary) AS Total_Salary_Expenditure
FROM 
    fifa_players
GROUP BY 
    Club
ORDER BY 
    Total_Salary_Expenditure DESC
LIMIT 5;

-- Comparing Europe and America for best FIFA players:
SELECT 
    Continent,
    AVG("Fifa Score") as Avg_Fifa_Score,
    COUNT(*) as Player_Count
FROM 
    fifa_players
WHERE 
    Continent IN ('Europe', 'South America')
GROUP BY 
    Continent
ORDER BY 
    Avg_Fifa_Score DESC;



