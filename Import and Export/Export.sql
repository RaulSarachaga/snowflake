#Con este ejemplo se muestra como exportar código desde Snowflake hacia Azure Data Lake


COPY INTO @azuredatalake/export/mine.parquet
FROM (SELECT * FROM Mine)
FILE_FORMAT=(type=parquet) SINGLE=TRUE;


COPY INTO @azuredatalake/export/mine.csv
FROM (SELECT * FROM Mine)
FILE_FORMAT=(TYPE=csv COMPRESSION = NONE) SINGLE=TRUE HEADER=TRUE;
                    
                    
list @azuredatalake/export/;
