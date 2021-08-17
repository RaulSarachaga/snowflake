#Con este Script podemos extrar información desde un data lake hacia una tabla de Snowflake


CREATE STAGE "DB_MINE"."PUBLIC".AzureDataLake URL = 'azure://bi64pro012349.blob.core.windows.net/landing' CREDENTIALS = (AZURE_SAS_TOKEN = '?sv=2020-08-04&ss=b&srt=co&srwdlacx&se=2021-08-14T03:40:54Z&st=2021-08-13T19:40:54Z&spr=https&sig=T8MUodVzOFeUXtvJN%2BnzKopA%2BmTsFJFDns850HWNgD4%3D');


show stages;

list @azuredatalake;

create or replace file format CSVFORMAT_QUOTES TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY='"';

select file.$1, file.$2, file.$3, file.$4, file.$5, file.$6
from @azuredatalake/Mine/Mina.csv (file_format => CSVFORMAT_QUOTES) file
LIMIT 1;


DELETE FROM Mine;

select count(*) from Mine;


create or replace pipe  mine_pipe as
copy into Mine from @azuredatalake/Mine/
file_format = CSVFORMAT_QUOTES;


select count(*) from Mine;

select system$pipe_status('mine_pipe');


alter pipe mine_pipe refresh;
    
select *
from table(information_schema.copy_history(table_name=>'Mine', start_time=> dateadd(hours, -1, current_timestamp())));
