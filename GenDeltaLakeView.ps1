##############################################################################################################################################
##
##	Note: Delta-format is currently not supported in Synapse serverless SQL pool. This is a workaround that leverages the transaction log.
##	You can vote for this feature here: https://feedback.azure.com/forums/307516-azure-synapse-analytics/suggestions/41120794-delta-lake-support-to-sql-on-demand
##
##############################################################################################################################################

cls 

## Prerequisite: create credential on your Synapse serverless SQL pool with URL of your storage and valid SAS token, example:
## CREATE CREDENTIAL [https://sqlondemandstorage.blob.core.windows.net]
## WITH IDENTITY='SHARED ACCESS SIGNATURE',  
## SECRET = 'sv=2018-03-28&ss=bf&srt=sco&sp=rl&st=2019-10-14T12%3A10%3A25Z&se=2061-12-31T12%3A10%3A00Z&sig=KlSU2ullCscyTS0An0nozEpo4tO5JAgGBvw%2FJX2lguw%3D'


## Step 1: setup connection to your SQL on-demand:
$params = @{
  'ServerInstance' =  '*****-ondemand.sql.azuresynapse.net' ## Synapse serverless SQL endpoint
  'Database' = '****'                                       ## Don't use system database (don't use master)
  'Username' = '....'
  'Password' = '.....'
  'OutputSqlErrors' = $true
}

## Step 2: define view name, URL of delta lake and optional partitions:

## Example 1: Create view `dbo.standard` on top of non-partitioned delta-format collection:

#$viewName = "dbo.standard"
#$root = 'https://****.blob.core.windows.net/delta-format-container/folder/'
#$partitons = $null

## Example 2: Create view `dbo.green` on top of delta-format collection partitioned by "puYear" and "puMonth" columns:

$viewName = "dbo.green"
$root = 'https://****.blob.core.windows.net/delta-format-container/folder/'
$partitons = @("puYear","puMonth")
#### Note: Use this option if your folders are in format /puYear=<number>/puMonth=/number>/<delta parquet files>

$mode = 0
# 0 (default) - generate view on Synapse SQL
# 1 - show view definition
# 2 - show list of files

###################################################################################################################################################



if($partitons -eq $null) {
$partitionPattern = $null
$partitionColumns = ""
}
else {
$partitionPattern = $partitons | % {"$_=*"}
$partitionPattern = ($partitionPattern -join "/") + "/"
$partitionColumns = $partitons | % {$i=1} {"$_ = p.filepath($i)"; $i++}
$partitionColumns = $partitionColumns -join ","
}

$sqlQuery = "
declare @version varchar(50) = '0';
select @version = json_value(value, '$.version')
from openrowset(bulk '"+ $root + "_delta_log/*_last_checkpoint',
        FORMAT='CSV',
        FIELDTERMINATOR ='0x0b', 
        FIELDQUOTE = '0x0b', 
        ROWTERMINATOR = '0x0b'
) WITH (value varchar(max)) c

set @version = REPLICATE('0', 20-LEN(@version)) + @version;
;
with
checkpoint_logs (version, added_file, removed_file) as (
select version = a.filepath(1), added_file = json_value([add], '$.path'), removed_file = json_value([remove], '$.path')
from openrowset(bulk '" + $root + "_delta_log/*.checkpoint.parquet',
                format='parquet')
        with ( [add] varchar(4000), [remove] varchar(4000) ) as a
where a.filepath(1) = @version or @version is null
)
,version(value) as (
select value = iif( @version is not null, 
                    @version,
                    (select top(1) version from checkpoint_logs order by version desc)
                    )
)
,
-- Add remaining added/removed files from .json files after the last checkpoint
tail_logs (version, added_file, removed_file) as (
SELECT version = r.filepath(1), j.added_file, j.removed_file
FROM 
    OPENROWSET(
        BULK '" + $root + "_delta_log/*.json',
		FORMAT='CSV', 
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b', 
        ROWTERMINATOR = '0x0A'
    )
    WITH (jsonContent NVARCHAR(4000)) AS [r]
    CROSS APPLY OPENJSON(jsonContent)
        WITH (added_file nvarchar(1000) '$.add.path', removed_file nvarchar(1000) '$.remove.path') AS j
WHERE r.filepath(1) > @version OR @version = '00000000000000000000' --> Take the changes after checkpoint
),
log_files (version, added_file, removed_file) as (
    select * from checkpoint_logs
    union all
    select * from tail_logs
),
files (location) as (
select location = added_file from log_files a
    where added_file is not null
    and added_file not in (select removed_file from log_files where removed_file is not null)
)
SELECT * FROM files"

$params.Query = $sqlQuery

$results = Invoke-Sqlcmd  @params

if($mode -eq 2) {
    $results
}

$files = $results | %{ "'" +$root+$_.location + "'" }

$files = $files -join ","


$root = $root.TrimEnd("/")
$deltaFilesPattern = $root + "/" + $partitionPattern + "*.parquet"

if ($partitionColumns.Trim() -ne "") {
    $partitionColumns = ", " + $partitionColumns
}
    

$sqlQuery = "CREATE OR ALTER VIEW $viewName AS SELECT * $partitionColumns FROM OPENROWSET (BULK '$deltaFilesPattern', format='parquet') as p WHERE p.filepath() IN ($files)"

if($mode -eq 0) {
    $params.Query = $sqlQuery
    Invoke-Sqlcmd  @params
}

if($mode -eq 1) {
    $sqlQuery
}
