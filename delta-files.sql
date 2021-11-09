SET QUOTED_IDENTIFIER OFF
GO
BEGIN TRY
EXEC('CREATE SCHEMA delta');
END TRY
BEGIN CATCH END CATCH
GO
CREATE OR ALTER PROCEDURE delta.get_files @table sysname AS
BEGIN
    DECLARE @root VARCHAR(1000);

    select @root = TRIM('/' FROM eds.location) + '/' + TRIM('/' FROM et.location) + '/' from sys.external_tables et 
    join sys.external_data_sources eds on et.data_source_id = eds.data_source_id
    join sys.external_file_formats ff on et.file_format_id = ff.file_format_id
    where et.name = @table

    SET QUOTED_IDENTIFIER OFF

    DECLARE @sql NVARCHAR(max) = "
    declare @version varchar(50) = '0';
    select @version = json_value(value, '$.version')
    from openrowset(bulk '"+ @root + "_delta_log/*_last_checkpoint',
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
    from openrowset(bulk '" + @root + "_delta_log/*.checkpoint.parquet',
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
            BULK '" + @root + "_delta_log/*.json',
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
    SELECT DISTINCT '" + @root + "' + location AS path FROM files"

    EXEC(@sql)
END
