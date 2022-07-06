CREATE PROCEDURE delta_load @folder varchar(8000), @table_name varchar(128), @credential varchar(8000)
AS BEGIN
    --declare @folder varchar(8000)
    --declare @table_name varchar(128)
    --declare @credential varchar(8000)

    -- test parameters
    --set @folder = 'https://jovanpoptest.dfs.core.windows.net/delta-log/green/';
    --set @table_name  = 'hrkljush'
    --set @credential  = ''

    IF (@credential IS NULL)
        SET @credential = ''

    IF (@credential <> '')
        SET @credential = ' CREDENTIAL(' + @credential + '),'

    declare @tsql varchar(8000);

    IF OBJECT_ID(@table_name) IS NOT NULL
    BEGIN
        SET @tsql = 'DROP TABLE ' + @table_name
        EXEC(@tsql)
    END

    IF OBJECT_ID('tempdb..#delta_json') IS NOT NULL
        DROP TABLE #delta_json

    create table #delta_json
    (
        jsoninput varchar(max)
    ) with (distribution=round_robin, heap)

    -- Read all the delta transaction logs
    set @tsql = '
    COPY INTO  #delta_json (jsoninput)
    FROM ''' + @folder + '_delta_log/*.json''
    WITH ( ' + @credential + '
            FILE_TYPE = ''CSV''
            ,fieldterminator =''0x0b''
            ,fieldquote = ''0x0b''
            ,rowterminator = ''0x0a'' ) '

    print(@tsql);
    exec(@tsql);

    with files(location) as (
    select location = JSON_VALUE(jsoninput, '$.add.path') from #delta_json a
            where JSON_VALUE(jsoninput, '$.add.path') is not null
            and JSON_VALUE(jsoninput, '$.add.path') 
                not in (select JSON_VALUE(jsoninput, '$.remove.path') from #delta_json where JSON_VALUE(jsoninput, '$.remove.path') is not null)
    )
    select @tsql = 'COPY INTO ' + @table_name + ' FROM ' + STRING_AGG(CAST(''''+@folder+location+'''' AS VARCHAR(MAX)), ',')
    + ' WITH ( ' + @credential + ' FILE_TYPE = ''PARQUET'', AUTO_CREATE_TABLE = ''ON'' )' 
    from files;

    print(@tsql);
    EXEC(@tsql)

END
