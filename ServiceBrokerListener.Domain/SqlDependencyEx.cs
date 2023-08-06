namespace ServiceBrokerListener.Domain;

#region Namespace imports#

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

#endregion

public sealed class SqlDependencyEx : IAsyncDisposable
{

    #region Constants and Fields - SQL Scripts - Procedures

    private const string PermissionsInfoTemplate =
        """
        DECLARE @msg VARCHAR(MAX)
        DECLARE @crlf CHAR(1)

        SET @crlf = CHAR(10)
        SET @msg = 'Current user must have following permissions: '
        SET @msg = @msg + '[CREATE PROCEDURE, CREATE SERVICE, CREATE QUEUE, SUBSCRIBE QUERY NOTIFICATIONS, CONTROL, REFERENCES] '
        SET @msg = @msg + 'that are required to start query notifications. '
        SET @msg = @msg + 'Grant described permissions with following script: ' + @crlf
        SET @msg = @msg + 'GRANT CREATE PROCEDURE TO [<username>];' + @crlf
        SET @msg = @msg + 'GRANT CREATE SERVICE TO [<username>];' + @crlf
        SET @msg = @msg + 'GRANT CREATE QUEUE  TO [<username>];' + @crlf
        SET @msg = @msg + 'GRANT REFERENCES ON CONTRACT::[DEFAULT] TO [<username>];' + @crlf
        SET @msg = @msg + 'GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [<username>];' + @crlf
        SET @msg = @msg + 'GRANT CONTROL ON SCHEMA::[<schemaname>] TO [<username>];'

        PRINT @msg
        """;

    /// <summary>
    /// T-SQL script-template which creates notification setup procedure.
    /// {0} - database name.
    /// {1} - setup procedure name.
    /// {2} - service broker configuration statement.
    /// {3} - notification trigger configuration statement.
    /// {4} - notification trigger check statement.
    /// {5} - table name.
    /// {6} - schema name.
    /// </summary>
    private const string CreateInstallationProcedureTemplate =
        """
        USE [{0}]

        " + SQL_PERMISSIONS_INFO + @"

        IF OBJECT_ID ('{6}.{1}', 'P') IS NULL
        BEGIN
            EXEC ('
                CREATE PROCEDURE {6}.{1}
                AS
                BEGIN
                    -- Service Broker configuration statement.
                    {2}

                    -- Notification Trigger check statement.
                    {4}

                    -- Notification Trigger configuration statement.
                    DECLARE @triggerStatement NVARCHAR(MAX)
                    DECLARE @select NVARCHAR(MAX)
                    DECLARE @sqlInserted NVARCHAR(MAX)
                    DECLARE @sqlDeleted NVARCHAR(MAX)

                    SET @triggerStatement = N''{3}''

                    SET @select = STUFF((SELECT '','' + ''['' + COLUMN_NAME + '']''
                                         FROM INFORMATION_SCHEMA.COLUMNS
                                         WHERE DATA_TYPE NOT IN  (''text'',''ntext'',''image'',''geometry'',''geography'') AND TABLE_SCHEMA = ''{6}'' AND TABLE_NAME = ''{5}'' AND TABLE_CATALOG = ''{0}''
                                         FOR XML PATH ('''')
                                         ), 1, 1, '''')
                    SET @sqlInserted =
                        N''SET @retvalOUT = (SELECT '' + @select + N''
                                             FROM INSERTED
                                             FOR XML PATH(''''row''''), ROOT (''''inserted''''))''
                    SET @sqlDeleted =
                        N''SET @retvalOUT = (SELECT '' + @select + N''
                                             FROM DELETED
                                             FOR XML PATH(''''row''''), ROOT (''''deleted''''))''
                    SET @triggerStatement = REPLACE(@triggerStatement, ''%inserted_select_statement%'', @sqlInserted)
                    SET @triggerStatement = REPLACE(@triggerStatement, ''%deleted_select_statement%'', @sqlDeleted)

                    EXEC sp_executesql @triggerStatement
                END
                ')
        END
        """;

    /// <summary>
    /// T-SQL script-template which creates notification uninstall procedure.
    /// {0} - database name.
    /// {1} - uninstall procedure name.
    /// {2} - notification trigger drop statement.
    /// {3} - service broker uninstall statement.
    /// {4} - schema name.
    /// {5} - install procedure name.
    /// </summary>
    private const string CreateUninstallationProcedureTemplate =
        """
        USE [{0}]

        " + SQL_PERMISSIONS_INFO + @"

        IF OBJECT_ID ('{4}.{1}', 'P') IS NULL
        BEGIN
            EXEC ('
                CREATE PROCEDURE {4}.{1}
                AS
                BEGIN
                    -- Notification Trigger drop statement.
                    {3}

                    -- Service Broker uninstall statement.
                    {2}

                    IF OBJECT_ID (''{4}.{5}'', ''P'') IS NOT NULL
                        DROP PROCEDURE {4}.{5}

                    DROP PROCEDURE {4}.{1}
                END
                ')
        END
        """;

    #endregion

    #region Constants and Fields - SQL Scripts - ServiceBroker notification

    /// <summary>
    /// T-SQL script-template which prepares database for ServiceBroker notification.
    /// {0} - database name;
    /// {1} - conversation queue name.
    /// {2} - conversation service name.
    /// {3} - schema name.
    /// </summary>
    private const string InstallServiceBrokerNotificationTemplate =
        """
        -- Setup Service Broker
        IF EXISTS (SELECT * FROM sys.databases WHERE name = '{0}' AND is_broker_enabled = 0)
        BEGIN
            ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
            ALTER DATABASE [{0}] SET ENABLE_BROKER;
            ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE

            -- FOR SQL Express
            ALTER AUTHORIZATION ON DATABASE::[{0}] TO [sa]
        END

        -- Create a queue which will hold the tracked information
        IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = '{3}.{1}')
            CREATE QUEUE {3}.[{1}]

        -- Create a service on which tracked information will be sent
        IF NOT EXISTS(SELECT * FROM sys.services WHERE name = '{3}.{2}')
            CREATE SERVICE [{2}] ON QUEUE {3}.[{1}] ([DEFAULT])
        """;

    /// <summary>
    /// T-SQL script-template which removes database notification.
    /// {0} - conversation queue name.
    /// {1} - conversation service name.
    /// {2} - schema name.
    /// </summary>
    private const string UninstallServiceBrokerNotificationTemplate =
        """
        DECLARE @serviceId INT

        SELECT @serviceId = service_id
        FROM sys.services
        WHERE sys.services.name = '{1}'

        DECLARE @ConvHandle uniqueidentifier
        DECLARE Conv CURSOR FOR

        SELECT CEP.conversation_handle
        FROM sys.conversation_endpoints CEP
        WHERE CEP.service_id = @serviceId AND ([state] != 'CD' OR [lifetime] > GETDATE() + 1)

        OPEN Conv;
        FETCH NEXT FROM Conv INTO @ConvHandle;
        WHILE (@@FETCH_STATUS = 0)
        BEGIN
            END CONVERSATION @ConvHandle WITH CLEANUP;
            FETCH NEXT FROM Conv INTO @ConvHandle;
        END
        CLOSE Conv;
        DEALLOCATE Conv;

        -- Drop service
        IF (@serviceId IS NOT NULL)
            DROP SERVICE [{1}];

        -- Drop queue.
        IF OBJECT_ID ('{2}.{0}', 'SQ') IS NOT NULL
            DROP QUEUE {2}.[{0}];
        """;

    #endregion

    #region Constants and Fields - SQL Scripts - Notification Trigger

    /// <summary>
    /// T-SQL script-template which creates notification trigger.
    /// {0} - notification trigger name.
    /// {1} - schema name.
    /// </summary>
    private const string DeleteNotificationTriggerTemplate =
        """
        IF OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL
            DROP TRIGGER {1}.[{0}];
        """;

    private const string CheckNotificationTriggerTemplate =
        """
        IF OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL
            RETURN;
        """;

    /// <summary>
    /// T-SQL script-template which creates notification trigger.
    /// {0} - monitorable table name.
    /// {1} - notification trigger name.
    /// {2} - event data (INSERT, DELETE, UPDATE...).
    /// {3} - conversation service name.
    /// {4} - detailed changes tracking mode.
    /// {5} - schema name.
    /// %inserted_select_statement% - sql code which sets trigger "inserted" value to @retvalOUT variable.
    /// %deleted_select_statement% - sql code which sets trigger "deleted" value to @retvalOUT variable.
    /// </summary>
    private const string CreateNotificationTriggerTemplate =
        """
        CREATE TRIGGER [{1}]
        ON {5}.[{0}]
        AFTER {2}
        AS

        SET NOCOUNT ON;

        --Trigger {0} is rising...
        IF EXISTS (SELECT * FROM sys.services WHERE name = '{3}')
        BEGIN
            DECLARE @message NVARCHAR(MAX)
            SET @message = N'<root/>'

            IF ({4} EXISTS(SELECT 1))
            BEGIN
                DECLARE @retvalOUT NVARCHAR(MAX)

                %inserted_select_statement%

                IF (@retvalOUT IS NOT NULL)
                BEGIN SET @message = N'<root>' + @retvalOUT END

                %deleted_select_statement%

                IF (@retvalOUT IS NOT NULL)
                BEGIN
                    IF (@message = N'<root/>')
                    BEGIN
                        SET @message = N'<root>' + @retvalOUT
                    END
                    ELSE
                    BEGIN
                        SET @message = @message + @retvalOUT
                    END
                END

                IF (@message != N'<root/>')
                BEGIN
                    SET @message = @message + N'</root>'
                END
            END

            --Beginning of dialog...
            DECLARE @ConvHandle UNIQUEIDENTIFIER

            --Determine the Initiator Service, Target Service and the Contract
            BEGIN DIALOG @ConvHandle
                FROM SERVICE [{3}] TO SERVICE '{3}' ON CONTRACT [DEFAULT] WITH ENCRYPTION=OFF, LIFETIME = 60;

            --Send the Message
            SEND ON CONVERSATION @ConvHandle MESSAGE TYPE [DEFAULT] (@message);

            --End conversation
            END CONVERSATION @ConvHandle;
        END
        """;

    #endregion

    #region Constants and Fields - SQL Scripts - Miscellaneous

    /// <summary>
    /// T-SQL script-template which helps to receive changed data in monitorable table.
    /// {0} - database name.
    /// {1} - conversation queue name.
    /// {2} - timeout.
    /// {3} - schema name.
    /// </summary>
    private const string ReceiveEventTemplate =
        """
        USE [{0}]

        DECLARE @ConvHandle UNIQUEIDENTIFIER
        DECLARE @message VARBINARY(MAX)

        WAITFOR (RECEIVE TOP(1) @ConvHandle=Conversation_Handle, @message=message_body FROM {3}.[{1}]), TIMEOUT {2};

        BEGIN TRY
            END CONVERSATION @ConvHandle;
        END TRY
        BEGIN CATCH
        END CATCH

        SELECT CAST(@message AS NVARCHAR(MAX))
        """;

    /// <summary>
    /// T-SQL script-template which executes stored procedure.
    /// {0} - database name.
    /// {1} - procedure name.
    /// {2} - schema name.
    /// </summary>
    private const string ExecuteProcedureTemplate =
        """
        USE [{0}]

        IF OBJECT_ID ('{2}.{1}', 'P') IS NOT NULL
            EXEC {2}.{1}
        """;

    /// <summary>
    /// T-SQL script-template which returns all dependency identities in the database.
    /// {0} - database name.
    /// </summary>
    private const string GetDependencyIdentitiesTemplate =
        """
        USE [{0}]

        SELECT REPLACE(name, 'ListenerService_', '')
        FROM sys.services
        WHERE [name] like 'ListenerService_%';
        """;

    #endregion

    #region Constants and Fields - SQL Scripts - Forced cleaning of database

    /// <summary>
    /// T-SQL script-template which cleans database from notifications.
    /// {0} - database name.
    /// </summary>
    private const string ForceDatabaseCleaningTemplate =
        """
        USE [{0}]

        DECLARE @db_name VARCHAR(MAX)
        SET @db_name = '{0}' -- provide your own db name

        DECLARE @proc_name VARCHAR(MAX)
        DECLARE procedures CURSOR
        FOR
        SELECT sys.schemas.name + '.' + sys.objects.name
        FROM sys.objects
        INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
        WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_UninstallListenerNotification_%'

        OPEN procedures;
        FETCH NEXT FROM procedures INTO @proc_name

        WHILE (@@FETCH_STATUS = 0)
        BEGIN
            EXEC ('USE [' + @db_name + '] EXEC ' + @proc_name + ' IF (OBJECT_ID (''' + @proc_name + ''', ''P'') IS NOT NULL) DROP PROCEDURE ' + @proc_name)
            FETCH NEXT FROM procedures INTO @proc_name
        END

        CLOSE procedures;
        DEALLOCATE procedures;

        DECLARE procedures CURSOR
        FOR
        SELECT sys.schemas.name + '.' + sys.objects.name
        FROM sys.objects
        INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
        WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_InstallListenerNotification_%'

        OPEN procedures;
        FETCH NEXT FROM procedures INTO @proc_name

        WHILE (@@FETCH_STATUS = 0)
        BEGIN
            EXEC ('USE [' + @db_name + '] DROP PROCEDURE ' + @proc_name)
            FETCH NEXT FROM procedures INTO @proc_name
        END

        CLOSE procedures;
        DEALLOCATE procedures;
        """;

    #endregion

    #region Constants and Fields

    private const int CommandTimeoutInSeconds = 60_000;

    private static readonly List<int> ActiveEntities = new();

    private CancellationTokenSource cancellationTokenSource;

    #endregion

    #region Constructors and Destructor

    public SqlDependencyEx(
        string connectionString,
        string databaseName,
        string tableName,
        string schemaName = "dbo",
        NotificationTypes listenerType = NotificationTypes.Insert | NotificationTypes.Update | NotificationTypes.Delete,
        bool receiveDetails = true,
        int identity = 1)
    {
        this.ConnectionString = connectionString;
        this.DatabaseName = databaseName;
        this.TableName = tableName;
        this.SchemaName = schemaName;
        this.NotificationTypes = listenerType;
        this.DetailsIncluded = receiveDetails;
        this.Identity = identity;
        this.ConversationQueueName = $"ListenerQueue_{this.Identity}";
        this.ConversationServiceName = $"ListenerService_{this.Identity}";
        this.ConversationTriggerName = $"tr_Listener_{this.Identity}";
        this.InstallListenerProcedureName = $"sp_InstallListenerNotification_{this.Identity}";
        this.UninstallListenerProcedureName = $"sp_UninstallListenerNotification_{this.Identity}";
    }

    #endregion

    #region Properties

    public string ConversationQueueName { get; }

    public string ConversationServiceName { get; }

    public string ConversationTriggerName { get; }

    public string InstallListenerProcedureName { get; }

    public string UninstallListenerProcedureName { get; }

    public string ConnectionString { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string SchemaName { get; }

    public NotificationTypes NotificationTypes { get; }

    public bool DetailsIncluded { get; }

    public int Identity { get; }

    public bool Active { get; private set; }

    #endregion

    #region Events

    public event EventHandler<TableChangedEventArgs> TableChanged;

    public event EventHandler NotificationProcessStopped;

    #endregion

    #region Public Methods

    public static async Task<int[]> GetDependencyDbIdentitiesAsync(
        string connectionString,
        string database)
    {
        ArgumentNullException.ThrowIfNull(nameof(connectionString));
        ArgumentNullException.ThrowIfNull(nameof(database));

        Collection<int> result = new();

        await using (SqlConnection connection = new(connectionString))
        await using (SqlCommand command = connection.CreateCommand())
        {
            await connection.OpenAsync().ConfigureAwait(false);
            command.CommandText = string.Format(GetDependencyIdentitiesTemplate, database);
            command.CommandType = CommandType.Text;

            await using (SqlDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false)
                       && int.TryParse(reader.GetString(0), out int i))
                {
                    result.Add(i);
                }
            }
        }

        return result.Select(x => x).ToArray();
    }

    public static async Task CleanDatabaseAsync(
        string connectionString,
        string database)
    {
        await ExecuteNonQueryAsync(
            string.Format(ForceDatabaseCleaningTemplate, database),
            connectionString).ConfigureAwait(false);
    }

    public async Task StartAsync()
    {
        lock (ActiveEntities)
        {
            if (ActiveEntities.Contains(this.Identity))
            {
                throw new InvalidOperationException("An object with the same identity has already been started.");
            }

            ActiveEntities.Add(this.Identity);
        }

        // ASP.NET fix
        // IIS is not usually restarted when a new website version is deployed
        // This situation leads to notification absence in some cases
        await this.StopAsync().ConfigureAwait(false);

        await this.InstallNotificationAsync().ConfigureAwait(false);

        this.cancellationTokenSource = new CancellationTokenSource();

        // Pass the token to the cancelable operation.
        await Task.Run(this.NotificationLoopAsync, this.cancellationTokenSource.Token);
    }

    public async Task StopAsync()
    {
        await this.ReceiveEventAsync().ConfigureAwait(false);

        lock (ActiveEntities)
        {
            if (ActiveEntities.Contains(this.Identity))
            {
                ActiveEntities.Remove(this.Identity);
            }
        }

        if (this.cancellationTokenSource is null
            || this.cancellationTokenSource.Token.IsCancellationRequested)
        {
            return;
        }

        if (!this.cancellationTokenSource.Token.CanBeCanceled)
        {
            return;
        }

        this.cancellationTokenSource.Cancel();
        this.cancellationTokenSource.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await this.StopAsync().ConfigureAwait(false);
    }

    #endregion

    #region Private Methods

    private static async Task ExecuteNonQueryAsync(
        string commandText,
        string connectionString)
    {
        await using (SqlConnection connection = new(connectionString))
        await using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = commandText;
            command.CommandType = CommandType.Text;

            await connection.OpenAsync().ConfigureAwait(false);

            _ = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }

    private async Task NotificationLoopAsync()
    {
        try
        {
            while (true)
            {
                string message = await this.ReceiveEventAsync();
                this.Active = true;
                if (!string.IsNullOrWhiteSpace(message))
                {
                    this.OnTableChanged(message);
                }
            }
        }
        catch
        {
            // ignored
        }
        finally
        {
            this.Active = false;
            this.OnNotificationProcessStopped();
        }
    }

    private async Task<string> ReceiveEventAsync()
    {
        string commandText = string.Format(
            ReceiveEventTemplate,
            this.DatabaseName,
            this.ConversationQueueName,
            CommandTimeoutInSeconds / 2,
            this.SchemaName);

        string result;

        await using (SqlConnection connection = new(this.ConnectionString))
        await using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = commandText;
            command.CommandType = CommandType.Text;
            command.CommandTimeout = CommandTimeoutInSeconds;

            await connection.OpenAsync().ConfigureAwait(false);

            await using (SqlDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                result = await reader.ReadAsync().ConfigureAwait(false) && !reader.IsDBNull(0)
                    ? reader.GetString(0)
                    : string.Empty;
            }
        }

        return result;
    }

    private string GetUninstallNotificationProcedureScript()
    {
        string uninstallServiceBrokerNotificationScript = string.Format(
            UninstallServiceBrokerNotificationTemplate,
            this.ConversationQueueName,
            this.ConversationServiceName,
            this.SchemaName);

        string uninstallNotificationTriggerScript = string.Format(
            DeleteNotificationTriggerTemplate,
            this.ConversationTriggerName,
            this.SchemaName);

        string uninstallationProcedureScript = string.Format(
            CreateUninstallationProcedureTemplate,
            this.DatabaseName,
            this.UninstallListenerProcedureName,
            uninstallServiceBrokerNotificationScript.Replace("'", "''"),
            uninstallNotificationTriggerScript.Replace("'", "''"),
            this.SchemaName,
            this.InstallListenerProcedureName);

        return uninstallationProcedureScript;
    }

    private string GetInstallNotificationProcedureScript()
    {
        string installServiceBrokerNotificationScript = string.Format(
            InstallServiceBrokerNotificationTemplate,
            this.DatabaseName,
            this.ConversationQueueName,
            this.ConversationServiceName,
            this.SchemaName);

        string installNotificationTriggerScript = string.Format(
            CreateNotificationTriggerTemplate,
            this.TableName,
            this.ConversationTriggerName,
            this.GetTriggerTypeByListenerType(),
            this.ConversationServiceName,
            this.DetailsIncluded ? string.Empty : @"NOT",
            this.SchemaName);

        string uninstallNotificationTriggerScript = string.Format(
            CheckNotificationTriggerTemplate,
            this.ConversationTriggerName,
            this.SchemaName);

        string installationProcedureScript = string.Format(
            CreateInstallationProcedureTemplate,
            this.DatabaseName,
            this.InstallListenerProcedureName,
            installServiceBrokerNotificationScript.Replace("'", "''"),
            installNotificationTriggerScript.Replace("'", "''''"),
            uninstallNotificationTriggerScript.Replace("'", "''"),
            this.TableName,
            this.SchemaName);

        return installationProcedureScript;
    }

    private string GetTriggerTypeByListenerType()
    {
        StringBuilder result = new();

        if (this.NotificationTypes.HasFlag(NotificationTypes.Insert))
        {
            result.Append("INSERT");
        }

        if (this.NotificationTypes.HasFlag(NotificationTypes.Update))
        {
            result.Append(result.Length is 0 ? "UPDATE" : ", UPDATE");
        }

        if (this.NotificationTypes.HasFlag(NotificationTypes.Delete))
        {
            result.Append(result.Length is 0 ? "DELETE" : ", DELETE");
        }

        if (result.Length is 0)
        {
            result.Append("INSERT");
        }

        return result.ToString();
    }

    private async Task UninstallNotificationAsync()
    {
        string execUninstallationProcedureScript = string.Format(
            ExecuteProcedureTemplate,
            this.DatabaseName,
            this.UninstallListenerProcedureName,
            this.SchemaName);

        await ExecuteNonQueryAsync(execUninstallationProcedureScript, this.ConnectionString).ConfigureAwait(false);
    }

    private async Task InstallNotificationAsync()
    {
        string execInstallationProcedureScript = string.Format(
            ExecuteProcedureTemplate,
            this.DatabaseName,
            this.InstallListenerProcedureName,
            this.SchemaName);

        await ExecuteNonQueryAsync(this.GetInstallNotificationProcedureScript(), this.ConnectionString).ConfigureAwait(false);
        await ExecuteNonQueryAsync(this.GetUninstallNotificationProcedureScript(), this.ConnectionString).ConfigureAwait(false);
        await ExecuteNonQueryAsync(execInstallationProcedureScript, this.ConnectionString).ConfigureAwait(false);
    }

    private void OnTableChanged(string message) =>
        this.TableChanged?.Invoke(this, new TableChangedEventArgs(message));

    private void OnNotificationProcessStopped() =>
        _ = this.NotificationProcessStopped?.BeginInvoke(this, EventArgs.Empty, null, null);

    #endregion
}

