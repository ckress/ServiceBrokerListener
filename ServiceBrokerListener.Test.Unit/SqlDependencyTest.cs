namespace ServiceBrokerListener.UnitTests.Core;

#region Namespace imports

using System;
using System.Data;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using ServiceBrokerListener.Domain;

using Xunit;

#endregion

public class SqlDependencyTest : IDisposable
{
    #region Constants and Fields

    private const string TEST_DATABASE_NAME = "TestDatabase";

    private const string TEST_TABLE_NAME = "TestTable";

    private const string TEST_TABLE_NAME_2 = "TestTable2";

    private const string MASTER_CONNECTION_STRING =
        "Data Source=(local);Initial Catalog=master;Integrated Security=True";

    private const string TEST_CONNECTION_STRING =
        "Data Source=(local);Initial Catalog=TestDatabase;Integrated Security=True";

    private bool isDisposed;

    #endregion

    #region Constructors

    public SqlDependencyTest()
    {
        const string CreateDatabaseQuery = $"CREATE DATABASE [{TEST_DATABASE_NAME}]";

        const string CreateTableQuery =
            $"""
            USE [{TEST_DATABASE_NAME}]

            CREATE TABLE {TEST_TABLE_NAME} (TestField int)
            CREATE TABLE {TEST_TABLE_NAME_2} (TestField int)

            ALTER DATABASE [{TEST_DATABASE_NAME}]
            SET ENABLE_BROKER;

            ALTER AUTHORIZATION ON DATABASE::[{TEST_DATABASE_NAME}] TO [sa]

            ALTER DATABASE [{TEST_DATABASE_NAME}]
            SET TRUSTWORTHY ON;
            """;

        CleanUp();

        ExecuteNonQuery(CreateDatabaseQuery, MASTER_CONNECTION_STRING);
        ExecuteNonQuery(CreateTableQuery, MASTER_CONNECTION_STRING);
    }

    /*
    // TODO: override finalizer only if 'Dispose(bool isDisposing)' has code to free unmanaged resources
    ~SqlDependencyTest()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool isDisposing)' method
        this.Dispose(false);
    }
    */

    #endregion

    #region Public Methods - Tests

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task TestNotificationWithChangeAsync(int changesCount)
    {
        SqlDependency.Stop(TEST_CONNECTION_STRING);
        SqlDependency.Start(TEST_CONNECTION_STRING);

        int changesReceived = 0;

        await using (SqlConnection connection = new(TEST_CONNECTION_STRING))
        await using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = "SELECT TestField FROM dbo.TestTable";

            await connection.OpenAsync().ConfigureAwait(false);

            OnChangeEventHandler? onChange = null;
            onChange = async (s, e) =>
            {
                if (e.Info is SqlNotificationInfo.Insert)
                {
                    changesReceived++;
                }

                // SqlDependency magic to receive events consequentially.
                SqlDependency dep = (SqlDependency)s;
                dep.OnChange -= onChange;

                command.Notification = null;

                dep = new SqlDependency(command);
                dep.OnChange += onChange;

                await using (_ = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                }
            };

            // Create a dependency and associate it with the SqlCommand.
            SqlDependency dependency = new SqlDependency(command);

            // Subscribe to the SqlDependency event.
            dependency.OnChange += onChange;

            // Execute the command.
            await using (_ = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
            }

            await MakeTableInsertChangeAsync(changesCount).ConfigureAwait(false);
        }

        SqlDependency.Stop(TEST_CONNECTION_STRING);

        Assert.Equal(changesCount, changesReceived);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task TestResourcesReleasabilityWithChanges_PROOF_OF_FAIL(int changesCount)
    {
        await using (SqlConnection connection = new(TEST_CONNECTION_STRING))
        {
            await connection.OpenAsync().ConfigureAwait(false);

            int conversationEndpointsCount = await connection.GetUnclosedConversationEndpointsCountAsync().ConfigureAwait(false);
            int conversationGroupsCount = await connection.GetConversationGroupsCountAsync().ConfigureAwait(false);
            int serviceQueuesCount = await connection.GetServiceQueuesCountAsync().ConfigureAwait(false);
            int servicesCount = await connection.GetServicesCountAsync().ConfigureAwait(false);

            SqlDependency.Stop(TEST_CONNECTION_STRING);
            SqlDependency.Start(TEST_CONNECTION_STRING);

            await using (SqlCommand command = connection.CreateCommand())
            {
                command.CommandText = "SELECT TestField FROM dbo.TestTable";

                OnChangeEventHandler? onChange = null;
                onChange = async (sender, args) =>
                {
                    // SqlDependency magic to receive events consequentially.
                    SqlDependency dep = (SqlDependency)sender;
                    dep.OnChange -= onChange;

                    command.Notification = null;

                    dep = new SqlDependency(command);
                    dep.OnChange += onChange;

                    await using (_ = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                    }
                };

                // Create a dependency and associate it with the SqlCommand.
                SqlDependency dependency = new SqlDependency(command);

                // Subscribe to the SqlDependency event.
                dependency.OnChange += onChange;

                // Execute the command.
                await using (_ = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                }

                await MakeTableInsertChangeAsync(changesCount).ConfigureAwait(false);
            }

            SqlDependency.Stop(TEST_CONNECTION_STRING);

            // Microsoft SqlDependency REMOVES queue and service after use.
            Assert.Equal(
                servicesCount,
                await connection.GetServicesCountAsync().ConfigureAwait(false));
            Assert.Equal(
                serviceQueuesCount,
                await connection.GetServiceQueuesCountAsync().ConfigureAwait(false));

            // Microsoft SqlDependency KEEPS conversation group and endpoint in DB after use.
            // This behavior leads to GIANT memory leaks in SQL Server.
            Assert.NotEqual(
                conversationGroupsCount,
                await connection.GetConversationGroupsCountAsync().ConfigureAwait(false));
            Assert.NotEqual(
                conversationEndpointsCount,
                await connection.GetUnclosedConversationEndpointsCountAsync().ConfigureAwait(false));
        }
    }

    [Fact]
    public async Task TestTwoTablesNotificationAsync()
    {
        const int ChangesCountFirstTable = 5;
        const int ChangesCountSecondTable = 7;

        int changesReceived1 = 0;
        int changesReceived2 = 0;

        OnChangeEventHandler? onChange1 = null;
        onChange1 = (s, e) =>
            {
                if (e is not null && e.Info is SqlNotificationInfo.Insert)
                {
                    changesReceived1++;
                }

                using (SqlConnection connection = new SqlConnection(TEST_CONNECTION_STRING))
                using (SqlCommand command1 = connection.CreateCommand())
                {
                    command1.CommandText = "SELECT TestField FROM dbo.TestTable";

                    connection.Open();

                    SqlDependency dep = (SqlDependency)s;
                    if (dep is not null)
                    {
                        dep.OnChange -= onChange1;
                    }

                    command1.Notification = null;

                    dep = new SqlDependency(command1);
                    dep.OnChange += onChange1;

                    command1.ExecuteReader().Close();
                }
            };

        OnChangeEventHandler? onChange2 = null;
        onChange2 = (s, e) =>
        {
            if (e is not null && e.Info is SqlNotificationInfo.Insert)
            {
                changesReceived2++;
            }

            using (SqlConnection connection = new SqlConnection(TEST_CONNECTION_STRING))
            using (SqlCommand command2 = connection.CreateCommand())
            {
                command2.CommandText = "SELECT TestField FROM dbo.TestTable2";

                connection.Open();

                SqlDependency dep = (SqlDependency)s;
                if (dep is not null)
                {
                    dep.OnChange -= onChange2;
                }

                command2.Notification = null;

                dep = new SqlDependency(command2);
                dep.OnChange += onChange2;

                command2.ExecuteReader().Close();
            }
        };

        SqlDependency.Start(TEST_CONNECTION_STRING);

        onChange1(null, null);

        SqlDependency.Start(TEST_CONNECTION_STRING);

        onChange2(null, null);

        await MakeTableInsertChangeAsync(ChangesCountFirstTable).ConfigureAwait(false);
        await MakeTableInsertChangeAsync(ChangesCountSecondTable, TEST_TABLE_NAME_2).ConfigureAwait(false);

        Assert.Equal(ChangesCountFirstTable, changesReceived1);
        Assert.Equal(ChangesCountSecondTable, changesReceived2);

        SqlDependency.Stop(TEST_CONNECTION_STRING);
    }

    #endregion

    #region Public Methods

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    #endregion

    #region Protected Methods

    protected virtual void Dispose(bool isDisposing)
    {
        if (!isDisposed)
        {
            if (isDisposing)
            {
                // Dispose managed state (managed objects)
                CleanUp();
            }

            // Free unmanaged resources (unmanaged objects) and override finalizer
            // Set large fields to null
            isDisposed = true;
        }
    }

    #endregion

    #region Private Methods

    private static async Task MakeTableInsertChangeAsync(
        int changesCount,
        string databaseName = TEST_DATABASE_NAME,
        string tableName = TEST_TABLE_NAME)
    {
        const string InsertQueryTemplate = "USE [{0}] INSERT INTO [{1}] VALUES({2})";

        for (int i = 0; i < changesCount; i++)
        {
            string commandText = string.Format(InsertQueryTemplate, databaseName, tableName, i);
            await ExecuteNonQueryAsync(commandText, TEST_CONNECTION_STRING).ConfigureAwait(false);

            // It is one of weaknesses of Microsoft SqlDependency:
            // you must subscribe on OnChange again after every event firing.
            // Thus you may loose many table changes.
            // In this case we should wait a little bit to give enough time for resubscription.
            await Task.Delay(500).ConfigureAwait(false);
        }
    }

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

            _ = command.ExecuteNonQuery();
        }
    }

    private static void ExecuteNonQuery(
        string commandText,
        string connectionString)
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = commandText;
            command.CommandType = CommandType.Text;
            command.CommandTimeout = 60000;

            connection.Open();

            _ = command.ExecuteNonQuery();
        }
    }

    private static void CleanUp()
    {
        const string DropTestDatabaseScript =
            $"""
            IF EXISTS(SELECT * FROM sys.databases WHERE name='{TEST_DATABASE_NAME}')
            BEGIN
                ALTER DATABASE [{TEST_DATABASE_NAME}]
                SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

                DROP DATABASE [{TEST_DATABASE_NAME}]
            END
            """;

        ExecuteNonQuery(DropTestDatabaseScript, MASTER_CONNECTION_STRING);
    }

    #endregion
}
