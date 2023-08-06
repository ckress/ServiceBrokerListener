namespace ServiceBrokerListener.UnitTests.Core;

#region Namespace imports

using System;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

using Microsoft.Data.SqlClient;

using ServiceBrokerListener.Domain;

using Xunit;

#endregion

/// <summary>
/// TODO:
/// 1. Performance test.
/// 2. Check strange behavior.
/// </summary>
public class SqlDependencyExTest : IDisposable
{
    #region Constants and Fields

    private const string MASTER_CONNECTION_STRING =
        "Data Source=(local);Initial Catalog=master;Integrated Security=True";

    private const string TEST_CONNECTION_STRING =
        "Data Source=(local);Initial Catalog=TestDatabase;User Id=TempLogin;Password=8fdKJl3$nlNv3049jsKK;";

    private const string ADMIN_TEST_CONNECTION_STRING =
        "Data Source=(local);Initial Catalog=TestDatabase;Integrated Security=True";

    private const string INSERT_FORMAT =
        "USE [TestDatabase] INSERT INTO temp.[TestTable] (TestField) VALUES({0})";

    private const string REMOVE_FORMAT =
        "USE [TestDatabase] DELETE FROM temp.[TestTable] WHERE TestField = {0}";

    private const string TEST_DATABASE_NAME = "TestDatabase";

    private const string TEST_TABLE_NAME = "TestTable";

    private const string TEST_TABLE_1_FULL_NAME = "temp.TestTable";

    private const string TEST_TABLE_2_FULL_NAME = "temp2.TestTable";

    private const string TEST_TABLE_3_FULL_NAME = "temp.Order";

    private bool isDisposed;

    #endregion

    #region Constructors and Destructor

    public SqlDependencyExTest()
    {
        const string CreateDatabaseScript =
            """
            CREATE DATABASE TestDatabase;

            ALTER DATABASE [TestDatabase] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
            ALTER DATABASE [TestDatabase] SET ENABLE_BROKER;
            ALTER DATABASE [TestDatabase] SET MULTI_USER WITH ROLLBACK IMMEDIATE

            -- FOR SQL Express
            ALTER AUTHORIZATION ON DATABASE::[TestDatabase] TO [sa]
            ALTER DATABASE [TestDatabase] SET TRUSTWORTHY ON;
            """;

        const string CreateUserScript =
            """
            CREATE LOGIN TempLogin
            WITH PASSWORD = '8fdKJl3$nlNv3049jsKK', DEFAULT_DATABASE=TestDatabase;

            USE [TestDatabase];
            CREATE USER TempUser FOR LOGIN TempLogin;

            GRANT CREATE PROCEDURE TO [TempUser];
            GRANT CREATE SERVICE TO [TempUser];
            GRANT CREATE QUEUE  TO [TempUser];
            GRANT REFERENCES ON CONTRACT::[DEFAULT] TO [TempUser]
            GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [TempUser];
            GRANT CONTROL ON SCHEMA::[temp] TO [TempUser]
            GRANT CONTROL ON SCHEMA::[temp2] TO [TempUser];
            """;

        const string CreateTable1Script =
            """
            CREATE SCHEMA Temp
            CREATE TABLE TestTable (TestField int, StrField NVARCHAR(MAX));
            """;

        const string CreateTable2Script =
            """
            CREATE SCHEMA Temp2
            CREATE TABLE TestTable (TestField int, StrField NVARCHAR(MAX));
            """;

        const string CreateTable3Script = "CREATE TABLE [temp].[Order] (TestField int, StrField NVARCHAR(MAX));";

        const string CreateTable4Script = "CREATE TABLE [temp].[Order2] ([Order] int, StrField NVARCHAR(MAX));";

        const string CreateTable5Script = "CREATE TABLE [temp].[Order3] (TestField int, StrField text);";

        CleanUp();

        ExecuteNonQuery(CreateDatabaseScript, MASTER_CONNECTION_STRING);
        ExecuteNonQuery(CreateTable1Script, ADMIN_TEST_CONNECTION_STRING);
        ExecuteNonQuery(CreateTable2Script, ADMIN_TEST_CONNECTION_STRING);
        ExecuteNonQuery(CreateTable3Script, ADMIN_TEST_CONNECTION_STRING);
        ExecuteNonQuery(CreateTable4Script, ADMIN_TEST_CONNECTION_STRING);
        ExecuteNonQuery(CreateTable5Script, ADMIN_TEST_CONNECTION_STRING);
        ExecuteNonQuery(CreateUserScript, MASTER_CONNECTION_STRING);
    }

    /*
    // TODO: override finalizer only if 'Dispose(bool isDisposing)' has code to free unmanaged resources
    ~SqlDependencyExTest()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool isDisposing)' method
        this.Dispose(false);
    }
    */

    #endregion

    #region Public Methods - Tests

    [Theory]
    [InlineData(10, 10)]
    [InlineData(10, 60)]
    [InlineData(10, 0)]
    [InlineData(100, 0)]
    [InlineData(1000, 0)]
    public async Task NotificationTestAsync(
        int changesCount,
        int delayInSeconds)
    {
        int changesReceived = 0;

        await using (SqlDependencyEx sd = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"))
        {
            sd.TableChanged += (o, e) => changesReceived++;

            await sd.StartAsync().ConfigureAwait(false);

            if (delayInSeconds > 0)
            {
                await Task.Delay(delayInSeconds * 1000).ConfigureAwait(false);
            }

            await MakeTableInsertDeleteChangesAsync(changesCount).ConfigureAwait(false);

            // Wait a little bit to receive all changes.
            await Task.Delay(1000).ConfigureAwait(false);
        }

        Assert.Equal(changesCount, changesReceived);
    }

    [Theory]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public async Task ResourcesReleasabilityTestAsync(int changesCount)
    {
        await using (SqlConnection connection = new(ADMIN_TEST_CONNECTION_STRING))
        {
            await connection.OpenAsync();

            int conversationEndpointsCount = await connection.GetUnclosedConversationEndpointsCountAsync().ConfigureAwait(false);
            int conversationGroupsCount = await connection.GetConversationGroupsCountAsync().ConfigureAwait(false);
            int serviceQueuesCount = await connection.GetServiceQueuesCountAsync().ConfigureAwait(false);
            int servicesCount = await connection.GetServicesCountAsync().ConfigureAwait(false);
            int triggersCount = await connection.GetTriggersCountAsync().ConfigureAwait(false);
            int proceduresCount = await connection.GetProceduresCountAsync().ConfigureAwait(false);

            await using (SqlDependencyEx sd = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"))
            {
                await sd.StartAsync().ConfigureAwait(false);

                // Make sure we've created one queue, sevice, trigger and two procedures.
                Assert.Equal(servicesCount + 1, await connection.GetServicesCountAsync().ConfigureAwait(false));
                Assert.Equal(serviceQueuesCount + 1, await connection.GetServiceQueuesCountAsync().ConfigureAwait(false));
                Assert.Equal(triggersCount + 1, await connection.GetTriggersCountAsync().ConfigureAwait(false));
                Assert.Equal(proceduresCount + 2, await connection.GetProceduresCountAsync().ConfigureAwait(false));

                await MakeTableInsertDeleteChangesAsync(changesCount).ConfigureAwait(false);

                // Wait a little bit to process all changes.
                await Task.Delay(1000).ConfigureAwait(false);
            }

            // Make sure we've released all resources.
            Assert.Equal(servicesCount, await connection.GetServicesCountAsync().ConfigureAwait(false));
            Assert.Equal(conversationGroupsCount, await connection.GetConversationGroupsCountAsync().ConfigureAwait(false));
            Assert.Equal(serviceQueuesCount, await connection.GetServiceQueuesCountAsync().ConfigureAwait(false));
            Assert.Equal(conversationEndpointsCount, await connection.GetUnclosedConversationEndpointsCountAsync().ConfigureAwait(false));
            Assert.Equal(triggersCount, await connection.GetTriggersCountAsync().ConfigureAwait(false));
            Assert.Equal(proceduresCount, await connection.GetProceduresCountAsync().ConfigureAwait(false));
        }
    }

    [Theory]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public async Task DetailsTestAsync(int chunkInsertCount)
    {
        int elementsInDetailsCount = 0;
        int changesReceived = 0;

        await using (SqlDependencyEx sd = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"))
        {
            sd.TableChanged += (o, e) =>
            {
                changesReceived++;

                if (e.Data is null)
                {
                    return;
                }

                XElement? inserted = e.Data.Element("inserted");
                XElement? deleted = e.Data.Element("deleted");

                elementsInDetailsCount += inserted is null ? 0 : inserted.Elements("row").Count();
                elementsInDetailsCount += deleted is null ? 0 : deleted.Elements("row").Count();
            };

            await sd.StartAsync().ConfigureAwait(false);

            await MakeChunkedInsertDeleteUpdateAsync(chunkInsertCount).ConfigureAwait(false);

            // Wait a little bit to receive all changes.
            await Task.Delay(1000).ConfigureAwait(false);
        }

        Assert.Equal(chunkInsertCount * 2, elementsInDetailsCount);
        Assert.Equal(3, changesReceived);
    }

    [Theory]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public async Task NotificationTypeTestAsync(int chunkInsertCount)
    {
        await TestNotificationTypeAsync(chunkInsertCount, NotificationTypes.Insert).ConfigureAwait(false);
        await TestNotificationTypeAsync(chunkInsertCount, NotificationTypes.Delete).ConfigureAwait(false);
        await TestNotificationTypeAsync(chunkInsertCount, NotificationTypes.Update).ConfigureAwait(false);
        await TestNotificationTypeAsync(chunkInsertCount, NotificationTypes.Insert | NotificationTypes.Delete).ConfigureAwait(false);
        await TestNotificationTypeAsync(chunkInsertCount, NotificationTypes.Insert | NotificationTypes.Update).ConfigureAwait(false);
        await TestNotificationTypeAsync(chunkInsertCount, NotificationTypes.Delete | NotificationTypes.Update).ConfigureAwait(false);
    }

    [Fact]
    public async Task MainPermissionExceptionCheckTestAsync()
    {
        bool errorReceived = false;

        try
        {
            string query = "USE [TestDatabase] DENY CREATE PROCEDURE TO [TempUser];";
            await ExecuteNonQueryAsync(query, MASTER_CONNECTION_STRING).ConfigureAwait(false);

            await using (SqlDependencyEx sd = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"))
            {
                await sd.StartAsync().ConfigureAwait(false);
            }
        }
        catch (SqlException)
        {
            errorReceived = true;
        }

        Assert.True(errorReceived);

        // It is impossible to start notification without CREATE PROCEDURE permission.
        try
        {
            string query = "USE [TestDatabase] GRANT CREATE PROCEDURE TO [TempUser];";
            await ExecuteNonQueryAsync(query, MASTER_CONNECTION_STRING).ConfigureAwait(false);
            errorReceived = false;

            await using (SqlDependencyEx sd = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"))
            {
                await sd.StartAsync().ConfigureAwait(false);
            }
        }
        catch (SqlException)
        {
            errorReceived = true;
        }

        Assert.False(errorReceived);

        // There is supposed to be no exceptions with admin rights.
        await using (SqlDependencyEx sd = new(MASTER_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"))
        {
            await sd.StartAsync().ConfigureAwait(false);
        }
    }

    [Fact]
    public async Task AdminPermissionExceptionCheckTestAsync()
    {
        const string ScriptDisableBroker =
            """
            ALTER DATABASE [TestDatabase] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
            ALTER DATABASE [TestDatabase] SET DISABLE_BROKER;
            ALTER DATABASE [TestDatabase] SET MULTI_USER WITH ROLLBACK IMMEDIATE
            """;

        // It is impossible to start notification without configured service broker.
        await ExecuteNonQueryAsync(ScriptDisableBroker, MASTER_CONNECTION_STRING).ConfigureAwait(false);

        bool errorReceived = false;

        try
        {
            await using (SqlDependencyEx sd = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"))
            {
                await sd.StartAsync().ConfigureAwait(false);
            }
        }
        catch (SqlException)
        {
            errorReceived = true;
        }

        Assert.True(errorReceived);

        // Service broker supposed to be configured automatically with MASTER connection string.
        int delayInSeconds = 0;
        int changesCount = 10;
        int changesReceived = 0;

        await using (SqlDependencyEx sd = new(MASTER_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp"))
        {
            sd.TableChanged += (o, e) => changesReceived++;

            await sd.StartAsync().ConfigureAwait(false);

            if (delayInSeconds > 0)
            {
                await Task.Delay(delayInSeconds * 1000).ConfigureAwait(false);
            }

            await MakeTableInsertDeleteChangesAsync(changesCount).ConfigureAwait(false);

            // Wait a little bit to receive all changes.
            await Task.Delay(1000).ConfigureAwait(false);
        }

        Assert.Equal(changesCount, changesReceived);
    }

    [Fact]
    public async Task GetActiveDbListenersTestAsync()
    {
        await using (SqlDependencyEx dep1 = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp", identity: 4))
        await using (SqlDependencyEx dep2 = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp", identity: 5))
        {
            await dep1.StartAsync().ConfigureAwait(false);

            // Make sure db has been got 1 dependency object.
            Assert.Equal(1, await GetDbDepCountAsync().ConfigureAwait(false));

            await dep2.StartAsync().ConfigureAwait(false);

            // Make sure db has been got 2 dependency object.
            Assert.Equal(2, await GetDbDepCountAsync().ConfigureAwait(false));
        }

        // Make sure db has no any dependency objects.
        Assert.Equal(0, await GetDbDepCountAsync().ConfigureAwait(false));

        static async Task<int> GetDbDepCountAsync()
        {
            int[] x = await SqlDependencyEx.GetDependencyDbIdentitiesAsync(TEST_CONNECTION_STRING, TEST_DATABASE_NAME).ConfigureAwait(false);
            return x.Length;
        }
    }

    [Fact]
    public async Task ClearDatabaseTest()
    {
        await using (SqlDependencyEx dep1 = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp", identity: 4))
        await using (SqlDependencyEx dep2 = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp", identity: 5))
        {
            await dep1.StartAsync().ConfigureAwait(false);

            // Make sure db has been got 1 dependency object.
            Assert.Equal(1, await GetDbDepCountAsync().ConfigureAwait(false));

            await dep2.StartAsync().ConfigureAwait(false);

            // Make sure db has been got 2 dependency object.
            Assert.Equal(2, await GetDbDepCountAsync().ConfigureAwait(false));

            // Forced db cleaning
            await SqlDependencyEx.CleanDatabaseAsync(TEST_CONNECTION_STRING, TEST_DATABASE_NAME).ConfigureAwait(false);

            // Make sure db has no any dependency objects.
            Assert.Equal(0, await GetDbDepCountAsync().ConfigureAwait(false));
        }

        static async Task<int> GetDbDepCountAsync()
        {
            int[] x = await SqlDependencyEx.GetDependencyDbIdentitiesAsync(TEST_CONNECTION_STRING, TEST_DATABASE_NAME).ConfigureAwait(false);
            return x.Length;
        }
    }

    [Fact]
    public async Task TwoTablesNotificationsTestAsync()
    {
        int table1InsertsReceived = 0;
        int table1DeletesReceived = 0;
        int table1TotalNotifications = 0;
        int table1TotalDeleted = 0;

        int table2InsertsReceived = 0;
        int table2DeletesReceived = 0;
        int table2TotalNotifications = 0;
        int table2TotalInserted = 0;

        await using (SqlDependencyEx sd1 = new(TEST_CONNECTION_STRING, "TestDatabase", "TestTable", "temp", NotificationTypes.Delete, true, 0))
        {
            sd1.TableChanged += (sender, args) =>
            {
                if (args.NotificationType is NotificationTypes.Delete)
                {
                    table1DeletesReceived++;
                    table1TotalDeleted += args.Data.Element("deleted").Elements("row").Count();
                }

                if (args.NotificationType is NotificationTypes.Insert)
                {
                    table1InsertsReceived++;
                }

                table1TotalNotifications++;
            };

            if (!sd1.Active)
            {
                await sd1.StartAsync().ConfigureAwait(false);
            }

            await using (SqlDependencyEx sd2 = new(TEST_CONNECTION_STRING, "TestDatabase", "TestTable", "temp2", NotificationTypes.Insert, true, 1))
            {
                sd2.TableChanged += (sender, args) =>
                {
                    if (args.NotificationType is NotificationTypes.Delete)
                    {
                        table2DeletesReceived++;
                    }

                    if (args.NotificationType is NotificationTypes.Insert)
                    {
                        table2InsertsReceived++;
                        table2TotalInserted += args.Data.Element("inserted").Elements("row").Count();
                    }

                    table2TotalNotifications++;
                };

                if (!sd2.Active)
                {
                    await sd2.StartAsync().ConfigureAwait(false);
                }

                await MakeChunkedInsertAsync(5, TEST_TABLE_1_FULL_NAME).ConfigureAwait(false);
                await MakeChunkedInsertAsync(3, TEST_TABLE_2_FULL_NAME).ConfigureAwait(false);
                await MakeChunkedInsertAsync(8, TEST_TABLE_2_FULL_NAME).ConfigureAwait(false);

                await DeleteFirstRowAsync(TEST_TABLE_1_FULL_NAME).ConfigureAwait(false);
                await DeleteFirstRowAsync(TEST_TABLE_1_FULL_NAME).ConfigureAwait(false);
                await DeleteFirstRowAsync(TEST_TABLE_1_FULL_NAME).ConfigureAwait(false);
                await DeleteFirstRowAsync(TEST_TABLE_1_FULL_NAME).ConfigureAwait(false);

                await DeleteFirstRowAsync(TEST_TABLE_2_FULL_NAME).ConfigureAwait(false);
                await DeleteFirstRowAsync(TEST_TABLE_2_FULL_NAME).ConfigureAwait(false);

                await MakeChunkedInsertAsync(1, TEST_TABLE_2_FULL_NAME).ConfigureAwait(false);
                await MakeChunkedInsertAsync(1, TEST_TABLE_1_FULL_NAME).ConfigureAwait(false);

                await DeleteFirstRowAsync(TEST_TABLE_1_FULL_NAME).ConfigureAwait(false);
                await DeleteFirstRowAsync(TEST_TABLE_2_FULL_NAME).ConfigureAwait(false);

                // Wait for notification to complete
                await Task.Delay(3000).ConfigureAwait(false);
            }
        }

        Assert.Equal(5, table1DeletesReceived);
        Assert.Equal(0, table1InsertsReceived);
        Assert.Equal(5, table1TotalNotifications);
        Assert.Equal(5, table1TotalDeleted);

        Assert.Equal(3, table2InsertsReceived);
        Assert.Equal(0, table2DeletesReceived);
        Assert.Equal(3, table2TotalNotifications);
        Assert.Equal(12, table2TotalInserted);
    }

    [Fact]
    public async Task NullCharacterInsertTestAsync()
    {
        int insertsReceived = 0;
        int deletesReceived = 0;
        int totalNotifications = 0;
        int totalDeleted = 0;

        SqlDependencyEx sd = new(TEST_CONNECTION_STRING, "TestDatabase", "TestTable", "temp", NotificationTypes.Insert, true, 0);
        await using (sd)
        {
            sd.TableChanged += (sender, args) =>
            {
                if (args.NotificationType == NotificationTypes.Delete)
                {
                    deletesReceived++;
                }

                if (args.NotificationType == NotificationTypes.Insert)
                {
                    insertsReceived++;
                }

                totalNotifications++;
            };

            if (!sd.Active)
            {
                await sd.StartAsync().ConfigureAwait(false);
            }

            await MakeNullCharacterInsertAsync().ConfigureAwait(false);
            await MakeNullCharacterInsertAsync().ConfigureAwait(false);
            await MakeNullCharacterInsertAsync().ConfigureAwait(false);

            // Wait for notification to complete
            await Task.Delay(3000).ConfigureAwait(false);
        }

        Assert.Equal(0, deletesReceived);
        Assert.Equal(3, insertsReceived);
        Assert.Equal(3, totalNotifications);
        Assert.Equal(0, totalDeleted);
    }

    [Fact]
    public async Task SpecialTableNameWithoutSquareBracketsTestAsync()
    {
        int insertsReceived = 0;
        int deletesReceived = 0;
        int totalNotifications = 0;
        int totalDeleted = 0;

        SqlDependencyEx sd = new(TEST_CONNECTION_STRING, "TestDatabase", "Order", "temp", NotificationTypes.Insert, true, 0);
        await using (sd)
        {
            sd.TableChanged += (sender, args) =>
            {
                if (args.NotificationType == NotificationTypes.Delete)
                {
                    deletesReceived++;
                }

                if (args.NotificationType == NotificationTypes.Insert)
                {
                    insertsReceived++;
                }

                totalNotifications++;
            };

            if (!sd.Active)
            {
                await sd.StartAsync().ConfigureAwait(false);
            }

            await MakeNullCharacterInsertAsync("[temp].[Order]").ConfigureAwait(false);
            await MakeNullCharacterInsertAsync("[temp].[Order]").ConfigureAwait(false);
            await MakeNullCharacterInsertAsync("[temp].[Order]").ConfigureAwait(false);

            // Wait for notification to complete
            await Task.Delay(3000).ConfigureAwait(false);
        }

        Assert.Equal(0, deletesReceived);
        Assert.Equal(3, insertsReceived);
        Assert.Equal(3, totalNotifications);
        Assert.Equal(0, totalDeleted);
    }

    [Fact]
    public async Task SpecialFieldNameWithoutSquareBracketsTestAsync()
    {
        int insertsReceived = 0;
        int deletesReceived = 0;
        int totalNotifications = 0;
        int totalDeleted = 0;

        SqlDependencyEx sd = new(TEST_CONNECTION_STRING, "TestDatabase", "Order2", "temp", NotificationTypes.Insert, true, 0);
        await using (sd)
        {
            sd.TableChanged += (sender, args) =>
            {
                if (args.NotificationType == NotificationTypes.Delete)
                {
                    deletesReceived++;
                }

                if (args.NotificationType == NotificationTypes.Insert)
                {
                    insertsReceived++;
                }

                totalNotifications++;
            };

            if (!sd.Active)
            {
                await sd.StartAsync().ConfigureAwait(false);
            }

            await MakeNullCharacterInsertAsync("[temp].[Order2]", "[Order]").ConfigureAwait(false);
            await MakeNullCharacterInsertAsync("[temp].[Order2]", "[Order]").ConfigureAwait(false);
            await MakeNullCharacterInsertAsync("[temp].[Order2]", "[Order]").ConfigureAwait(false);

            // Wait for notification to complete
            await Task.Delay(3000).ConfigureAwait(false);
        }

        Assert.Equal(0, deletesReceived);
        Assert.Equal(3, insertsReceived);
        Assert.Equal(3, totalNotifications);
        Assert.Equal(0, totalDeleted);
    }

    [Fact]
    public async Task UnsupportedFieldTypeTestAsync()
    {
        int insertsReceived = 0;
        int deletesReceived = 0;
        int totalNotifications = 0;
        int totalDeleted = 0;

        SqlDependencyEx sd = new(TEST_CONNECTION_STRING, "TestDatabase", "Order3", "temp", NotificationTypes.Insert, true, 0);
        await using (sd)
        {
            sd.TableChanged += (sender, args) =>
            {
                if (args.NotificationType is NotificationTypes.Delete)
                {
                    deletesReceived++;
                }

                if (args.NotificationType is NotificationTypes.Insert)
                {
                    insertsReceived++;
                }

                totalNotifications++;
            };

            if (!sd.Active)
            {
                await sd.StartAsync().ConfigureAwait(false);
            }

            await MakeNullCharacterInsertAsync("[temp].[Order3]").ConfigureAwait(false);
            await MakeNullCharacterInsertAsync("[temp].[Order3]").ConfigureAwait(false);
            await MakeNullCharacterInsertAsync("[temp].[Order3]").ConfigureAwait(false);

            // Wait for notification to complete
            await Task.Delay(3000).ConfigureAwait(false);
        }

        Assert.Equal(0, deletesReceived);
        Assert.Equal(3, insertsReceived);
        Assert.Equal(3, totalNotifications);
        Assert.Equal(0, totalDeleted);
    }

    [Theory]
    [InlineData(10, 0)]
    public async Task NotificationWithoutDetailsTest(
        int changesCount,
        int delayInSeconds)
    {
        int changesReceived = 0;

        await using (SqlDependencyEx sd = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp", receiveDetails: false))
        {
            sd.TableChanged += (s, args) =>
            {
                Assert.Equal(NotificationTypes.None, args.NotificationType);
                Assert.Empty(args.Data.Elements());

                changesReceived++;
            };

            await sd.StartAsync().ConfigureAwait(false);

            if (delayInSeconds > 0)
            {
                await Task.Delay(delayInSeconds * 1000).ConfigureAwait(false);
            }

            await MakeTableInsertDeleteChangesAsync(changesCount).ConfigureAwait(false);

            // Wait a little bit to receive all changes.
            await Task.Delay(1000).ConfigureAwait(false);
        }

        Assert.Equal(changesCount, changesReceived);
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

    private static async Task TestNotificationTypeAsync(
        int insertsCount,
        NotificationTypes testType)
    {
        int elementsInDetailsCount = 0;
        int changesReceived = 0;
        int expectedElementsInDetails = 0;

        NotificationTypes[] types = Enum.GetValues(typeof(NotificationTypes))
                                        .Cast<int>()
                                        .Where(enumValue => enumValue is not 0 && (enumValue & (int)testType) == enumValue)
                                        .Cast<NotificationTypes>()
                                        .ToArray();
        foreach (NotificationTypes temp in types)
        {
            switch (temp)
            {
                case NotificationTypes.Insert:
                case NotificationTypes.Delete:
                    expectedElementsInDetails += insertsCount / 2;
                    break;
                case NotificationTypes.Update:
                    expectedElementsInDetails += insertsCount;
                    break;
            }
        }

        await using (SqlDependencyEx sd = new(TEST_CONNECTION_STRING, TEST_DATABASE_NAME, TEST_TABLE_NAME, "temp", testType))
        {
            sd.TableChanged += (o, e) =>
            {
                changesReceived++;

                if (e.Data is null)
                {
                    return;
                }

                XElement? inserted = e.Data.Element("inserted");
                XElement? deleted = e.Data.Element("deleted");

                elementsInDetailsCount += inserted is null ? 0 : inserted.Elements("row").Count();
                elementsInDetailsCount += deleted is null ? 0 : deleted.Elements("row").Count();
            };

            await sd.StartAsync().ConfigureAwait(false);

            await MakeChunkedInsertDeleteUpdateAsync(insertsCount).ConfigureAwait(false);

            // Wait a little bit to receive all changes.
            await Task.Delay(1000).ConfigureAwait(false);
        }

        Assert.Equal(expectedElementsInDetails, elementsInDetailsCount);
        Assert.Equal(types.Length, changesReceived);
    }

    private static async Task MakeChunkedInsertDeleteUpdateAsync(int changesCount)
    {
        string query = CreateQuery();

        await ExecuteNonQueryAsync(query, TEST_CONNECTION_STRING).ConfigureAwait(false);
        await ExecuteNonQueryAsync("UPDATE temp.TestTable SET StrField = NULL", TEST_CONNECTION_STRING).ConfigureAwait(false);
        await ExecuteNonQueryAsync("DELETE FROM temp.TestTable", TEST_CONNECTION_STRING).ConfigureAwait(false);

        string CreateQuery()
        {
            const string InsertTemplate = "INSERT INTO #TmpTbl VALUES({0}, N'{1}')";

            // insert unicode statement
            StringBuilder sb = new();
            sb.AppendLine("SELECT 0 AS Number, N'юникод<>_1000001' AS Str INTO #TmpTbl");
            for (int i = 1; i < changesCount / 2; i++)
            {
                sb.AppendLine(string.Format(InsertTemplate, i, "юникод<>_" + i));
            }

            sb.AppendLine("INSERT INTO temp.TestTable (TestField, StrField) SELECT * FROM #TmpTbl");

            return sb.ToString();
        }
    }

    private static async Task MakeChunkedInsertAsync(
        int chunkSize,
        string tableName = "temp.TestTable")
    {
        string query = CreateQuery();

        await ExecuteNonQueryAsync(query, TEST_CONNECTION_STRING).ConfigureAwait(false);

        string CreateQuery()
        {
            const string InsertTemplate = "INSERT INTO #TmpTbl VALUES({0}, N'{1}')";

            // insert a unicode statement
            StringBuilder sb = new();
            sb.AppendLine("SELECT 0 AS Number, N'юникод<>_1000001' AS Str INTO #TmpTbl");
            for (int i = 1; i < chunkSize; i++)
            {
                sb.AppendLine(string.Format(InsertTemplate, i, "юникод<>_" + i));
            }

            sb.AppendLine($"INSERT INTO {tableName} (TestField, StrField) SELECT * FROM #TmpTbl");

            return sb.ToString();
        }
    }

    private static async Task MakeNullCharacterInsertAsync(
        string tableName = "temp.TestTable",
        string firstFieldName = "TestField",
        string secondFieldName = "StrField")
    {
        // insert a unicode statement
        string query =
            $"""
            SELECT 0 AS Number, CONVERT(VARCHAR(MAX), 0x00) AS Str INTO #TmpTbl")
            "INSERT INTO {tableName} ({firstFieldName}, {secondFieldName}) SELECT * FROM #TmpTbl"
            """;

        await ExecuteNonQueryAsync(query, TEST_CONNECTION_STRING).ConfigureAwait(false);
    }

    private static async Task DeleteFirstRowAsync(string tableName = "temp.TestTable")
    {
        string query = $"WITH q AS (SELECT TOP 1 * FROM {tableName}) DELETE FROM q";
        await ExecuteNonQueryAsync(query, TEST_CONNECTION_STRING).ConfigureAwait(false);
    }

    private static async Task MakeTableInsertDeleteChangesAsync(int changesCount)
    {
        for (int i = 0; i < changesCount / 2; i++)
        {
            await ExecuteNonQueryAsync(string.Format(INSERT_FORMAT, i), MASTER_CONNECTION_STRING).ConfigureAwait(false);
            await ExecuteNonQueryAsync(string.Format(REMOVE_FORMAT, i), MASTER_CONNECTION_STRING).ConfigureAwait(false);
        }
    }

    private static async Task ExecuteNonQueryAsync(
        string commandText,
        string connectionString)
    {
        await using (SqlConnection connection = new SqlConnection(connectionString))
        await using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = commandText;
            command.CommandType = CommandType.Text;
            command.CommandTimeout = 60000;

            await connection.OpenAsync().ConfigureAwait(false);

            _ = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
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
            """
            IF (EXISTS(select * from sys.databases where name='TestDatabase'))
            BEGIN
                ALTER DATABASE [TestDatabase] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [TestDatabase]
            END

            IF (EXISTS(select * from master.dbo.syslogins where name = 'TempLogin'))
            BEGIN
                DECLARE @loginNameToDrop sysname
                SET @loginNameToDrop = 'TempLogin';

                DECLARE sessionsToKill CURSOR FAST_FORWARD FOR
                    SELECT session_id
                    FROM sys.dm_exec_sessions
                    WHERE login_name = @loginNameToDrop

                OPEN sessionsToKill

                DECLARE @sessionId INT
                DECLARE @statement NVARCHAR(200)

                FETCH NEXT FROM sessionsToKill INTO @sessionId

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    PRINT 'Killing session ' + CAST(@sessionId AS NVARCHAR(20)) + ' for login ' + @loginNameToDrop

                    SET @statement = 'KILL ' + CAST(@sessionId AS NVARCHAR(20))
                    EXEC sp_executesql @statement

                    FETCH NEXT FROM sessionsToKill INTO @sessionId
                END

                CLOSE sessionsToKill
                DEALLOCATE sessionsToKill

                PRINT 'Dropping login ' + @loginNameToDrop
                SET @statement = 'DROP LOGIN [' + @loginNameToDrop + ']'
                EXEC sp_executesql @statement
            END
            """;

        ExecuteNonQuery(DropTestDatabaseScript, MASTER_CONNECTION_STRING);
    }

    #endregion
}
