namespace ServiceBrokerListener.Domain;

#region Namespace imports

using System.Data;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

#endregion

public static class SqlConnectionExtensions
{
    #region Constants

    const string TriggersCountQuery = "SELECT COUNT(*) FROM sys.objects WHERE [type] = 'TR'";

    const string ProceduresCountQuery = "SELECT COUNT(*) FROM sys.objects WHERE [type] = 'P'";

    const string UnclosedConversationEndpointsCountQuery =
        "SELECT COUNT(*) FROM sys.conversation_endpoints WHERE [state] != 'CD' OR [lifetime] > GETDATE() + 1";

    const string ServiceQueuesCountQuery = "SELECT COUNT(*) FROM sys.service_queues";

    const string ConversationGroupsCountQuery = "SELECT COUNT(*) FROM sys.conversation_groups";

    const string ServicesCountQuery = "SELECT COUNT(*) FROM sys.services";

    #endregion

    #region Public Methods

    public static async Task<int> GetTriggersCountAsync(this SqlConnection connection)
        => await GetCountAsync(connection, TriggersCountQuery).ConfigureAwait(false);

    public static async Task<int> GetProceduresCountAsync(this SqlConnection connection)
        => await GetCountAsync(connection, ProceduresCountQuery).ConfigureAwait(false);

    public static async Task<int> GetUnclosedConversationEndpointsCountAsync(this SqlConnection connection)
        => await GetCountAsync(connection, UnclosedConversationEndpointsCountQuery).ConfigureAwait(false);

    public static async Task<int> GetServiceQueuesCountAsync(this SqlConnection connection)
        => await GetCountAsync(connection, ServiceQueuesCountQuery).ConfigureAwait(false);

    public static async Task<int> GetConversationGroupsCountAsync(this SqlConnection connection)
        => await GetCountAsync(connection, ConversationGroupsCountQuery).ConfigureAwait(false);

    public static async Task<int> GetServicesCountAsync(this SqlConnection connection)
        => await GetCountAsync(connection, ServicesCountQuery).ConfigureAwait(false);

    #endregion

    #region Private Methods

    private static async Task<int> GetCountAsync(
        SqlConnection connection,
        string commandText)
    {
        if (connection.State is not ConnectionState.Open)
        {
            return -1;
        }

        await using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = commandText;
            command.CommandType = CommandType.Text;

            object? o = await command.ExecuteScalarAsync().ConfigureAwait(false);

            return o is null ? -1 : (int)o;
        }
    }

    #endregion
}
