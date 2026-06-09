using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace Platform.Messaging.Helpers;

public static class RelationalLeaseClaimHelper
{
    public static async Task<List<TClaimed>> ClaimBatchAsync<TClaimed>(
        DbContext dbContext,
        string commandText,
        Action<DbCommand> configureCommand,
        Func<DbDataReader, TClaimed> map,
        CancellationToken cancellationToken)
    {
        var connection = dbContext.Database.GetDbConnection();
        var shouldCloseConnection = connection.State != System.Data.ConnectionState.Open;
        if (shouldCloseConnection)
            await connection.OpenAsync(cancellationToken);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            configureCommand(command);

            var claimedItems = new List<TClaimed>();
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                claimedItems.Add(map(reader));

            await transaction.CommitAsync(cancellationToken);
            return claimedItems;
        }
        finally
        {
            if (shouldCloseConnection)
                await connection.CloseAsync();
        }
    }

    public static async Task<TClaimed?> ClaimSingleAsync<TClaimed>(
        DbContext dbContext,
        string commandText,
        Action<DbCommand> configureCommand,
        Func<DbDataReader, TClaimed> map,
        CancellationToken cancellationToken)
    {
        var claimedItems = await ClaimBatchAsync(
            dbContext,
            commandText,
            configureCommand,
            map,
            cancellationToken);

        return claimedItems.Count == 0 ? default : claimedItems[0];
    }
}
