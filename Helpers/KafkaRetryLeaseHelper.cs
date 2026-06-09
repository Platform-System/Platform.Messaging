using System.Data.Common;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Platform.Messaging.Models;

namespace Platform.Messaging.Helpers;

public static class KafkaRetryLeaseHelper
{
    public static async Task<ClaimedKafkaRetryMessage?> ClaimSingleAsync<TEntity>(
        DbContext dbContext,
        IQueryable<TEntity> eligibleQuery,
        Expression<Func<TEntity, DateTime>> _,
        Action<TEntity, DateTime> applyLease,
        TimeSpan leaseDuration,
        string tableName,
        string extraWhereSql,
        Action<DbCommand> configureExtraParameters,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        var now = DateTime.UtcNow;
        if (!dbContext.Database.IsRelational())
        {
            var fallbackRetryMessage = await eligibleQuery
                .FirstOrDefaultAsync(cancellationToken);

            if (fallbackRetryMessage is null)
                return null;

            applyLease(fallbackRetryMessage, now.Add(leaseDuration));
            await dbContext.SaveChangesAsync(cancellationToken);

            return new ClaimedKafkaRetryMessage(
                (Guid)dbContext.Entry(fallbackRetryMessage).Property("Id").CurrentValue!,
                (string)dbContext.Entry(fallbackRetryMessage).Property("Payload").CurrentValue!,
                (int)dbContext.Entry(fallbackRetryMessage).Property("RetryCount").CurrentValue!);
        }

        return await RelationalLeaseClaimHelper.ClaimSingleAsync(
            dbContext,
            $"""
            UPDATE "{tableName}" AS retry
            SET "NextAttemptAt" = @leaseUntil,
                "UpdatedAt" = @now,
                "UpdatedBy" = @updatedBy
            WHERE retry."Id" = (
                SELECT candidate."Id"
                FROM "{tableName}" AS candidate
                WHERE NOT candidate."IsSoftDeleted"
                  AND candidate."NextAttemptAt" <= @now
                  {extraWhereSql}
                ORDER BY candidate."NextAttemptAt", candidate."CreatedAt"
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING retry."Id", retry."Payload", retry."RetryCount";
            """,
            command =>
            {
                DbCommandParameterHelper.AddParameter(command, "@leaseUntil", now.Add(leaseDuration));
                DbCommandParameterHelper.AddParameter(command, "@now", now);
                DbCommandParameterHelper.AddParameter(command, "@updatedBy", "system");
                configureExtraParameters(command);
            },
            reader => new ClaimedKafkaRetryMessage(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetInt32(2)),
            cancellationToken);
    }
}
