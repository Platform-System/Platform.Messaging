using System.Text.Json;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Platform.Messaging.Models;

namespace Platform.Messaging.Helpers;

public static class KafkaRetryStoreHelper
{
    public static async Task UpsertAsync<TEntity>(
        DbContext dbContext,
        IQueryable<TEntity> existingQuery,
        Func<TEntity> createEntity,
        Action<TEntity> updateEntity,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        var existingEntity = await existingQuery.FirstOrDefaultAsync(cancellationToken);
        if (existingEntity is null)
        {
            await dbContext.Set<TEntity>().AddAsync(createEntity(), cancellationToken);
        }
        else
        {
            updateEntity(existingEntity);
            dbContext.Set<TEntity>().Update(existingEntity);
        }

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    public static Task DeleteAsync<TEntity>(
        DbContext dbContext,
        IQueryable<TEntity> query,
        CancellationToken cancellationToken)
        where TEntity : class
        => EntityMutationHelper.DeleteAsync(
            dbContext,
            query,
            token => query.FirstOrDefaultAsync(token),
            cancellationToken);

    public static Task DeleteByIdAsync<TEntity>(
        DbContext dbContext,
        IQueryable<TEntity> set,
        Guid id,
        CancellationToken cancellationToken)
        where TEntity : class
        => DeleteAsync(
            dbContext,
            set.Where(entity => EF.Property<Guid>(entity, "Id") == id),
            cancellationToken);

    public static Task UpdateAsync<TEntity>(
        DbContext dbContext,
        IQueryable<TEntity> query,
        Expression<Func<SetPropertyCalls<TEntity>, SetPropertyCalls<TEntity>>> setPropertyCalls,
        Action<TEntity> applyFallback,
        CancellationToken cancellationToken)
        where TEntity : class
        => EntityMutationHelper.UpdateAsync(
            dbContext,
            query,
            setPropertyCalls,
            token => query.FirstOrDefaultAsync(token),
            applyFallback,
            cancellationToken);

    public static Task UpdateEnvelopeByIdAsync<TEntity, TMessage>(
        DbContext dbContext,
        IQueryable<TEntity> set,
        Guid id,
        KafkaRetryEnvelope<TMessage> envelope,
        JsonSerializerOptions serializerOptions,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        var payload = JsonSerializer.Serialize(envelope, serializerOptions);
        var nextAttemptAt = envelope.NextAttemptAt ?? DateTime.UtcNow;

        return UpdateAsync(
            dbContext,
            set.Where(entity => EF.Property<Guid>(entity, "Id") == id),
            setters => setters
                .SetProperty(entity => EF.Property<string>(entity, "Payload"), payload)
                .SetProperty(entity => EF.Property<int>(entity, "RetryCount"), envelope.RetryCount)
                .SetProperty(entity => EF.Property<DateTime>(entity, "NextAttemptAt"), nextAttemptAt),
            entity =>
            {
                dbContext.Entry(entity).Property("Payload").CurrentValue = payload;
                dbContext.Entry(entity).Property("RetryCount").CurrentValue = envelope.RetryCount;
                dbContext.Entry(entity).Property("NextAttemptAt").CurrentValue = nextAttemptAt;
            },
            cancellationToken);
    }
}
