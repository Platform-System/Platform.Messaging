using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using System.Linq.Expressions;

namespace Platform.Messaging.Helpers;

public static class EntityMutationHelper
{
    public static Task UpdateAsync<TEntity>(
        DbContext dbContext,
        IQueryable<TEntity> query,
        Expression<Func<SetPropertyCalls<TEntity>, SetPropertyCalls<TEntity>>> setPropertyCalls,
        Func<CancellationToken, Task<TEntity?>> loadEntityAsync,
        Action<TEntity> applyFallback,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        if (dbContext.Database.IsRelational())
            return query.ExecuteUpdateAsync(setPropertyCalls, cancellationToken);

        return UpdateFallbackAsync(dbContext, loadEntityAsync, applyFallback, cancellationToken);
    }

    public static Task DeleteAsync<TEntity>(
        DbContext dbContext,
        IQueryable<TEntity> query,
        Func<CancellationToken, Task<TEntity?>> loadEntityAsync,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        if (dbContext.Database.IsRelational())
            return query.ExecuteDeleteAsync(cancellationToken);

        return DeleteFallbackAsync(dbContext, loadEntityAsync, cancellationToken);
    }

    private static async Task UpdateFallbackAsync<TEntity>(
        DbContext dbContext,
        Func<CancellationToken, Task<TEntity?>> loadEntityAsync,
        Action<TEntity> applyFallback,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        var entity = await loadEntityAsync(cancellationToken);
        if (entity is null)
            return;

        applyFallback(entity);
        await dbContext.SaveChangesAsync(cancellationToken);
    }

    private static async Task DeleteFallbackAsync<TEntity>(
        DbContext dbContext,
        Func<CancellationToken, Task<TEntity?>> loadEntityAsync,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        var entity = await loadEntityAsync(cancellationToken);
        if (entity is null)
            return;

        dbContext.Remove(entity);
        await dbContext.SaveChangesAsync(cancellationToken);
    }
}
