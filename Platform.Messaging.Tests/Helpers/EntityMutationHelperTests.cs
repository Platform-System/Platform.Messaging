using Microsoft.EntityFrameworkCore;
using Platform.Messaging.Helpers;
using Xunit;

namespace Platform.Messaging.Tests.Helpers;

public sealed class EntityMutationHelperTests
{
    [Fact]
    public async Task UpdateAsync_WhenUsingNonRelationalProvider_AppliesFallbackAndPersistsChanges()
    {
        await using var dbContext = CreateDbContext();
        var entity = new TestEntity { Id = Guid.NewGuid(), Name = "before" };
        await dbContext.Entities.AddAsync(entity);
        await dbContext.SaveChangesAsync();

        await EntityMutationHelper.UpdateAsync(
            dbContext,
            dbContext.Entities.Where(x => x.Id == entity.Id),
            setters => setters.SetProperty(x => x.Name, "after"),
            token => dbContext.Entities.FirstOrDefaultAsync(x => x.Id == entity.Id, token),
            loadedEntity => loadedEntity.Name = "after",
            CancellationToken.None);

        var persisted = await dbContext.Entities.SingleAsync(x => x.Id == entity.Id);
        Assert.Equal("after", persisted.Name);
    }

    [Fact]
    public async Task DeleteAsync_WhenUsingNonRelationalProvider_RemovesEntity()
    {
        await using var dbContext = CreateDbContext();
        var entity = new TestEntity { Id = Guid.NewGuid(), Name = "delete-me" };
        await dbContext.Entities.AddAsync(entity);
        await dbContext.SaveChangesAsync();

        await EntityMutationHelper.DeleteAsync(
            dbContext,
            dbContext.Entities.Where(x => x.Id == entity.Id),
            token => dbContext.Entities.FirstOrDefaultAsync(x => x.Id == entity.Id, token),
            CancellationToken.None);

        Assert.False(await dbContext.Entities.AnyAsync(x => x.Id == entity.Id));
    }

    private static TestDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString("N"))
            .Options;

        return new TestDbContext(options);
    }

    private sealed class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions<TestDbContext> options)
            : base(options)
        {
        }

        public DbSet<TestEntity> Entities => Set<TestEntity>();
    }

    private sealed class TestEntity
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
