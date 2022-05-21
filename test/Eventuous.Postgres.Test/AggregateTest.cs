using System;
using System.Threading.Tasks;
using Eventuous.Postgres.Store;
using Npgsql;
using System.Linq;

namespace Eventuous.Postgres.Test;

public class AggregateTest : IDisposable
{
    TestFixture fixture;
    public AggregateTest() {
        fixture = new TestFixture();
    }

    public void Dispose()
    {
        fixture.Dispose();
    } 

    [Fact]
    public async Task AggregateTesting()
    {
        // arrange
        var account = new BankAccount();
        account.CreateAccount(new BankAccountId(Guid.NewGuid().ToString()));
        account.CreditAccount(100);
        account.DebitAccount(50);
        var streamName = new StreamName($"Account-{Guid.NewGuid().ToString()}");

        await fixture.AggregateStore.Store(streamName, account, CancellationToken.None);
        var storedAccount = await fixture.AggregateStore.Load<BankAccount>(streamName, CancellationToken.None);

        // assert
        Assert.Equal(50, storedAccount.State.Balance);
    }

    [Fact]
    public async Task AggregateTestingWithWrongVersion()
    {
        // arrange
        var account = new BankAccount();
        account.CreateAccount(new BankAccountId(Guid.NewGuid().ToString()));
        account.CreditAccount(100);
        account.DebitAccount(50);
        var streamName = new StreamName($"Account-{Guid.NewGuid().ToString()}");

        await fixture.AggregateStore.Store(streamName, account, CancellationToken.None);

        // assert
        await Assert.ThrowsAsync<WrongExpectedVersionException>(async () => await fixture.AggregateStore.Store(streamName, account, CancellationToken.None));
    }

}