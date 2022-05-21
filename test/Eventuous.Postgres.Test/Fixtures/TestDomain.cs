using Eventuous;
namespace Eventuous.Postgres.Test;

record AccountCreated(string AccountNumber);
record AccountCredited(decimal Amount);
record AccountDebited(decimal Amount);

public record BankAccountId: AggregateId {
    public BankAccountId(string value) : base(value) { }
}

public record BankAccountState : AggregateState<BankAccountState, BankAccountId> {
    public BankAccountState() {
        On<AccountCreated>((state, account) => state with { Id = new BankAccountId(account.AccountNumber), Balance = 0});
        On<AccountCredited>((state, credited) => state with { Balance = state.Balance + credited.Amount});
        On<AccountDebited>((state, credited) => state with { Balance = state.Balance - credited.Amount});
    }

    public decimal Balance { get; init; }
}

public class BankAccount: Aggregate<BankAccountState, BankAccountId> {
    public void CreateAccount(BankAccountId id) {
        EnsureDoesntExist();
        Apply(new AccountCreated(id));
    }

    public void CreditAccount(decimal amount) {
        EnsureExists();
        Apply(new AccountCredited(amount));
    }

    public void DebitAccount(decimal amount) {
        EnsureExists();
        Apply(new AccountDebited(amount));
    }

}