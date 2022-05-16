namespace Eventuous.Postgres.Test;

record AccountCreated(string AccountNumber);
record AccountCredited(decimal amount);
record AccountDebited(decimal amount);