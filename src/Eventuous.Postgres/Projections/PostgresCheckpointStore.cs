using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Eventuous.Subscriptions.Checkpoints;
using Dapper;
using System.Linq;

namespace Eventuous.Postgres.Projections;

public class PostgresCheckpointStore: ICheckpointStore {
    readonly IDbConnection _conn;
    readonly PostgresCheckpointStoreOptions _options;
    public PostgresCheckpointStore(
        IDbConnection conn,
        PostgresCheckpointStoreOptions? options = null
    ) {
        _conn = conn;
        _options = options ?? new PostgresCheckpointStoreOptions();
    }
    public async ValueTask<Checkpoint> GetLastCheckpoint(string checkpointId, CancellationToken cancellationToken)
    {
        var position = (await _conn.QueryAsync<ulong>("select position from checkpoints where id = @id", checkpointId)).First();
        return new Checkpoint(checkpointId, position);
    }

    public async ValueTask<Checkpoint> StoreCheckpoint(Checkpoint checkpoint, bool force, CancellationToken cancellationToken)
    {
        var sql = $@"
            INSERT INTO checkpoints (id, position)
            VALUES ('{checkpoint.Id}', {checkpoint.Position})
            ON CONFLICT (id) DO UPDATE
                SET position = {checkpoint.Position}
        ";
        await _conn.ExecuteAsync(sql);
        return checkpoint;
    }
}

public record PostgresCheckpointStoreOptions {
    public string SchemaName { get; init; } = "public";
};