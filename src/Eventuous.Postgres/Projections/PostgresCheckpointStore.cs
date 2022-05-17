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
        var sql = $@"
            SELECT position 
            FROM {_options.SchemaName}.checkpoints 
            WHERE id = '{checkpointId}'
        ";

        var position = (await _conn.QueryAsync<ulong>(sql));

        return new Checkpoint(checkpointId, position.Any() ? position.First() : 0);
    }

    public async ValueTask<Checkpoint> StoreCheckpoint(Checkpoint checkpoint, bool force, CancellationToken cancellationToken)
    {
        var sql = $@"
            INSERT INTO @schema.checkpoints (id, position)
            VALUES ('@checkpointId', @position)
            ON CONFLICT (id) DO UPDATE
                SET position = @position
        ";
        await _conn.ExecuteAsync(sql, new { schema = _options.SchemaName, checkpointId = checkpoint.Id, position = checkpoint.Position});
        return checkpoint;
    }
}

public record PostgresCheckpointStoreOptions {
    public string SchemaName { get; init; } = "public";
};