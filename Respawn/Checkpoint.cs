
namespace Respawn
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading.Tasks;

    public class Checkpoint
    {
        private DatabaseTables _tables;
        private string _deleteSql;
        private string _disableFKsSql;

        public string[] TablesToIgnore { get; set; } = new string[0];
        public string[] SchemasToInclude { get; set; } = new string[0];
        public string[] SchemasToExclude { get; set; } = new string[0];
        public IDbAdapter DbAdapter { get; set; } = Respawn.DbAdapter.SqlServer;

        public int? CommandTimeout { get; set; }

        public class Relationship
        {
            public string Name { get; set; }
            public string PrimaryKeyTable { get; set; }
            public string ForeignKeyTable { get; set; }

            public bool IsSelfReferencing => PrimaryKeyTable == ForeignKeyTable;

        }

        public virtual async Task Reset(string nameOrConnectionString)
        {
            using (var connection = new SqlConnection(nameOrConnectionString))
            {
                await connection.OpenAsync();

                await Reset(connection);
            }
        }

        public virtual async Task Reset(DbConnection connection)
        {
            if (string.IsNullOrWhiteSpace(_deleteSql))
            {
                await BuildDeleteTables(connection);
            }

            await ExecuteDeleteSqlAsync(connection);
        }

        private async Task ExecuteDeleteSqlAsync(DbConnection connection)
        {
            using (var tx = connection.BeginTransaction())
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;
                cmd.CommandText = _deleteSql + _disableFKsSql;
                cmd.Transaction = tx;

                await cmd.ExecuteNonQueryAsync();

                tx.Commit();
            }
        }

        private async Task BuildDeleteTables(DbConnection connection)
        {
            var allTables = await GetAllTables(connection);

            var allRelationships = await GetRelationships(connection);

            _tables = BuildTableList(allTables, allRelationships);

            _disableFKsSql = DbAdapter.BuildDisableFKCommandText(_tables.TablesToDisableFKContraints, allRelationships);

            _deleteSql = DbAdapter.BuildDeleteCommandText(_tables.TablesToDelete);

        }

        private static DatabaseTables BuildTableList(ICollection<string> allTables, IList<Relationship> allRelationships,
            List<string> tablesToDelete = null)
        {
            if (tablesToDelete == null)
            {
                tablesToDelete = new List<string>();
            }

            var referencedTables = allRelationships
                .Where(rel => !rel.IsSelfReferencing)
                .Select(rel => rel.PrimaryKeyTable)
                .Distinct()
                .ToList();

            var leafTables = allTables.Except(referencedTables).ToList();

            if (referencedTables.Count > 0 && leafTables.Count == 0)
            {
               return new DatabaseTables { TablesToDelete = tablesToDelete.ToArray(), TablesToDisableFKContraints = referencedTables.ToArray() };
            }

            tablesToDelete.AddRange(leafTables);

            if (referencedTables.Any())
            {
                var relationships = allRelationships.Where(x => !leafTables.Contains(x.ForeignKeyTable)).ToArray();
                var tables = allTables.Except(leafTables).ToArray();
                return BuildTableList(tables, relationships, tablesToDelete);
            }

            return new DatabaseTables { TablesToDelete = tablesToDelete.ToArray() };
        }

        private async Task<IList<Relationship>> GetRelationships(DbConnection connection)
        {
            var rels = new List<Relationship>();
            var commandText = DbAdapter.BuildRelationshipCommandText(this);

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = commandText;
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var rel = new Relationship
                        {
                            Name = reader.GetString(0),
                            PrimaryKeyTable = $"{DbAdapter.QuoteCharacter}{reader.GetString(1)}{DbAdapter.QuoteCharacter}.{DbAdapter.QuoteCharacter}{reader.GetString(2)}{DbAdapter.QuoteCharacter}",
                            ForeignKeyTable = $"{DbAdapter.QuoteCharacter}{reader.GetString(3)}{DbAdapter.QuoteCharacter}.{DbAdapter.QuoteCharacter}{reader.GetString(4)}{DbAdapter.QuoteCharacter}"
                        };
                        rels.Add(rel);
                    }
                }
            }

            return rels;
        }

        private async Task<IList<string>> GetAllTables(DbConnection connection)
        {
            var tables = new List<string>();

            string commandText = DbAdapter.BuildTableCommandText(this);

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = commandText;
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        if (!await reader.IsDBNullAsync(0))
                        {
                            tables.Add($"{DbAdapter.QuoteCharacter}{reader.GetString(0)}{DbAdapter.QuoteCharacter}.{DbAdapter.QuoteCharacter}{reader.GetString(1)}{DbAdapter.QuoteCharacter}");
                        }
                        else
                        {
                            tables.Add($"{DbAdapter.QuoteCharacter}{reader.GetString(1)}{DbAdapter.QuoteCharacter}");
                        }
                    }
                }
            }

            return tables.ToList();
        }
    }
}
