
namespace Respawn
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Data.SqlClient;
    using System.Linq;

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

        internal class Relationship
        {
            public string Name { get; set; }
            public string PrimaryKeyTable { get; set; }
            public string ForeignKeyTable { get; set; }

            public bool IsSelfReferencing => PrimaryKeyTable == ForeignKeyTable;
        }

        public virtual void Reset(string nameOrConnectionString)
        {
            using (var connection = new SqlConnection(nameOrConnectionString))
            {
                connection.Open();

                Reset(connection);
            }
        }

        public virtual void Reset(DbConnection connection)
        {
            if (string.IsNullOrWhiteSpace(_deleteSql))
            {
                BuildDeleteTables(connection);
            }

            ExecuteDeleteSql(connection);
        }

        private void ExecuteDeleteSql(DbConnection connection)
        {
            using (var tx = connection.BeginTransaction())
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;
                cmd.CommandText = _deleteSql + _disableFKsSql;
                cmd.Transaction = tx;

                cmd.ExecuteNonQuery();

                tx.Commit();
            }
        }

        private void BuildDeleteTables(DbConnection connection)
        {
            var allTables = GetAllTables(connection);

            var allRelationships = GetRelationships(connection);

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
                //throw new InvalidOperationException("There is a circular dependency between the DB tables and we can't safely build the list of tables to delete.");
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

        private IList<Relationship> GetRelationships(DbConnection connection)
        {
            var rels = new List<Relationship>();
            var commandText = DbAdapter.BuildRelationshipCommandText(this);

            var values = new List<string>();
            values.AddRange(TablesToIgnore);
            values.AddRange(SchemasToExclude);
            values.AddRange(SchemasToInclude);

            using (var cmd = connection.CreateCommand(commandText, values.ToArray()))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var rel = new Relationship
                        {
                            Name = reader.GetString(0),
                            PrimaryKeyTable = "\"" + reader.GetString(1) + "\".\"" + reader.GetString(2) + "\"",
                            ForeignKeyTable = "\"" + reader.GetString(3) + "\".\"" + reader.GetString(4) + "\""
                        };
                        rels.Add(rel);
                    }
                }
            }

            return rels;
        }

        private IList<string> GetAllTables(DbConnection connection)
        {
            var tables = new List<string>();

            string commandText = DbAdapter.BuildTableCommandText(this);

            var values = new List<string>();
            values.AddRange(TablesToIgnore);
            values.AddRange(SchemasToExclude);
            values.AddRange(SchemasToInclude);

            using (var cmd = connection.CreateCommand(commandText, values.ToArray()))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tables.Add("\"" + reader.GetString(0) + "\".\"" + reader.GetString(1) + "\"");
                    }
                }
            }

            return tables.ToList();
        }
    }
}
