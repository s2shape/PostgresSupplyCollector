using System;
using System.Collections.Generic;
using System.Linq;
using Npgsql;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;

namespace PostgresSupplyCollector
{
    public class PostgresSupplyCollector : SupplyCollectorBase
    {
        public override List<string> DataStoreTypes() {
            return (new[] { "PostgreSQL" }).ToList();
        }

        public string BuildConnectionString(string user, string password, string database, string host, int port = 5432) {
            var builder = new NpgsqlConnectionStringBuilder();
            builder.Host = host;
            builder.Port = port;
            builder.Database = database;
            builder.Username = user;
            builder.Password = password;
            return builder.ConnectionString;
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            var result = new List<string>();
            using (var conn = new NpgsqlConnection(dataEntity.Container.ConnectionString)) {
                conn.Open();

                using (var cmd = conn.CreateCommand()) {
                    cmd.CommandText = $"SELECT {dataEntity.Name} FROM {dataEntity.Collection.Name} LIMIT {sampleSize}";

                    using (var reader = cmd.ExecuteReader()) {
                        while (reader.Read()) {
                            result.Add(reader[0].ToString());
                        }
                    }
                }
            }
            return result;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            var metrics = new List<DataCollectionMetrics>();

            using (var conn = new NpgsqlConnection(container.ConnectionString))
            {
                conn.Open();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = 
                        "select t.table_schema, t.table_name, pg_relation_size('\"' || t.table_schema || '\".\"' || t.table_name || '\"') as size, s.n_live_tup, s.n_dead_tup\n" +
                        "from information_schema.tables t\n" +
                        "left outer join pg_stat_user_tables s on s.schemaname = t.table_schema and s.relname = t.table_name\n" +
                        "where table_schema not in ('pg_catalog', 'information_schema')";

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read()) {
                            int column = 0;

                            var schema = reader.GetString(column++);
                            var table = reader.GetString(column++);
                            var size = reader.GetInt64(column++);
                            var liveRows = reader.GetInt64(column++);
                            var deadRows = reader.GetInt64(column++);

                            var deadSize = ((double) size / (liveRows + deadRows)) * deadRows;

                            metrics.Add(new DataCollectionMetrics() {
                                Schema = schema,
                                Name = table,
                                RowCounts = liveRows,
                                TotalSpaceKB = size / 1024,
                                TotalSpaceMB = (decimal)size / (1024 * 1024),
                                UnUsedSpaceKB = (long)(deadSize / 1024),
                                UnUsedSpaceMB = (decimal)deadSize / (1024 * 1024),
                                UsedSpaceKB = (long)(size - deadSize) / 1024,
                                UsedSpaceMB = (decimal)(size - deadSize) / (1024 * 1024)
                            });
                        }
                    }
                }
            }

            return metrics;
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container) {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            using (var conn = new NpgsqlConnection(container.ConnectionString))
            {
                conn.Open();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText =
                        "select c.table_schema, c.table_name, c.column_name, c.data_type, c.is_generated, c.is_nullable, c.is_identity,\n"+
                        "(select count(*)\n" +
                        "   from information_schema.constraint_column_usage ccu\n" +
                        "   join information_schema.table_constraints tc on ccu.constraint_name = tc.constraint_name and ccu.constraint_schema = tc.constraint_schema and tc.constraint_type = 'PRIMARY KEY'\n" +
                        "   where ccu.table_schema = c.table_schema and ccu.table_name = c.table_name and ccu.column_name = c.column_name\n" +
                        ") as is_primary,\n" +
                        "(select count(*)\n" +
                        "   from information_schema.constraint_column_usage ccu\n" +
                        "   join information_schema.table_constraints tc on ccu.constraint_name = tc.constraint_name and ccu.constraint_schema = tc.constraint_schema and tc.constraint_type = 'UNIQUE'\n" +
                        "   where ccu.table_schema = c.table_schema and ccu.table_name = c.table_name and ccu.column_name = c.column_name\n" +
                        ") as is_unique,\n" +
                        "(select count(*)\n" +
                        "   from information_schema.key_column_usage kcu\n" +
                        "   join information_schema.table_constraints tc on kcu.constraint_name = tc.constraint_name and kcu.constraint_schema = tc.constraint_schema and tc.constraint_type = 'FOREIGN KEY'\n" +
                        "   where kcu.table_schema = c.table_schema and kcu.table_name = c.table_name and kcu.column_name = c.column_name\n" +
                        ") as is_ref\n" +
                        "from information_schema.columns c\n" +
                        "where c.table_schema not in ('pg_catalog', 'information_schema')\n" +
                        "order by table_schema, table_name, ordinal_position";

                    DataCollection collection = null;
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int column = 0;

                            var schema = reader.GetString(column++);
                            var table = reader.GetString(column++);
                            var columnName = reader.GetString(column++);
                            var dataType = reader.GetString(column++);
                            var isGenerated = "YES".Equals(reader.GetString(column++), StringComparison.InvariantCultureIgnoreCase);
                            var isNullable = "YES".Equals(reader.GetString(column++), StringComparison.InvariantCultureIgnoreCase);
                            var isIdentity = "YES".Equals(reader.GetString(column++), StringComparison.InvariantCultureIgnoreCase);
                            var isPrimary = reader.GetInt64(column++) > 0;
                            var isUnique = reader.GetInt64(column++) > 0;
                            var isRef = reader.GetInt64(column++) > 0;

                            if (collection == null || !collection.Schema.Equals(schema) ||
                                !collection.Name.Equals(table)) {

                                collection = new DataCollection() {
                                    Container = container,
                                    Schema = schema,
                                    Name = table
                                };
                                collections.Add(collection);
                            }

                            entities.Add(new DataEntity() {
                                Container = container,
                                Collection = collection,
                                DbDataType = dataType,
                                DataType = dataType,
                                IsAutoNumber = isGenerated,
                                IsComputed = isGenerated,
                                IsForeignKey = isRef,
                                IsIndexed = isPrimary || isRef,
                                IsPrimaryKey = isPrimary,
                                IsUniqueKey = isUnique,
                                Name = columnName
                            });
                        }
                    }
                }
            }

            return (collections, entities);
        }

        public override bool TestConnection(DataContainer container) {
            try {
                using (var conn = new NpgsqlConnection(container.ConnectionString)) {
                    conn.Open();
                }

                return true;
            }
            catch (Exception) {
                return false;
            }
        }
    }
}
