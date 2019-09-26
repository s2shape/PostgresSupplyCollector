using System;
using System.IO;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using S2.BlackSwan.SupplyCollector.Models;
using SupplyCollectorDataLoader;

namespace PostgresSupplyCollectorLoader
{
    public class PostgresSupplyCollectorLoader : SupplyCollectorDataLoaderBase
    {
        public override void InitializeDatabase(DataContainer dataContainer) {
            // do nothing
        }

        private NpgsqlDbType ConvertDbType(DataType dataType)
        {
            switch (dataType)
            {
                case DataType.String:
                    return NpgsqlDbType.Text;
                case DataType.Int:
                    return NpgsqlDbType.Integer;
                case DataType.Double:
                    return NpgsqlDbType.Double;
                case DataType.Boolean:
                    return NpgsqlDbType.Boolean;
                case DataType.DateTime:
                    return NpgsqlDbType.Timestamp;
                default:
                    return NpgsqlDbType.Integer;
            }
        }

        public override void LoadSamples(DataEntity[] dataEntities, long count)
        {
            using (var conn = new NpgsqlConnection(dataEntities[0].Container.ConnectionString))
            {
                conn.Open();

                var sb = new StringBuilder();
                sb.Append("CREATE TABLE ");
                sb.Append(dataEntities[0].Collection.Name);
                sb.Append(" (\n");
                sb.Append("id_field serial PRIMARY KEY");

                foreach (var dataEntity in dataEntities)
                {
                    sb.Append(",\n");
                    sb.Append(dataEntity.Name);
                    sb.Append(" ");

                    switch (dataEntity.DataType)
                    {
                        case DataType.String:
                            sb.Append("text");
                            break;
                        case DataType.Int:
                            sb.Append("integer");
                            break;
                        case DataType.Double:
                            sb.Append("float");
                            break;
                        case DataType.Boolean:
                            sb.Append("boolean");
                            break;
                        case DataType.DateTime:
                            sb.Append("timestamp");
                            break;
                        default:
                            sb.Append("integer");
                            break;
                    }

                    sb.AppendLine();
                }

                sb.Append(");");

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sb.ToString();
                    cmd.ExecuteNonQuery();
                }

                var r = new Random();
                long rows = 0;
                while (rows < count)
                {
                    long bulkSize = 10000;
                    if (bulkSize + rows > count)
                        bulkSize = count - rows;

                    sb = new StringBuilder();
                    sb.Append("INSERT INTO ");
                    sb.Append(dataEntities[0].Collection.Name);
                    sb.Append("(");

                    bool first = true;
                    foreach (var dataEntity in dataEntities)
                    {
                        if (!first)
                        {
                            sb.Append(", ");
                        }
                        sb.Append(dataEntity.Name);
                        first = false;
                    }
                    sb.Append(") VALUES ");

                    for (int i = 0; i < bulkSize; i++)
                    {
                        if (i > 0)
                            sb.Append(", ");

                        sb.Append("(");
                        first = true;
                        foreach (var dataEntity in dataEntities)
                        {
                            if (!first)
                            {
                                sb.Append(", ");
                            }

                            switch (dataEntity.DataType)
                            {
                                case DataType.String:
                                    sb.Append("'");
                                    sb.Append(new Guid().ToString());
                                    sb.Append("'");
                                    break;
                                case DataType.Int:
                                    sb.Append(r.Next().ToString());
                                    break;
                                case DataType.Double:
                                    sb.Append(r.NextDouble().ToString().Replace(",", "."));
                                    break;
                                case DataType.Boolean:
                                    sb.Append(r.Next(100) > 50 ? "true" : "false");
                                    break;
                                case DataType.DateTime:
                                    var val = DateTimeOffset
                                        .FromUnixTimeMilliseconds(
                                            DateTimeOffset.Now.ToUnixTimeMilliseconds() + r.Next()).DateTime;
                                    sb.Append("'");
                                    sb.Append(val.ToString("s"));
                                    sb.Append("'");
                                    break;
                                default:
                                    sb.Append(r.Next().ToString());
                                    break;
                            }

                            first = false;
                        }

                        sb.Append(")");
                    }

                    sb.Append(";");

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = sb.ToString();
                        cmd.ExecuteNonQuery();
                    }

                    rows += bulkSize;
                    Console.Write(".");
                }

                Console.WriteLine();
            }
        }

        public override void LoadUnitTestData(DataContainer dataContainer) {
            using (var conn = new NpgsqlConnection(dataContainer.ConnectionString))
            {
                conn.Open();

                using (var reader = new StreamReader("tests/data.sql"))
                {
                    var sb = new StringBuilder();
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        if (String.IsNullOrEmpty(line))
                            continue;

                        sb.AppendLine(line);
                        if (line.TrimEnd().EndsWith(";"))
                        {
                            using (var cmd = conn.CreateCommand())
                            {
                                cmd.CommandTimeout = 600;
                                cmd.CommandText = sb.ToString();

                                cmd.ExecuteNonQuery();
                            }

                            sb.Clear();
                        }
                    }
                }
            }

        }
    }
}
