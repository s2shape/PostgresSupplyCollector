using System;
using System.Collections.Generic;
using System.Linq;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;

namespace PostgresSupplyCollectorTests
{
    public class PostgresSupplyCollectorTests {
        private readonly PostgresSupplyCollector.PostgresSupplyCollector _instance;
        public readonly DataContainer _container;

        public PostgresSupplyCollectorTests() {
            _instance = new PostgresSupplyCollector.PostgresSupplyCollector();
            _container = new DataContainer()
            {
                ConnectionString = _instance.BuildConnectionString("postgres", "postgres", "postgres", "localhost", 5432)
            };
        }

        [Fact]
        public void DataStoreTypesTest()
        {
            var result = _instance.DataStoreTypes();
            Assert.Contains("PostgreSQL", result);
        }

        [Fact]
        public void TestConnectionTest()
        {
            var result = _instance.TestConnection(_container);
            Assert.True(result);
        }

        [Fact]
        public void GetDataCollectionMetricsTest() {
            var metrics = new DataCollectionMetrics[] {
                new DataCollectionMetrics()
                    {Name = "test_data_types", RowCounts = 1, TotalSpaceKB = 8, TotalSpaceMB = (decimal)0.008},
                new DataCollectionMetrics()
                    {Name = "test_arrays", RowCounts = 1, TotalSpaceKB = 8, TotalSpaceMB = (decimal)0.008},
                new DataCollectionMetrics()
                    {Name = "test_field_names", RowCounts = 1, TotalSpaceKB = 8, TotalSpaceMB = (decimal)0.008},
                new DataCollectionMetrics()
                    {Name = "test_index", RowCounts = 7, TotalSpaceKB = 8, TotalSpaceMB = (decimal)0.008},
                new DataCollectionMetrics()
                    {Name = "test_index_ref", RowCounts = 2, TotalSpaceKB = 8, TotalSpaceMB = (decimal)0.008}
            };

            var result = _instance.GetDataCollectionMetrics(_container);
            Assert.Equal(5, result.Count);

            foreach (var metric in metrics) {
                var resultMetric = result.Find(x => x.Name.Equals(metric.Name));
                Assert.NotNull(resultMetric);

                Assert.Equal(metric.RowCounts, resultMetric.RowCounts);
                Assert.Equal(metric.TotalSpaceKB, resultMetric.TotalSpaceKB);
                Assert.Equal(metric.TotalSpaceMB, resultMetric.TotalSpaceMB, 3);
            }
        }

        [Fact]
        public void GetTableNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);
            Assert.Equal(5, tables.Count);
            Assert.Equal(31, elements.Count);

            var tableNames = new string[] {"test_data_types", "test_arrays", "test_field_names", "test_index", "test_index_ref"};
            foreach (var tableName in tableNames) {
                var table = tables.Find(x => x.Name.Equals(tableName));
                Assert.NotNull(table);
            }
        }

        [Fact]
        public void DataTypesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var dataTypes = new Dictionary<string, string>() {
                {"serial_field", "integer"},
                {"bool_field", "boolean"},
                {"char_field", "character"},
                {"varchar_field", "character varying"},
                {"text_field", "text"},
                {"smallint_field", "smallint"},
                {"int_field", "integer"},
                {"float_field", "double precision"},
                {"real_field", "real"},
                {"numeric_field", "numeric"},
                {"date_field", "date"},
                {"time_field", "time without time zone"},
                {"timestamp_field", "timestamp without time zone"},
                {"timestamptz_field", "timestamp with time zone"},
                {"interval_field", "interval"},
                {"json_field", "json"},
                {"uuid_field", "uuid"}
            };

            var columns = elements.Where(x => x.Collection.Name.Equals("test_data_types")).ToArray();
            Assert.Equal(17, columns.Length);

            foreach (var column in columns) {
                Assert.Contains(column.Name, (IDictionary<string, string>)dataTypes);
                Assert.Equal(column.DbDataType, dataTypes[column.Name]);
            }
        }

        [Fact]
        public void SpecialFieldNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var fieldNames = new string[] { "id", "low_case", "upcase", "camelcase", "Table", "array", "SELECT" }; // first 4 without quotes are converted to lower case

            var columns = elements.Where(x => x.Collection.Name.Equals("test_field_names")).ToArray();
            Assert.Equal(fieldNames.Length, columns.Length);

            foreach (var column in columns) {
                Assert.Contains(column.Name, fieldNames);
            }
        }

        [Fact]
        public void AttributesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var idFields = elements.Where(x => x.Name.Equals("id")).ToArray();
            Assert.Equal(4, idFields.Length);

            foreach (var idField in idFields) {
                Assert.Equal("integer", idField.DataType);
                Assert.True(idField.IsAutoNumber);
                Assert.True(idField.IsPrimaryKey);
            }

            var uniqueField = elements.Find(x => x.Name.Equals("name"));
            Assert.True(uniqueField.IsUniqueKey);

            var refField = elements.Find(x => x.Name.Equals("index_id"));
            Assert.True(refField.IsForeignKey);

            foreach (var column in elements) {
                if (column.Name.Equals("id") || column.Name.Equals("name") || column.Name.Equals("index_id")) {
                    continue;
                }

                Assert.False(column.IsPrimaryKey);
                Assert.False(column.IsAutoNumber);
                Assert.False(column.IsForeignKey);
                Assert.False(column.IsIndexed);
            }
        }


        [Fact]
        public void ArraysTest()
        {
            var textEntity = new DataEntity()
            {
                Container = _container,
                Collection = new DataCollection()
                {
                    Container = _container,
                    Name = "test_arrays"
                },
                Name = "text_array"
            };
            var samples = _instance.CollectSample(textEntity, 1);
            Assert.True(samples != null && samples.Count == 1);

            Assert.Equal("one,two", samples[0]);

            var intEntity = new DataEntity()
            {
                Container = _container,
                Collection = new DataCollection()
                {
                    Container = _container,
                    Name = "test_arrays"
                },
                Name = "int_array"
            };
            samples = _instance.CollectSample(intEntity, 1);
            Assert.True(samples != null && samples.Count == 1);

            Assert.Equal("1,2,3", samples[0]);
        }


        [Fact]
        public void CollectSampleTest()
        {
            var entity = new DataEntity() {
                Container = _container,
                Collection = new DataCollection() {
                    Container = _container,
                    Name = "test_index"
                },
                Name = "name"
            };
            var samples = _instance.CollectSample(entity, 5);
            Assert.Equal(5, samples.Count);
            Assert.Contains("Wednesday", samples);
        }

    }
}
