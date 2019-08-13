using System;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;

namespace PostgresSupplyCollectorTests
{
    public class PostgresSupplyCollectorTests {
        private readonly PostgresSupplyCollector.PostgresSupplyCollector _instance;
        public readonly DataContainer _container;
        public readonly DataEntity _emailToAddress;

        public PostgresSupplyCollectorTests() {
            _instance = new PostgresSupplyCollector.PostgresSupplyCollector();
            _container = new DataContainer()
            {
                ConnectionString = _instance.BuildConnectionString("postgres", "postgres", "postgres", "localhost", 5432)
            };
            _emailToAddress = new DataEntity()
            {
                Container = _container,
                Collection = new DataCollection()
                {
                    Name = "EMAIL"
                },
                Name = "TO_ADDRS_EMAILS"
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
        public void GetDataCollectionMetricsTest()
        {
            var result = _instance.GetDataCollectionMetrics(_container);
            var contactsAuditMetrics = result.Find(x => x.Name == "CONTACTS_AUDIT");
            var leadsMetrics = result.Find(x => x.Name == "LEADS");
            var emailMetrics = result.Find(x => x.Name == "EMAIL");

            Assert.Equal(3, result.Count);

            Assert.Equal(200, contactsAuditMetrics.RowCounts);
            Assert.Equal(116, contactsAuditMetrics.TotalSpaceKB);
            Assert.Equal(.1M, contactsAuditMetrics.TotalSpaceMB, 1);

            Assert.Equal(200, leadsMetrics.RowCounts);
            Assert.Equal(104, leadsMetrics.TotalSpaceKB);
            Assert.Equal(.1M, leadsMetrics.TotalSpaceMB, 1);

            Assert.Equal(200, emailMetrics.RowCounts);
            Assert.Equal(84, emailMetrics.TotalSpaceKB);
            Assert.Equal(.1M, emailMetrics.TotalSpaceMB, 1);
        }

        [Fact]
        public void GetSchemaTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);
            Assert.Equal(3, tables.Count);
            Assert.Equal(156, elements.Count);
            foreach (DataEntity element in elements)
            {
                Assert.NotEmpty(element.DataType);
                Assert.NotEmpty(element.DbDataType);
            }
        }

        [Fact]
        public void CollectSampleTest()
        {
            var samples = _instance.CollectSample(_emailToAddress, 161);
            Assert.Equal(161, samples.Count);
            Assert.Contains("qa25@example.com", samples);
        }

    }
}
