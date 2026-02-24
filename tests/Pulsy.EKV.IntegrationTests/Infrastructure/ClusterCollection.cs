using Xunit;

namespace Pulsy.EKV.IntegrationTests.Infrastructure;

[CollectionDefinition(Name)]
public class ClusterCollection : ICollectionFixture<ClusterFixture>
{
    public const string Name = "Cluster";
}
