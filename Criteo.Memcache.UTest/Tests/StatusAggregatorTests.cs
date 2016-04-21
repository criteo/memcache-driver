using Criteo.Memcache.Headers;
using NUnit.Framework;

namespace Criteo.Memcache.UTest.Tests
{
    /// <summary>
    /// Test of StatusAggregator
    /// </summary>
    [TestFixture]
    public class StatusAggregatorTests
    {
        [TestCase(Status.NoError,                                               Result = Status.NoError)]
        [TestCase(Status.KeyNotFound,                                           Result = Status.KeyNotFound)]
        [TestCase(Status.InvalidArguments,                                      Result = Status.InvalidArguments)]
        [TestCase(Status.InvalidArguments, Status.NoError,                      Result = Status.NoError)]
        [TestCase(Status.NoError, Status.InvalidArguments,                      Result = Status.NoError)]
        [TestCase(Status.NoError, Status.KeyNotFound,                           Result = Status.NoError)]
        [TestCase(Status.KeyNotFound, Status.NoError,                           Result = Status.NoError)]
        [TestCase(Status.KeyNotFound, Status.InternalError,                     Result = Status.InternalError)]
        [TestCase(Status.InternalError, Status.KeyNotFound,                     Result = Status.InternalError)]
        [TestCase(Status.InternalError, Status.KeyNotFound, Status.NoError,     Result = Status.NoError)]
        [TestCase(Status.KeyNotFound, Status.KeyNotFound, Status.KeyNotFound,   Result = Status.KeyNotFound)]
        [TestCase(Status.KeyNotFound, Status.KeyNotFound, Status.InternalError, Result = Status.InternalError)]
        public Status TestAggregation(params Status[] statuses)
        {
            return StatusAggregator.AggregateStatus(statuses);
        }
    }
}
