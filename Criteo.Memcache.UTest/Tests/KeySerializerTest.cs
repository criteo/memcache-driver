using System;
using Criteo.Memcache.KeySerializer;

using Moq;
using Moq.Protected;

using NUnit.Framework;

namespace Criteo.Memcache.UTest.Tests
{
    [TestFixture]
    public class KeySerializerTest
    {
        [Test]
        public void KeyLengthCheckingTest()
        {
            var serializerOK = new Mock<KeySerializerWithChecks<string>>();
            serializerOK
                .Protected()
                .Setup<byte[]>("DoSerializeToBytes", ItExpr.IsAny<string>())
                .Returns(new byte[250]);

            Assert.DoesNotThrow(
                () => serializerOK.Object.SerializeToBytes("SomeShortKey"),
                "Serializing a small enough key should not cause any error");

            var serializerNotOK = new Mock<KeySerializerWithChecks<string>>();
            serializerNotOK
                .Protected()
                .Setup<byte[]>("DoSerializeToBytes", ItExpr.IsAny<string>())
                .Returns(new byte[255]);

            Assert.Throws<ArgumentException>(
                () => serializerNotOK.Object.SerializeToBytes("SomeVeryLongKey"),
                "Serializing a very large key should throw an ArgumentException");
        }

        [TestCase(null, null)]
        [TestCase("", new byte[0])]
        [TestCase("hello", new byte[] { (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o' })]
        public void UTF8KeySerializerTest(string input, byte[] output)
        {
            Assert.AreEqual(new UTF8KeySerializer().SerializeToBytes(input), output);
        }
    }
}
