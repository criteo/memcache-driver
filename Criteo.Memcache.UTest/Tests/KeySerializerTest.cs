/* Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*/
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
