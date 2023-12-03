//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Cassandra.Responses;
using Cassandra.Serialization;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests
{
    [TestFixture]
    public class FrameParserTests
    {
        private const ProtocolVersion Version = ProtocolVersion.MaxSupported;
        private static readonly ISerializerManager Serializer = new SerializerManager(Version);
        private const int ReadFailureErrorCode = 0x1300;
        private const int WriteFailureErrorCode = 0x1500;

        [Test]
        public void Should_Parse_ErrorResponse_Of_Syntax_Error()
        {
            var body = GetErrorBody(0x2000, "Test syntax error");
            var header = FrameHeader.ParseResponseHeader(Version, GetHeaderBuffer(body.Length), 0);
            var response = FrameParser.Parse(new Frame(header, new MemoryStream(body), Serializer.GetCurrentSerializer(), null));
            var ex = IsErrorResponse<SyntaxError>(response);
            Assert.That("Test syntax error", Is.EqualTo(ex.Message));
        }
        
        [Test]
        public void Should_Parse_ErrorResponse_With_Warnings()
        {
            // Protocol warnings are [string list]: A [short] n, followed by n [string]
            var warningBuffers = BeConverter.GetBytes((ushort)1).Concat(GetProtocolString("Test warning"));
            var body = warningBuffers.Concat(GetErrorBody(0x2000, "Test syntax error")).ToArray();
            var header = FrameHeader.ParseResponseHeader(Version, GetHeaderBuffer(body.Length, HeaderFlags.Warning), 0);
            var response = FrameParser.Parse(new Frame(header, new MemoryStream(body), Serializer.GetCurrentSerializer(), null));
            var ex = IsErrorResponse<SyntaxError>(response);
            Assert.That("Test syntax error", Is.EqualTo(ex.Message));
        }

        [Test]
        public void Should_Parse_ErrorResponse_For_Read_Failure_Under_Protocol_V4()
        {
            // <cl><received><blockfor><numfailures><data_present>
            var additional = new byte[] {0, (byte) ConsistencyLevel.LocalQuorum} // Consistency level
                            .Concat(new byte[] {0, 0, 0, 1}) // Received
                            .Concat(new byte[] {0, 0, 0, 3}) // Required
                            .Concat(new byte[] {0, 0, 0, 2}) // Failures (int)
                            .Concat(new byte[] {1}); // Data present
            var body = GetErrorBody(ReadFailureErrorCode, "Test error message", additional);
            var response = GetResponse(body, ProtocolVersion.V4);
            var ex = IsErrorResponse<ReadFailureException>(response);
            Assert.That(ConsistencyLevel.LocalQuorum, Is.EqualTo(ex.ConsistencyLevel));
            Assert.That(1, Is.EqualTo(ex.ReceivedAcknowledgements));
            Assert.That(3, Is.EqualTo(ex.RequiredAcknowledgements));
            Assert.That(2, Is.EqualTo(ex.Failures));
            Assert.That(ex.WasDataRetrieved, Is.True);
            Assert.That(ex.Reasons, Is.EqualTo(Is.Empty));
        }

        [Test]
        public void Should_Parse_ErrorResponse_For_Read_Failure_Under_Latest_Protocol()
        {
            // <cl><received><blockfor><reasons><data_present>
            var additional = new byte[] {0, (byte) ConsistencyLevel.LocalQuorum} // Consistency level
                             .Concat(new byte[] {0, 0, 0, 2}) // Received
                             .Concat(new byte[] {0, 0, 0, 3}) // Required
                             .Concat(new byte[] {0, 0, 0, 1}) // Reasons length (int)
                             .Concat(new byte[] {4, 10, 10, 0, 1}) // Reasons first item IP
                             .Concat(new byte[] {0, 5}) // Reasons first item code
                             .Concat(new byte[] {0}); // Data present
            var body = GetErrorBody(ReadFailureErrorCode, "Test error message", additional);
            var response = GetResponse(body);
            var ex = IsErrorResponse<ReadFailureException>(response);
            Assert.That(ConsistencyLevel.LocalQuorum, Is.EqualTo(ex.ConsistencyLevel));
            Assert.That(2, Is.EqualTo(ex.ReceivedAcknowledgements));
            Assert.That(3, Is.EqualTo(ex.RequiredAcknowledgements));
            Assert.That(1, Is.EqualTo(ex.Failures));
            Assert.That(ex.WasDataRetrieved, Is.False);
            Assert.That(ex.Reasons, Is.EquivalentTo(new Dictionary<IPAddress, int>
            {
                { IPAddress.Parse("10.10.0.1"), 5 }
            }));
        }

        [Test]
        public void Should_Parse_ErrorResponse_For_Write_Failure_Under_Protocol_V4()
        {
            // <cl><received><blockfor><reasonmap><write_type>
            var additional = new byte[] {0, (byte) ConsistencyLevel.All} // Consistency level
                             .Concat(new byte[] {0, 0, 0, 1}) // Received
                             .Concat(new byte[] {0, 0, 0, 3}) // Required
                             .Concat(new byte[] {0, 0, 0, 2}) // Failures (int)
                             .Concat(new byte[] {0, (byte) "SIMPLE".Length}) // WriteType length
                             .Concat(Encoding.UTF8.GetBytes("SIMPLE")); // WriteType text

            var body = GetErrorBody(WriteFailureErrorCode, "Test error message", additional);
            var response = GetResponse(body, ProtocolVersion.V4);
            var ex = IsErrorResponse<WriteFailureException>(response);
            Assert.That(ConsistencyLevel.All, Is.EqualTo(ex.ConsistencyLevel));
            Assert.That(1, Is.EqualTo(ex.ReceivedAcknowledgements));
            Assert.That(3, Is.EqualTo(ex.RequiredAcknowledgements));
            Assert.That(2, Is.EqualTo(ex.Failures));
            Assert.That("SIMPLE", Is.EqualTo(ex.WriteType));
            Assert.That(ex.Reasons, Is.Empty);
        }

        [Test]
        public void Should_Parse_ErrorResponse_For_Write_Failure_Under_Latest_Protocol()
        {
            // <cl><received><blockfor><numfailures><write_type>
            var additional = new byte[] {0, (byte) ConsistencyLevel.Quorum} // Consistency level
                             .Concat(new byte[] {0, 0, 0, 2}) // Received
                             .Concat(new byte[] {0, 0, 0, 3}) // Required
                             .Concat(new byte[] {0, 0, 0, 1}) // Reasons length (int)
                             .Concat(new byte[] {4, 12, 10, 0, 1}) // Reasons first item IP
                             .Concat(new byte[] {0, 4}) // Reasons first item code
                             .Concat(new byte[] {0, (byte) "COUNTER".Length}) // WriteType length
                             .Concat(Encoding.UTF8.GetBytes("COUNTER")); // WriteType text

            var body = GetErrorBody(WriteFailureErrorCode, "Test error message", additional);
            var response = GetResponse(body);
            var ex = IsErrorResponse<WriteFailureException>(response);
            Assert.That(ConsistencyLevel.Quorum, Is.EqualTo(ex.ConsistencyLevel));
            Assert.That(2, Is.EqualTo(ex.ReceivedAcknowledgements));
            Assert.That(3, Is.EqualTo(ex.RequiredAcknowledgements));
            Assert.That(1, Is.EqualTo(ex.Failures));
            Assert.That("COUNTER", Is.EqualTo(ex.WriteType));
            Assert.That(ex.Reasons, Is.EquivalentTo(new Dictionary<IPAddress, int>
            {
                { IPAddress.Parse("12.10.0.1"), 4 }
            }));
        }

        private static byte[] GetHeaderBuffer(int length, HeaderFlags flags = 0)
        {
            var headerBuffer = new byte[] {0x80 | (int) Version, (byte) flags, 0, 0, ErrorResponse.OpCode}
                .Concat(BeConverter.GetBytes(length));
            return headerBuffer.ToArray();
        }

        private static byte[] GetErrorBody(int code, string message, IEnumerable<byte> additional = null)
        {
            // error body = [int][string][additional]
            return BeConverter.GetBytes(code)
                              .Concat(GetProtocolString(message))
                              .Concat(additional ?? Array.Empty<byte>())
                              .ToArray();
        }

        private static Response GetResponse(byte[] body, ProtocolVersion version = Version)
        {
            var header = FrameHeader.ParseResponseHeader(version, GetHeaderBuffer(body.Length), 0);
            return FrameParser.Parse(new Frame(
                header, new MemoryStream(body), new SerializerManager(version).GetCurrentSerializer(), null));
        }

        private static byte[] GetProtocolString(string value)
        {
            var textBuffer = Encoding.UTF8.GetBytes(value);
            return BeConverter.GetBytes((ushort) textBuffer.Length).Concat(textBuffer).ToArray();
        }

        private static TException IsErrorResponse<TException>(Response response) where TException : DriverException
        {
            ClassicAssert.IsInstanceOf<ErrorResponse>(response);
            var ex = ((ErrorResponse) response).Output.CreateException();
            ClassicAssert.IsInstanceOf<TException>(ex);
            return (TException) ex;
        }
    }
}