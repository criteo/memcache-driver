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
﻿namespace Criteo.Memcache.Headers
{
    public enum Status : ushort
    {
        NoError = 0x0000,
        KeyNotFound = 0x0001,
        KeyExists = 0x0002,
        ValueTooLarge = 0x0003,
        InvalidArguments = 0x0004,
        ItemNotStored = 0x0005,
        IncrDecrOnNonNumeric = 0x0006,

        // Revamp from http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Response_Status
        VbucketBelongsToAnotherServer = 0x0007,
        AuthenticationError = 0x0008,
        AuthenticationContinue = 0x0009,
        NotSupported = 0x0083,
        InternalError = 0x0084,
        Busy = 0x0085,
        TemporaryFailure = 0x0086,
        // End revamp

        UnknownCommand = 0x0081,
        OutOfMemory = 0x0082,

        // SASL Commands from Couchbase
        AuthRequired = 0x0020,
        StepRequired = 0x0021,
    }

    public enum Opcode : byte
    {
        Get = 0x00,
        Set = 0x01,
        Add = 0x02,
        Replace = 0x03,
        Delete = 0x04,
        Increment = 0x05,
        Decrement = 0x06,
        Quit = 0x07,
        Flush = 0x08,
        GetQ = 0x09,
        NoOp = 0x0A,
        Version = 0x0B,
        GetK = 0x0C,
        GetKQ = 0x0D,
        Append = 0x0E,
        Prepend = 0x0F,
        Stat = 0x10,
        SetQ = 0x11,
        AddQ = 0x12,
        ReplaceQ = 0x13,
        DeleteQ = 0x14,
        IncrementQ = 0x15,
        DecrementQ = 0x16,
        QuitQ = 0x17,
        FlushQ = 0x18,
        AppendQ = 0x19,
        PrependQ = 0x1A,

        // revamp comands
        // https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Command_Opcodes
        Verbosity = 0x1b,
        Touch = 0x1c,
        GAT = 0x1d,
        GATQ = 0x1e,
        SASL_list_mechs = 0x20,
        ListAuth = SASL_list_mechs,
        SASL_Auth = 0x21,
        StartAuth = SASL_Auth,
        SASL_Step = 0x22,
        StepAuth = SASL_Step,
        RGet = 0x30,
        RSet = 0x31,
        RSetQ = 0x32,
        RAppend = 0x33,
        RAppendQ = 0x34,
        RPrepend = 0x35,
        RPrependQ = 0x36,
        RDelete = 0x37,
        RDeleteQ = 0x38,
        RIncr = 0x39,
        RIncrQ = 0x3a,
        RDecr = 0x3b,
        RDecrQ = 0x3c,
        Set_VBucket = 0x3d,
        Get_VBucket = 0x3e,
        Del_VBucket = 0x3f,
        TAP_Connect = 0x40,
        TAP_Mutation = 0x41,
        TAP_Delete = 0x42,
        TAP_Flush = 0x43,
        TAP_Opaque = 0x44,
        TAP_VBucket_Set = 0x45,
        TAP_Checkpoint_Start = 0x46,
        TAP_Checkpoint_End = 0x47,
    }

    public static class HeaderUtil
    {
        static public unsafe void CopyFrom(this byte[] data, int offset, ushort value)
        {
            byte* bufferIn = (byte*)&value;
            for (int i = 0; i < sizeof(ushort); ++i)
                data[offset + sizeof(ushort) - i - 1] = bufferIn[i];
        }

        static public unsafe void CopyFrom(this byte[] data, int offset, uint value)
        {
            byte* bufferIn = (byte*)&value;
            for (int i = 0; i < sizeof(uint); ++i)
                data[offset + sizeof(uint) - i - 1] = bufferIn[i];
        }

        static public unsafe void CopyFrom(this byte[] data, int offset, ulong value)
        {
            byte* bufferIn = (byte*)&value;
            for (int i = 0; i < sizeof(ulong); ++i)
                data[offset + sizeof(ulong) - i - 1] = bufferIn[i];
        }

        static public unsafe ushort CopyToUShort(this byte[] data, int offset)
        {
            ushort value;
            byte* bufferIn = (byte*)&value;
            for (int i = 0; i < sizeof(ushort); ++i)
                bufferIn[i] = data[offset + sizeof(ushort) - i - 1];
            return value;
        }

        static public unsafe uint CopyToUInt(this byte[] data, int offset)
        {
            uint value;
            byte* bufferIn = (byte*)&value;
            for (int i = 0; i < sizeof(uint); ++i)
                bufferIn[i] = data[offset + sizeof(uint) - i - 1];
            return value;
        }

        static public unsafe uint CopyToUIntNoRevert(this byte[] data, int offset)
        {
            fixed(byte* dataPtr = &data[offset])
                return ((uint*)dataPtr)[0];
        }

        static public unsafe ulong CopyToULong(this byte[] data, int offset)
        {
            ulong value;
            byte* bufferIn = (byte*)&value;
            for (int i = 0; i < sizeof(ulong); ++i)
                bufferIn[i] = data[offset + sizeof(ulong) - i - 1];
            return value;
        }

        static public bool IsQuiet(this Opcode that)
        {
            switch (that)
            {
                case Opcode.GetQ:
                case Opcode.GetKQ:
                case Opcode.SetQ:
                case Opcode.AddQ:
                case Opcode.ReplaceQ:
                case Opcode.DeleteQ:
                case Opcode.IncrementQ:
                case Opcode.DecrementQ:
                case Opcode.QuitQ:
                case Opcode.FlushQ:
                case Opcode.AppendQ:
                case Opcode.PrependQ:
                    return true;
                default:
                    return false;
            }
        }
    }
}
