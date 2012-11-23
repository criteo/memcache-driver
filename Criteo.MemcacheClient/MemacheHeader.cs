using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Criteo.MemcacheClient
{
    public struct MemacheRequestHeader
    {
        public MemacheRequestHeader(Opcode instruction)
        {
            Opcode = instruction;
            KeyLength = 0;
            ExtraLength = 0;
            DataType = 0;
            Reserved = 0;
            TotalBodyLength = 0;
            Opaque = 0;
            Cas = 0;
        }

        private const byte Magic = 0x80;
        public Opcode Opcode;
        public ushort KeyLength;
        public byte ExtraLength;
        public byte DataType;
        private ushort Reserved;
        public uint TotalBodyLength;
        public uint Opaque;
        public ulong Cas;

        public void ToData(byte[] data, int offset = 0)
        {
            data[offset] = Magic;
            data[1 + offset] = (byte)Opcode;
            data.CopyFrom(2 + offset, KeyLength);
            data[4 + offset] = ExtraLength;
            data[5 + offset] = DataType;
            data.CopyFrom(6 + offset, Reserved);
            data.CopyFrom(8 + offset, TotalBodyLength);
            data.CopyFrom(12 + offset, Opaque);
            data.CopyFrom(16 + offset, Cas);
        }
    }

    public struct MemacheResponseHeader
    {
        private const byte Magic = 0x81;
        public Opcode Opcode;
        public ushort KeyLength;
        public byte ExtraLength;
        public byte DataType;
        public Status Status;
        public uint TotalBodyLength;
        public uint Opaque;
        public ulong Cas;

        public void FromData(byte[] data, int offset = 0)
        {
            if (data[offset] != Magic)
                throw new ArgumentException("The buffer does not begin with the MagicNumber");
            Opcode = (Opcode)data[1 + offset];
            KeyLength = data.CopyToUShort(2 + offset);
            ExtraLength = data[4 + offset];
            DataType = data[5 + offset];
            Status = (Status)data.CopyToUShort(6 + offset);
            TotalBodyLength = data.CopyToUInt(8 + offset);
            Opaque = data.CopyToUInt(12 + offset);
            Cas = data.CopyToULong(16 + offset);
        }
    }

    struct UdpHeader
    {
        public ushort RequestId;
        public ushort SequenceNumber;
        public ushort TotalDatagrams;
        public const ushort Reserved = 0;

        public void FromData(byte[] data)
        {
            RequestId = data.CopyToUShort(0);
            SequenceNumber = data.CopyToUShort(2);
            TotalDatagrams = data.CopyToUShort(4);
            if(Reserved != data.CopyToUShort(6))
                throw new ArgumentException("Udp header reserved field in not null");
        }

        public void ToData(byte[] data)
        {
            data.CopyFrom(0, RequestId);
            data.CopyFrom(2, SequenceNumber);
            data.CopyFrom(4, TotalDatagrams);
            data.CopyFrom(6, Reserved);
        }
    }

    public enum Status : ushort
    {
        NoError = 0x0000,
        KeyNotFound = 0x0001,
        KeyExists = 0x0002,
        ValueTooLarge = 0x0003,
        InvalidArguments = 0x0004,
        ItemNotStored = 0x0005,
        IncrDecrOnNonNumeric = 0x0006,
        UnknownCommand = 0x0081,
        OutOfMemory = 0x0082,
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

        static public unsafe ulong CopyToULong(this byte[] data, int offset)
        {
            ulong value;
            byte* bufferIn = (byte*)&value;
            for (int i = 0; i < sizeof(ulong); ++i)
                bufferIn[i] = data[offset + sizeof(ulong) - i - 1];
            return value;
        }

        static public void CopyTo<T>(this IEnumerable<T> that, T[] buffer, int offset)
        {
            foreach(T item in that)
                buffer[offset++] = item;
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
