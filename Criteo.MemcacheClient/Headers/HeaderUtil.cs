using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Criteo.MemcacheClient.Headers
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
            foreach (T item in that)
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
