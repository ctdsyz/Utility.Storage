using System;
using System.Collections.Generic;
using System.Linq;
using Utility.Storage.StorageHelper.Common;


namespace Utility.Storage.StorageHelper
{
    public class ResultsReader
    {
        internal ResultsReader(SqlMapper.GridReader reader)
        {
            if (reader == null) throw new ArgumentNullException();

            this.reader = reader;
        }

        public T ReadSingle<T>()
        {
            return this.Read<T>().FirstOrDefault();
        }

        public T[] Read<T>()
        {
            return this.Read<T>(true);
        }

        public T[] Read<T>(bool buffered)
        {
            if (!this.HasNext) return new T[0];

            return GetResult(this.reader.Read<T>(buffered));
        }

        public TResult[] Read<T1, T2, TResult>(Func<T1, T2, TResult> convert, string splitOn, bool buffered = true)
        {
            if (convert == null) throw new ArgumentNullException("convert");
            if (string.IsNullOrWhiteSpace(splitOn)) throw new ArgumentException("splitOn is invalid");

            if (!this.HasNext) return new TResult[0];

            return GetResult(this.reader.Read<T1, T2, TResult>(convert, splitOn, buffered));
        }

        public TResult[] Read<T1, T2, T3, TResult>(Func<T1, T2, T3, TResult> convert, string splitOn, bool buffered = true)
        {
            if (convert == null) throw new ArgumentNullException("convert");
            if (string.IsNullOrWhiteSpace(splitOn)) throw new ArgumentException("splitOn is invalid");

            if (!this.HasNext) return new TResult[0];

            return GetResult(this.reader.Read<T1, T2, T3, TResult>(convert, splitOn, buffered));
        }

        public TResult[] Read<T1, T2, T3, T4, TResult>(Func<T1, T2, T3, T4, TResult> convert, string splitOn, bool buffered = true)
        {
            if (convert == null) throw new ArgumentNullException("convert");
            if (string.IsNullOrWhiteSpace(splitOn)) throw new ArgumentException("splitOn is invalid");

            if (!this.HasNext) return new TResult[0];

            return GetResult(this.reader.Read<T1, T2, T3, T4, TResult>(convert, splitOn, buffered));
        }

        public TResult[] Read<T1, T2, T3, T4, T5, TResult>(Func<T1, T2, T3, T4, T5, TResult> convert, string splitOn, bool buffered = true)
        {
            if (convert == null) throw new ArgumentNullException("convert");
            if (string.IsNullOrWhiteSpace(splitOn)) throw new ArgumentException("splitOn is invalid");

            if (!this.HasNext) return new TResult[0];

            return GetResult(this.reader.Read<T1, T2, T3, T4, T5, TResult>(convert, splitOn, buffered));
        }

        public TResult[] Read<T1, T2, T3, T4, T5, T6, TResult>(Func<T1, T2, T3, T4, T5, T6, TResult> convert, string splitOn, bool buffered = true)
        {
            if (convert == null) throw new ArgumentNullException("convert");
            if (string.IsNullOrWhiteSpace(splitOn)) throw new ArgumentException("splitOn is invalid");

            if (!this.HasNext) return new TResult[0];

            return GetResult(this.reader.Read<T1, T2, T3, T4, T5, T6, TResult>(convert, splitOn, buffered));
        }

        public TResult[] Read<T1, T2, T3, T4, T5, T6, T7, TResult>(Func<T1, T2, T3, T4, T5, T6, T7, TResult> convert, string splitOn, bool buffered = true)
        {
            if (convert == null) throw new ArgumentNullException("convert");
            if (string.IsNullOrWhiteSpace(splitOn)) throw new ArgumentException("splitOn is invalid");

            if (!this.HasNext) return new TResult[0];

            return GetResult(this.reader.Read<T1, T2, T3, T4, T5, T6, T7, TResult>(convert, splitOn, buffered));
        }

        private static TResult[] GetResult<TResult>(IEnumerable<TResult> s)
        {
            return s == null ? new TResult[0] : s.ToArray();
        }

        private bool HasNext
        {
            get
            {
                return !this.reader.IsConsumed;
            }
        }

        private SqlMapper.GridReader reader;
    }
}
