using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Utility.Storage.StorageHelper.Common;

namespace Utility.Storage.StorageHelper
{
    /// <summary>
    /// SQL或存储过程所需的参数集合。参数名若未包含'@'会自动补全
    /// </summary>
    public class Parameters
    {
        internal Parameters()
        {

        }


        public Parameters this[string name, bool? value]
        {
            get
            {
                return this.With(name, value);
            }
        }

        public Parameters this[string name, byte? value]
        {
            get
            {
                return this.With(name, value);
            }
        }

        public Parameters this[string name, short? value]
        {
            get
            {
                return this.With(name, value);
            }
        }

        public Parameters this[string name, int? value]
        {
            get
            {
                return this.With(name, value);
            }
        }

        public Parameters this[string name, long? value]
        {
            get
            {
                return this.With(name, value);
            }
        }

        public Parameters this[string name, decimal? value]
        {
            get
            {
                return this.With(name, value);
            }
        }

        public Parameters this[string name, double? value]
        {
            get
            {
                return this.With(name, value);
            }
        }

        public Parameters this[string name, DateTime? value]
        {
            get
            {
                return this.With(name, value);
            }
        }

        public Parameters this[string name, string value, int size, bool isUnicodeString = false, bool isLengthFixed = false]
        {
            get
            {
                return this.String(name, value, size, isUnicodeString, isLengthFixed);
            }
        }

        /// <summary>
        /// 将输入的items转换成xml作为参数传递：转换后的格式为&lt;root&gt;&lt;x s=&#39;{value1}&#39; /&gt;&lt;x s=&#39;{value2}&#39; /&gt;...&lt;/root&gt;. SQL需根据此格式解析.
        /// </summary>
        public Parameters this[string name, IEnumerable<int> items]
        {
            get
            {
                return this.With(name, items);
            }
        }

        /// <summary>
        /// 将输入的items转换成xml作为参数传递：转换后的格式为&lt;root&gt;&lt;x s=&#39;{value1}&#39; /&gt;&lt;x s=&#39;{value2}&#39; /&gt;...&lt;/root&gt;. SQL需根据此格式解析.
        /// </summary>
        public Parameters this[string name, IEnumerable<string> items]
        {
            get
            {
                return this.With(name, items);
            }
        }

        public Parameters this[string name, DataTable table]
        {
            get
            {
                return this.With(name, table, DbType.Object);
            }
        }

        public Parameters TimeNow(string name = "now")
        {
            return this.With(name, DateTime.Now);
        }

        public Parameters Xml(string xml, string name)
        {
            return this.With(name, xml, System.Data.DbType.Xml);
        }

        public Parameters Table(string name, DataTable table)
        {
            return this.With(name, table, DbType.Object);
        }

        /// <summary>
        /// 将输入的items转换成xml作为参数传递：转换后的格式为&lt;root&gt;&lt;x s=&#39;{value1}&#39; /&gt;&lt;x s=&#39;{value2}&#39; /&gt;...&lt;/root&gt;. SQL需根据此格式解析.
        /// </summary>
        public Parameters Array<T>(IEnumerable<T> items, string name, Func<T, string> convert = null)
        {
            if (items == null) return this.Xml(null, name);

            var sb = new StringBuilder("<root>");

            if (convert == null)
            {
                convert = x => x.ToString();
            }

            if (items != null)
            {
                foreach (var item in items)
                {
                    sb.Append("<x s='").Append(convert(item)).Append("' />");
                }
            }

            sb.Append("</root>");

            return this.Xml(sb.ToString(), name);
        }

        public Parameters ID(string id, string name = "id", int size = 36, bool isUnicodeString = false, bool isLengthFixed = false)
        {
            return this.String(name, id, size, isUnicodeString, isLengthFixed);
        }

        /// <summary>
        /// autoWrappedPercent=true时会自动在参数like两边加上%
        /// </summary>
        public Parameters Like(string like, string name, int size = 50, bool autoWrappedPercent = true, bool isUnicodeString = false, bool isLengthFixed = false)
        {
            var s = string.IsNullOrEmpty(like) ? null : (autoWrappedPercent ? string.Concat("%", like, "%") : like);

            return this.String(name, s, size, isUnicodeString, isLengthFixed);
        }

        public Parameters With(string name, object value, DbType type)
        {
            return this.With(name, value, type, -1);
        }

        public Parameters With(string name, object value, DbType type, int size)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("name is not valid");
            if (name[0] != ParameterNamePrefix) name = ParameterNamePrefix + name;

            if (size < 0)
            {
                this.parameters.Add(name, value, type);
            }
            else
            {
                this.parameters.Add(name, value, type, size: size);
            }

            return this;
        }

        public Parameters With(string name, byte? value)
        {
            return this.With(name, value, System.Data.DbType.Int16);
        }

        public Parameters With(string name, short? value)
        {
            return this.With(name, value, DbType.Int16);
        }

        public Parameters With(string name, int value)
        {
            return this.With(name, value, System.Data.DbType.Int32);
        }

        public Parameters With(string name, int? value)
        {
            return this.With(name, value, System.Data.DbType.Int32);
        }

        public Parameters With(string name, long? value)
        {
            return this.With(name, value, DbType.Int64);
        }

        public Parameters With(string name, bool? value)
        {
            return this.With(name, value, System.Data.DbType.Boolean);
        }

        public Parameters With(string name, decimal? value)
        {
            return this.With(name, value, System.Data.DbType.Decimal);
        }

        public Parameters With(string name, double? value)
        {
            return this.With(name, value, System.Data.DbType.Double);
        }

        public Parameters With(string name, DateTime? value)
        {
            if (value.HasValue)
            {
                return this.With(name, value.Value < DateTime_19000101 ? DateTime_19000101 : value.Value, System.Data.DbType.DateTime);
            }
            else
            {
                return this.With(name, null, System.Data.DbType.DateTime);
            }
        }

        public Parameters With(string name, IEnumerable items)
        {
            if (items == null) return this.Xml(null, name);

            var sb = new StringBuilder("<root>");

            int n = 0;
            if (items != null)
            {
                foreach (var item in items)
                {
                    sb.Append("<x s='").Append(item == null ? string.Empty : item.ToString()).Append("' />");
                    n++;
                }
            }

            sb.Append("</root>");

            return this.Xml(n > 0 ? sb.ToString() : null, name);
        }

        public Parameters String(string name, string value, int size, bool isUnicodeString = false, bool isLengthFixed = false)
        {
            return this.With(name, value, isUnicodeString ? (isLengthFixed ? DbType.StringFixedLength : DbType.String) : (isLengthFixed ? DbType.AnsiStringFixedLength : DbType.AnsiString), size: size);
        }

        internal DynamicParameters Result
        {
            get { return this.parameters; }
        }

        private DynamicParameters parameters = new DynamicParameters();

        private static readonly DateTime DateTime_19000101 = new DateTime(1900, 1, 1);

        private char ParameterNamePrefix = '@';
    }
}
