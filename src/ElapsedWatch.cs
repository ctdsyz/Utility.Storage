using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Utility.Storage.StorageHelper
{
    /// <summary>
    /// 计时器
    /// </summary>
    class ElapsedWatch : IDisposable
    {
        /// <summary>
        /// 初始化并立即启动
        /// </summary>
        /// <param name="description">统计的描述信息</param>
        public ElapsedWatch(string description = null)
            : this(true, description)
        {
        }

        /// <param name="startNow">立即启动</param>
        /// <param name="description">统计的描述信息</param>
        public ElapsedWatch(bool startNow = false, string description = null)
        {
            this.steps = new List<KeyValuePair<string, TimeSpan>>();

            this.watch = new Stopwatch();

            if (startNow) this.Start(description);
        }

        /// <summary>
        /// 开始计时
        /// </summary>
        /// <param name="description">统计的描述信息</param>
        public ElapsedWatch Start(string description = null)
        {
            if (this.watch.IsRunning) return this;

            this.stepDescription = description;
            this.watch.Restart();

            return this;
        }

        /// <summary>
        /// 停止计时
        /// </summary>
        public ElapsedWatch Stop()
        {
            if (!this.watch.IsRunning) return this;

            this.watch.Stop();

            this.steps.Add(new KeyValuePair<string, TimeSpan>(this.stepDescription, this.watch.Elapsed));
            this.total += this.watch.Elapsed;

            return this;
        }

        /// <summary>
        /// 停止上次计时、保存计时信息、开启新的计时
        /// </summary>
        /// <param name="description">统计的描述信息</param>
        public ElapsedWatch Next(string description = null)
        {
            return this.Stop().Start(description);
        }

        /// <summary>
        /// 停止计时，记录统计信息
        /// </summary>
        public void Dispose()
        {
            this.Stop();

            this.WriteResult();
        }

        public ElapsedInfo ElapsedInfo
        {
            get
            {
                return new ElapsedInfo
                {
                    Total = this.total,
                    Steps = this.steps.ToArray()
                };
            }
        }

        /// <summary>
        /// 最近一次统计的时间
        /// </summary>
        public static ElapsedInfo LastElapsed
        {
            get
            {
                return lastElapsed;
            }
        }

        private void WriteResult()
        {
            lastElapsed = this.ElapsedInfo;
        }

        private Stopwatch watch;

        private TimeSpan total;

        private List<KeyValuePair<string, TimeSpan>> steps;

        private string stepDescription;

        [ThreadStatic]
        private static ElapsedInfo lastElapsed;
    }

    public class ElapsedInfo
    {
        /// <summary>
        /// 总时间
        /// </summary>
        public TimeSpan Total { get; internal set; }

        /// <summary>
        /// 每一步的时间
        /// </summary>
        public KeyValuePair<string, TimeSpan>[] Steps { get; internal set; }
    }
}
