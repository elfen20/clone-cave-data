using Cave;
using Cave.Data;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    [TestFixture]
    public class TimeSpanFieldTest
    {
        [Table]
        struct TestStruct : IEquatable<TestStruct>
        {
            [Field(Flags = FieldFlags.ID)]
            public long ID;

            [Field]
            public TimeSpan T1;

            [Field]
            [TimeSpanFormat(DateTimeType.BigIntHumanReadable)]
            public TimeSpan T2;

            [Field]
            [TimeSpanFormat(DateTimeType.BigIntTicks)]
            public TimeSpan T3;

            [Field]
            [TimeSpanFormat(DateTimeType.DecimalSeconds)]
            public TimeSpan T4;

            [Field]
            [TimeSpanFormat(DateTimeType.DoubleSeconds)]
            public TimeSpan T5;

            [Field]
            [TimeSpanFormat(DateTimeType.Native)]
            public TimeSpan T6;

            [Field]
            [TimeSpanFormat(DateTimeType.Undefined)]
            public TimeSpan T7;

            public bool Equals(TestStruct other)
            {
                var test_T2 = Math.Abs(T2.TotalSeconds - other.T2.TotalSeconds) < 1;
                var test_T5 = Math.Abs(T5.TotalSeconds - other.T5.TotalSeconds) < 1;

                return T1 == other.T1
                    && test_T2
                    && T3 == other.T3
                    && T4 == other.T4
                    && test_T5
                    && T6 == other.T6
                    && T7 == other.T7;
            }

            public override bool Equals(object obj)
            {
                if (obj is TestStruct test)
                {
                    return Equals(test);
                }
                return false;
            }

            public void Init(TimeSpan t)
            {
                T1 = T2 = T3 = T4 = T5 = T6 = T7 = t;
                T2 = new TimeSpan(Math.Min(DateTime.MaxValue.Ticks, Math.Max(DateTime.MinValue.Ticks, T2.Ticks)));
            }
        }

        [Test]
        public void TestDAT()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStruct));

            TestStruct t1 = new TestStruct();
            t1.Init(TimeSpan.MinValue);
            TestStruct t2 = new TestStruct();
            t2.Init(TimeSpan.MaxValue);
            TestStruct t3 = new TestStruct();
            t3.Init(DateTime.Now.TimeOfDay);

            var ms = new MemoryStream();
            DatWriter w = new DatWriter(layout, ms);
            w.Write(t1);
            w.Write(t2);
            w.Write(t3);
            w.Close();
            var ms2 = new MemoryStream(ms.ToArray());
            DatReader r = new DatReader(ms2);
            var list = r.ReadList<TestStruct>();
            Assert.AreEqual(3, list.Count);
            Assert.AreEqual(t1, list[0]);
            Assert.AreEqual(t2, list[1]);
            Assert.AreEqual(t3, list[2]);
        }

        [Test]
        public void TestCSV()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStruct));

            TestStruct t1 = new TestStruct();
            t1.Init(TimeSpan.MinValue);
            TestStruct t2 = new TestStruct();
            t2.Init(TimeSpan.MaxValue);
            TestStruct t3 = new TestStruct();
            t3.Init(DateTime.Now.TimeOfDay);

            var ms = new MemoryStream();
            CsvWriter w = new CsvWriter(layout, ms);
            w.Write(t1);
            w.Write(t2);
            w.Write(t3);
            w.Close();
            var ms2 = new MemoryStream(ms.ToArray());
            CsvReader r = new CsvReader(layout, ms2);
            var list = r.ReadList<TestStruct>();
            Assert.AreEqual(3, list.Count);
            Assert.AreEqual(t1, list[0]);
            Assert.AreEqual(t2, list[1]);
            Assert.AreEqual(t3, list[2]);
        }
    }
}
