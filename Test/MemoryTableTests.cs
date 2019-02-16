using Cave;
using Cave.Collections.Generic;
using Cave.Data;
using Cave.IO;
using NUnit.Framework;
using System;
using System.Linq;
using Test.Cave;

namespace Test.Cave.Data
{
    [TestFixture]
    public class MemoryTableTests
    {
        [Test]
        public void OrderByWithLimit()
        {
            MemoryTable<SmallTestStruct> test = new MemoryTable<SmallTestStruct>();
            
            Set<int> collisionCheck = new Set<int>();
            for (int i = 0; i < 100; i++)
            {
                var content = string.Empty;
                while (content.Length == 0)
                {
                    content = DefaultRNG.GetPassword(DefaultRNG.UInt8 % 16, ASCII.Strings.Letters);
                }
                int integer = content.GetHashCode();
                while (collisionCheck.Contains(integer)) integer++;
                collisionCheck.Add(integer);
                test.Insert(new SmallTestStruct()
                {
                    Integer = integer,
                    Content = content,
                    DateTime = DateTime.UtcNow + new TimeSpan(integer * TimeSpan.TicksPerSecond),
                });
            }

            var array = test.ToArray();

            CollectionAssert.AreEqual(array.OrderBy(a => a.Integer), test.GetStructs(Search.None, ResultOption.SortAscending(nameof(SmallTestStruct.Integer))));
            CollectionAssert.AreEqual(array.OrderBy(a => a.DateTime), test.GetStructs(Search.None, ResultOption.SortAscending(nameof(SmallTestStruct.DateTime))));
            CollectionAssert.AreEqual(array.OrderByDescending(a => a.Integer), test.GetStructs(Search.None, ResultOption.SortDescending(nameof(SmallTestStruct.Integer))));
            CollectionAssert.AreEqual(array.OrderByDescending(a => a.DateTime), test.GetStructs(Search.None, ResultOption.SortDescending(nameof(SmallTestStruct.DateTime))));

            CollectionAssert.AreEqual(
                array.OrderBy(a => a.Integer).SubRange(0, 3), 
                test.GetStructs(Search.None, ResultOption.SortAscending(nameof(SmallTestStruct.Integer)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderByDescending(a => a.Integer).SubRange(0, 3),
                test.GetStructs(Search.None, ResultOption.SortDescending(nameof(SmallTestStruct.Integer)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderBy(a => a.DateTime).SubRange(0, 3),
                test.GetStructs(Search.None, ResultOption.SortAscending(nameof(SmallTestStruct.DateTime)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderByDescending(a => a.DateTime).SubRange(0, 3),
                test.GetStructs(Search.None, ResultOption.SortDescending(nameof(SmallTestStruct.DateTime)) + ResultOption.Limit(3)));
        }

        [Test]
        public void OrderByWithLimitWithIndex()
        {
            MemoryTable<TestStructClean> test = new MemoryTable<TestStructClean>();

            Set<int> collisionCheck = new Set<int>();
            for (int i = 0; i < 1000; i++)
            {
                var content = string.Empty;
                while (content.Length == 0)
                {
                    content = DefaultRNG.GetPassword(DefaultRNG.UInt8 % 16, ASCII.Strings.Letters);
                }
                int integer = content.GetHashCode();
                while (collisionCheck.Contains(integer)) integer++;
                collisionCheck.Add(integer);
                test.Replace(new TestStructClean()
                {
                    ID = 1 + i % 100,
                    I = integer,
                    Text = content,
                    Date = DateTime.UtcNow + new TimeSpan(integer * TimeSpan.TicksPerSecond),
                });
            }
            test.Delete(1);

            var array = test.ToArray();

            CollectionAssert.AreEqual(array.OrderBy(a => a.I), test.GetStructs(Search.None, ResultOption.SortAscending(nameof(TestStructClean.I))));
            CollectionAssert.AreEqual(array.OrderBy(a => a.Date), test.GetStructs(Search.None, ResultOption.SortAscending(nameof(TestStructClean.Date))));
            CollectionAssert.AreEqual(array.OrderByDescending(a => a.I), test.GetStructs(Search.None, ResultOption.SortDescending(nameof(TestStructClean.I))));
            CollectionAssert.AreEqual(array.OrderByDescending(a => a.Date), test.GetStructs(Search.None, ResultOption.SortDescending(nameof(TestStructClean.Date))));

            CollectionAssert.AreEqual(
                array.OrderBy(a => a.I).SubRange(0, 3),
                test.GetStructs(Search.None, ResultOption.SortAscending(nameof(TestStructClean.I)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderByDescending(a => a.I).SubRange(0, 3),
                test.GetStructs(Search.None, ResultOption.SortDescending(nameof(TestStructClean.I)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderBy(a => a.Date).SubRange(0, 3),
                test.GetStructs(Search.None, ResultOption.SortAscending(nameof(TestStructClean.Date)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderByDescending(a => a.Date).SubRange(0, 3),
                test.GetStructs(Search.None, ResultOption.SortDescending(nameof(TestStructClean.Date)) + ResultOption.Limit(3)));
        }

        [Test]
        public void Default()
        {
            MemoryTable<SmallTestStruct> test = new MemoryTable<SmallTestStruct>();
            for (int i = 0; i < 1000; i++)
            {
                //test.Insert(new LogEntry() { Content = "", DateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified).AddHours(i), HostName = "host" + (i%10), Level = LogLevel.Debug, ProcessName = "this", Source = "this", });
                test.Insert(new SmallTestStruct() { Content = "", DateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified).AddHours(i), Name = "host" + (i%10), Level = TestEnum.A,  Source = "this", });
            }
            Assert.AreEqual(1000, test.RowCount);
            Assert.AreEqual(1, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Content))));
            Assert.AreEqual(1000, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.DateTime))));
            Assert.AreEqual(10, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Name))));
            Assert.AreEqual(10, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Content)) + ResultOption.Group(nameof(SmallTestStruct.Name))));

            var rows = test.GetStructs(Search.None, ResultOption.Group(nameof(SmallTestStruct.Name)) + ResultOption.SortDescending(nameof(SmallTestStruct.Name)));
            Assert.AreEqual(10, rows.Count);
            for (int i = 0; i < 10; i++) Assert.AreEqual("host" + (9 - i), rows[i].Name);

            rows = test.GetStructs(
                Search.FieldGreater(nameof(SmallTestStruct.DateTime), new DateTime(1970, 1, 1, 5, 0, 0, DateTimeKind.Unspecified)) &
                Search.FieldSmallerOrEqual(nameof(SmallTestStruct.DateTime), new DateTime(1970, 1, 1, 10, 0, 0, DateTimeKind.Unspecified)),
                ResultOption.SortDescending(nameof(SmallTestStruct.DateTime)));
            var rowsExpected = test.GetStructs().
                Where(i => i.DateTime > new DateTime(1970, 1, 1, 5, 0, 0, DateTimeKind.Unspecified) && 
                    i.DateTime <= new DateTime(1970, 1, 1, 10, 0, 0, DateTimeKind.Unspecified)).
                OrderBy(i => -i.DateTime.Ticks);
            CollectionAssert.AreEqual(rowsExpected, rows);

            rows = test.GetStructs(
                Search.FieldGreaterOrEqual(nameof(SmallTestStruct.DateTime), new DateTime(1970, 1, 1, 5, 0, 0, DateTimeKind.Unspecified)) &
                Search.FieldSmaller(nameof(SmallTestStruct.DateTime), new DateTime(1970, 1, 1, 10, 0, 0, DateTimeKind.Unspecified)),
                ResultOption.SortAscending(nameof(SmallTestStruct.DateTime)));
            rowsExpected = test.GetStructs().
                Where(i => i.DateTime >= new DateTime(1970, 1, 1, 5, 0, 0, DateTimeKind.Unspecified) &&
                    i.DateTime < new DateTime(1970, 1, 1, 10, 0, 0, DateTimeKind.Unspecified)).
                OrderBy(i => i.DateTime);
            CollectionAssert.AreEqual(rowsExpected, rows);

            for (int i = 0; i < 1000; i++)
            {
                SmallTestStruct e = new SmallTestStruct() { ID = i + 1, Content = "Updated" + i.ToString(), DateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified).AddHours(i % 100), Name = "this", Level = TestEnum.B,  Source = "this", };
                test.Update(e);
                Assert.AreEqual(e, test.GetStruct(i + 1));
            }
            Assert.AreEqual(100, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.DateTime))));
            Assert.AreEqual(1000, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Content))));
            Assert.AreEqual(1000, test.RowCount);

            for (int i = 0; i < 1000; i++)
            {
                SmallTestStruct e = new SmallTestStruct() { ID = i + 1, Content = "Replaced", DateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified).AddHours(i), Name = "this", Level = TestEnum.B,  Source = "this", };
                test.Update(e);
                Assert.AreEqual(e, test.GetStruct(i + 1));
            }
            Assert.AreEqual(1000, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.DateTime))));
            Assert.AreEqual(1, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Content))));
            Assert.AreEqual(1000, test.RowCount);
        }
    }
}
