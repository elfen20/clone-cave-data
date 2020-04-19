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
            var test = new MemoryStorage().CreateDatabase("db").CreateTable<SmallTestStruct>();
            var typed = new Table<long, SmallTestStruct>(test);

            var collisionCheck = new Set<int>();
            for (var i = 0; i < 100; i++)
            {
                var content = string.Empty;
                while (content.Length == 0)
                {
                    content = DefaultRNG.GetPassword(DefaultRNG.UInt8 % 16, ASCII.Strings.Letters);
                }
                var integer = content.GetHashCode();
                while (collisionCheck.Contains(integer)) integer++;
                collisionCheck.Add(integer);
                typed.Insert(new SmallTestStruct()
                {
                    Integer = integer,
                    Content = content,
                    DateTime = DateTime.UtcNow + new TimeSpan(integer * TimeSpan.TicksPerSecond),
                });
            }

            var array = typed.GetStructs();

            CollectionAssert.AreEqual(array.OrderBy(a => a.Integer).ToList(), typed.GetStructs(Search.None, ResultOption.SortAscending(nameof(SmallTestStruct.Integer))));
            CollectionAssert.AreEqual(array.OrderBy(a => a.DateTime).ToList(), typed.GetStructs(Search.None, ResultOption.SortAscending(nameof(SmallTestStruct.DateTime))));
            CollectionAssert.AreEqual(array.OrderByDescending(a => a.Integer).ToList(), typed.GetStructs(Search.None, ResultOption.SortDescending(nameof(SmallTestStruct.Integer))));
            CollectionAssert.AreEqual(array.OrderByDescending(a => a.DateTime).ToList(), typed.GetStructs(Search.None, ResultOption.SortDescending(nameof(SmallTestStruct.DateTime))));

            CollectionAssert.AreEqual(
                array.OrderBy(a => a.Integer).SubRange(0, 3),
                typed.GetStructs(Search.None, ResultOption.SortAscending(nameof(SmallTestStruct.Integer)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderByDescending(a => a.Integer).SubRange(0, 3),
                typed.GetStructs(Search.None, ResultOption.SortDescending(nameof(SmallTestStruct.Integer)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderBy(a => a.DateTime).SubRange(0, 3),
                typed.GetStructs(Search.None, ResultOption.SortAscending(nameof(SmallTestStruct.DateTime)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderByDescending(a => a.DateTime).SubRange(0, 3),
                typed.GetStructs(Search.None, ResultOption.SortDescending(nameof(SmallTestStruct.DateTime)) + ResultOption.Limit(3)));
        }

        [Test]
        public void OrderByWithLimitWithIndex()
        {
            var test = new MemoryStorage().CreateDatabase("db").CreateTable<TestStructClean>(); 
            var typed = new Table<long, TestStructClean>(test);

            var collisionCheck = new Set<int>();
            for (var i = 0; i < 1000; i++)
            {
                var content = string.Empty;
                while (content.Length == 0)
                {
                    content = DefaultRNG.GetPassword(DefaultRNG.UInt8 % 16, ASCII.Strings.Letters);
                }
                var integer = content.GetHashCode();
                while (collisionCheck.Contains(integer)) integer++;
                collisionCheck.Add(integer);
                typed.Replace(new TestStructClean()
                {
                    ID = 1 + i % 100,
                    I = integer,
                    Text = content,
                    Date = DateTime.UtcNow + new TimeSpan(integer * TimeSpan.TicksPerSecond),
                });
            }
            typed.Delete(1);

            var array = typed.GetStructs();

            CollectionAssert.AreEqual(array.OrderBy(a => a.I), typed.GetStructs(Search.None, ResultOption.SortAscending(nameof(TestStructClean.I))));
            CollectionAssert.AreEqual(array.OrderBy(a => a.Date), typed.GetStructs(Search.None, ResultOption.SortAscending(nameof(TestStructClean.Date))));
            CollectionAssert.AreEqual(array.OrderByDescending(a => a.I), typed.GetStructs(Search.None, ResultOption.SortDescending(nameof(TestStructClean.I))));
            CollectionAssert.AreEqual(array.OrderByDescending(a => a.Date), typed.GetStructs(Search.None, ResultOption.SortDescending(nameof(TestStructClean.Date))));

            CollectionAssert.AreEqual(
                array.OrderBy(a => a.I).SubRange(0, 3),
                typed.GetStructs(Search.None, ResultOption.SortAscending(nameof(TestStructClean.I)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderByDescending(a => a.I).SubRange(0, 3),
                typed.GetStructs(Search.None, ResultOption.SortDescending(nameof(TestStructClean.I)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderBy(a => a.Date).SubRange(0, 3),
                typed.GetStructs(Search.None, ResultOption.SortAscending(nameof(TestStructClean.Date)) + ResultOption.Limit(3)));

            CollectionAssert.AreEqual(
                array.OrderByDescending(a => a.Date).SubRange(0, 3),
                typed.GetStructs(Search.None, ResultOption.SortDescending(nameof(TestStructClean.Date)) + ResultOption.Limit(3)));
        }

        [Test]
        public void Default()
        {
            var test = new MemoryStorage().CreateDatabase("db").CreateTable<SmallTestStruct>();
            var typed = new Table<long, SmallTestStruct>(test);
            for (var i = 0; i < 1000; i++)
            {
                typed.Insert(new SmallTestStruct() { Content = "", DateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local).AddHours(i), Name = "host" + (i % 10), Level = TestEnum.A, Source = "this", });
            }
            Assert.AreEqual(1000, test.RowCount);
            Assert.AreEqual(1, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Content))));
            Assert.AreEqual(1000, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.DateTime))));
            Assert.AreEqual(10, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Name))));

            Assert.AreEqual(10, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Content)) + ResultOption.Group(nameof(SmallTestStruct.Name))));

            var rows = typed.GetStructs(Search.None, ResultOption.Group(nameof(SmallTestStruct.Name)) + ResultOption.SortDescending(nameof(SmallTestStruct.Name)));
            Assert.AreEqual(10, rows.Count);
            for (var i = 0; i < 10; i++) Assert.AreEqual("host" + (9 - i), rows[i].Name);

            rows = typed.GetStructs(
                Search.FieldGreater(nameof(SmallTestStruct.DateTime), new DateTime(1970, 1, 1, 5, 0, 0, DateTimeKind.Local)) &
                Search.FieldSmallerOrEqual(nameof(SmallTestStruct.DateTime), new DateTime(1970, 1, 1, 10, 0, 0, DateTimeKind.Local)),
                ResultOption.SortDescending(nameof(SmallTestStruct.DateTime)));
            var rowsExpected = typed.GetStructs().
                Where(i => i.DateTime > new DateTime(1970, 1, 1, 5, 0, 0, DateTimeKind.Local) &&
                    i.DateTime <= new DateTime(1970, 1, 1, 10, 0, 0, DateTimeKind.Local)).
                OrderBy(i => -i.DateTime.Ticks).ToList();
            CollectionAssert.AreEqual(rowsExpected, rows);

            rows = typed.GetStructs(
                Search.FieldGreaterOrEqual(nameof(SmallTestStruct.DateTime), new DateTime(1970, 1, 1, 5, 0, 0, DateTimeKind.Local)) &
                Search.FieldSmaller(nameof(SmallTestStruct.DateTime), new DateTime(1970, 1, 1, 10, 0, 0, DateTimeKind.Local)),
                ResultOption.SortAscending(nameof(SmallTestStruct.DateTime)));
            rowsExpected = typed.GetStructs().
                Where(i => i.DateTime >= new DateTime(1970, 1, 1, 5, 0, 0, DateTimeKind.Local) &&
                    i.DateTime < new DateTime(1970, 1, 1, 10, 0, 0, DateTimeKind.Local)).
                OrderBy(i => i.DateTime).ToList();
            CollectionAssert.AreEqual(rowsExpected, rows);

            for (var i = 0; i < 1000; i++)
            {
                var e = new SmallTestStruct() { ID = i + 1, Content = "Updated" + i.ToString(), DateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local).AddHours(i % 100), Name = "this", Level = TestEnum.B, Source = "this", };
                typed.Update(e);
                Assert.AreEqual(e, typed.GetStruct(i + 1));
            }
            Assert.AreEqual(100, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.DateTime))));
            Assert.AreEqual(1000, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Content))));
            Assert.AreEqual(1000, test.RowCount);

            for (var i = 0; i < 1000; i++)
            {
                var e = new SmallTestStruct() { ID = i + 1, Content = "Replaced", DateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local).AddHours(i), Name = "this", Level = TestEnum.B, Source = "this", };
                typed.Update(e);
                Assert.AreEqual(e, typed.GetStruct(i + 1));
            }
            Assert.AreEqual(1000, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.DateTime))));
            Assert.AreEqual(1, test.Count(Search.None, ResultOption.Group(nameof(SmallTestStruct.Content))));
            Assert.AreEqual(1000, test.RowCount);
        }
    }
}
