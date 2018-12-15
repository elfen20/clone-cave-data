using Cave.Data;
using NUnit.Framework;
using System;
using System.Linq;
using Test.Cave;

namespace Test.Cave.Data
{
    [TestFixture]
    public class Tests_MemoryTable
    {
        [Test]
        public void Tests_MemoryTable_1()
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
