using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using Cave.Data;
using Cave;

namespace Test.Cave.Data
{
    [TestFixture]
    public class SyncMemTableTests
    {
        [Test]
        public void Default()
        {
            {
                // Create SynchronizeMemoryTable from Layout
                var layoutA = RowLayout.CreateTyped(typeof(TestStructClean));
                var l_Syn = new SynchronizedMemoryTable(layoutA);
                RowLayout.CheckLayout(l_Syn.Layout, layoutA);
                Assert.AreEqual(l_Syn.Name, "TestStructClean");
                Assert.AreNotEqual(l_Syn.Storage, null);
                Assert.AreNotEqual(l_Syn.Database, null);
            }
            {
                // Create SynchronizeMemoryTable from MemoryTable.
                var l_Memory = new MemoryTable<TestStructClean>();
                var l_Syn = new SynchronizedMemoryTable(l_Memory);
                RowLayout.CheckLayout(l_Syn.Layout, l_Memory.Layout);
            }
            {
                // Create SynchronizeMemoryTable from ITable
                ITable l_Memory = new MemoryTable<TestStructClean>();
                var l_Syn = new SynchronizedMemoryTable(l_Memory);
                RowLayout.CheckLayout(l_Syn.Layout, l_Memory.Layout);
            }
            // SynchronizeMemoryTable<T>
            // Create SynchronizeMemoryTable<T> from ITable<T>.
            {
                var l_Memory = new MemoryTable<TestStructClean>();
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>(l_Memory);
                RowLayout.CheckLayout(l_Syn.Layout, l_Memory.Layout);
            }
            // Create SynchronizeMemoryTable<T> from Memory<T>.
            {
                var l_Memory = new MemoryTable<TestStructClean>();
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>(l_Memory);
                RowLayout.CheckLayout(l_Syn.Layout, l_Memory.Layout);
            }

            // Convert SynchronizeMemoryTable<T> to untype
            {
                var l_Memory = new MemoryTable<TestStructClean>();
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>(l_Memory);
                var l_SynExpect = (SynchronizedMemoryTable)l_Syn;
                RowLayout.CheckLayout(l_Syn.Layout, l_SynExpect.Layout);
            }
        }

        [Test]
        public void LoadTable()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            var l_Memory = new MemoryTable(layout);

            var t1 = TestStructClean.Create(1);
            t1.ID = 1;
            var row1 = Row.Create(layout, t1);
            l_Memory.Insert(row1);

            var t2 = TestStructClean.Create(2);
            t2.ID = 2;
            var row2 = Row.Create(layout, t2);
            l_Memory.Insert(row2);

            var t3 = TestStructClean.Create(3);
            t3.ID = 3;
            var row3 = Row.Create(layout, t3);
            l_Memory.Insert(row3);
            // SynchronizedMemoryTable
            {
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                l_Syn.LoadTable(l_Memory);
                var rows = l_Syn.GetRows();
                CollectionAssert.Contains(rows, row1);
                CollectionAssert.Contains(rows, row2);
                CollectionAssert.Contains(rows, row3);
            }
            // SynchronizedMemoryTable<T>
            {
                {
                    var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                    l_Syn.LoadTable(l_Memory);
                    var rows = l_Syn.GetRows();
                    CollectionAssert.Contains(rows, row1);
                    CollectionAssert.Contains(rows, row2);
                    CollectionAssert.Contains(rows, row3);
                }
            }
        }

        [Test]
        public void Count()
        {
            // SynchronizedMemoryTable
            {
                var layout = RowLayout.CreateTyped(typeof(TestStructClean));
                var l_Memory = new MemoryTable(layout);

                var t1 = TestStructClean.Create(1);
                t1.ID = 1;
                var row1 = Row.Create(layout, t1);
                l_Memory.Insert(row1);

                var t2 = TestStructClean.Create(2);
                t2.ID = 2;
                var row2 = Row.Create(layout, t2);
                l_Memory.Insert(row2);

                var t3 = TestStructClean.Create(3);
                t3.ID = 3;
                var row3 = Row.Create(layout, t3);
                l_Memory.Insert(row3);

                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                l_Syn.LoadTable(l_Memory);

                Assert.AreEqual(l_Syn.Count(Search.None), 3);
                Assert.AreEqual(l_Syn.RowCount, 3);
                Assert.AreEqual(l_Syn.Count(Search.FieldEquals("ID", 1L)), 1);
                Assert.AreEqual(l_Syn.Count(Search.FieldEquals("ID", 2L)), 1);
                Assert.AreEqual(l_Syn.Count(Search.FieldEquals("ID", 3L)), 1);
                Assert.AreEqual(l_Syn.Count(Search.FieldEquals("ID", 4L)), 0);
            }
            // SynchronizedMemoryTable<T>
            {
                var l_Memory = new MemoryTable<TestStructClean>();

                var t1 = TestStructClean.Create(1);
                t1.ID = 1;
                l_Memory.Insert(t1);

                var t2 = TestStructClean.Create(2);
                t2.ID = 2;
                l_Memory.Insert(t2);

                var t3 = TestStructClean.Create(3);
                t3.ID = 3;
                l_Memory.Insert(t3);

                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                l_Syn.LoadTable(l_Memory);

                Assert.AreEqual(l_Syn.Count(Search.None), 3);
                Assert.AreEqual(l_Syn.RowCount, 3);
                Assert.AreEqual(l_Syn.Count(Search.FieldEquals("ID", 1L)), 1);
                Assert.AreEqual(l_Syn.Count(Search.FieldEquals("ID", 2L)), 1);
                Assert.AreEqual(l_Syn.Count(Search.FieldEquals("ID", 3L)), 1);
                Assert.AreEqual(l_Syn.Count(Search.FieldEquals("ID", 4L)), 0);
            }
        }

        [Test]
        public void Clear()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            var l_Memory = new MemoryTable(layout);

            var t1 = TestStructClean.Create(1);
            t1.ID = 1;
            var row1 = Row.Create(layout, t1);
            l_Memory.Insert(row1);

            var t2 = TestStructClean.Create(2);
            t2.ID = 2;
            var row2 = Row.Create(layout, t2);
            l_Memory.Insert(row2);

            var t3 = TestStructClean.Create(3);
            t3.ID = 3;
            var row3 = Row.Create(layout, t3);
            l_Memory.Insert(row3);

            var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
            l_Syn.LoadTable(l_Memory);

            Assert.AreEqual(l_Syn.RowCount, l_Memory.RowCount);
            Assert.AreEqual(l_Syn.Count(Search.None), 3);
            l_Syn.Clear();
            Assert.AreEqual(l_Syn.Count(Search.None), 0);
        }

        [Test]
        public void GetRow()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            // SynchronizedMemoryTable
            {
                var l_Memory = new MemoryTable(layout);
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Syn.Insert(row);
                }
                // get row by id
                {
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1L);
                    Assert.AreEqual(l_Syn.GetRow(1).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                    row = Row.Create(layout, TestStructClean.Create(2));
                    row = row.SetID(layout.IDFieldIndex, 2L);
                    Assert.AreEqual(l_Syn.GetRow(2).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                    row = Row.Create(layout, TestStructClean.Create(3));
                    row = row.SetID(layout.IDFieldIndex, 3L);
                    Assert.AreEqual(l_Syn.GetRow(3).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                    try
                    {
                        l_Syn.GetRow(0);
                    }
                    catch (Exception ex)
                    {
                        Assert.IsInstanceOf<KeyNotFoundException>(ex);
                    }
                }
                // get row
                {
                    try
                    {
                        Assert.AreEqual(l_Syn.GetRow("ID", 0L), -1);
                    }
                    catch (Exception ex)
                    {
                        Assert.IsInstanceOf<Exception>(ex);
                    }
                }
                {
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1L);

                    Assert.AreEqual(l_Syn.GetRow("ID", 1L).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                    Assert.AreEqual(l_Syn.GetRow("UI", (uint)1).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(2));
                    row = row.SetID(layout.IDFieldIndex, 2);
                    Assert.AreEqual(l_Syn.GetRow("ID", 2L).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                }
                //Search
                {
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1L);

                    Assert.AreEqual(l_Syn.GetRow(Search.FieldEquals("ID", 1L)).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                }
                // Search + ResultOption
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Syn.Insert(row);
                }
                {
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1L);
                    Assert.AreEqual(l_Syn.GetRow(Search.FieldGreater("ID", 0L), ResultOption.Limit(1)).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(10));
                    row = row.SetID(layout.IDFieldIndex, 20);
                    Row rowExpect = l_Syn.GetRow(Search.FieldGreater("ID", 0L), ResultOption.Limit(1) + ResultOption.SortDescending("ID"));
                    Assert.AreEqual(rowExpect.GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    try
                    {
                        l_Syn.GetRow(Search.FieldGreater("ID", 0L));
                    }
                    catch (Exception ex)
                    {
                        Assert.IsInstanceOf<Exception>(ex);
                    }
                }
                // get rows
                {
                    var ids = l_Syn.GetRows("I", 1);
                    Assert.AreEqual(ids.Count, 2);
                    //CollectionAssert.AreEqual(ids, new long[] { 1, 11 });
                }
                {
                    var ids = l_Syn.GetRows(Search.FieldGreater("ID", 0L));
                    Assert.AreEqual(ids.Count, 20);

                    ids = l_Syn.GetRows(Search.FieldGreater("ID", 0L) & Search.FieldSmaller("ID", 11L));
                    Assert.AreEqual(ids.Count, 10);

                    ids = l_Syn.GetRows(Search.FieldGreater("ID", 0L) & Search.FieldSmaller("ID", 11L), ResultOption.Limit(5));
                    Assert.AreEqual(ids.Count, 5);

                    ids = l_Syn.GetRows(Search.FieldGreater("ID", 0L), ResultOption.SortDescending("ID"));
                    Assert.AreEqual(ids.Count, 20);

                    var row = Row.Create(layout, TestStructClean.Create(10));
                    row = row.SetID(layout.IDFieldIndex, 20);
                    Assert.AreEqual(ids[0].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                }
                {
                    var rows = l_Syn.GetRows(new long[] { 1, 2, 3, 4, 5 });
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1);
                    Assert.AreEqual(rows[0].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(2));
                    row = row.SetID(layout.IDFieldIndex, 2);
                    Assert.AreEqual(rows[1].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(3));
                    row = row.SetID(layout.IDFieldIndex, 3);
                    Assert.AreEqual(rows[2].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(4));
                    row = row.SetID(layout.IDFieldIndex, 4);
                    Assert.AreEqual(rows[3].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                }
            }
            // SynchronizedMemoryTable<T>
            {
                var l_Memory = new MemoryTable(layout);
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Syn.Insert(row);
                }
                // get row by id
                {
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1L);
                    Assert.AreEqual(l_Syn.GetRow(1).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                    row = Row.Create(layout, TestStructClean.Create(2));
                    row = row.SetID(layout.IDFieldIndex, 2L);
                    Assert.AreEqual(l_Syn.GetRow(2).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                    row = Row.Create(layout, TestStructClean.Create(3));
                    row = row.SetID(layout.IDFieldIndex, 3L);
                    Assert.AreEqual(l_Syn.GetRow(3).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                    try
                    {
                        l_Syn.GetRow(0);
                    }
                    catch (Exception ex)
                    {
                        Assert.IsInstanceOf<Exception>(ex);
                    }
                }
                // get row
                {
                    try
                    {
                        Assert.AreEqual(l_Syn.GetRow("ID", 0L), -1);
                    }
                    catch (Exception ex)
                    {
                        Assert.IsInstanceOf<Exception>(ex);
                    }
                }
                {
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1L);

                    Assert.AreEqual(l_Syn.GetRow("ID", 1L).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                    Assert.AreEqual(l_Syn.GetRow("UI", (uint)1).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(2));
                    row = row.SetID(layout.IDFieldIndex, 2);
                    Assert.AreEqual(l_Syn.GetRow("ID", 2L).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                }
                //Search
                {
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1L);

                    Assert.AreEqual(l_Syn.GetRow(Search.FieldEquals("ID", 1L)).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                }
                // Search + ResultOption
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Syn.Insert(row);
                }
                {
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1L);
                    Assert.AreEqual(l_Syn.GetRow(Search.FieldGreater("ID", 0L), ResultOption.Limit(1)).GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(10));
                    row = row.SetID(layout.IDFieldIndex, 20);
                    Row rowExpect = l_Syn.GetRow(Search.FieldGreater("ID", 0L), ResultOption.Limit(1) + ResultOption.SortDescending("ID"));
                    Assert.AreEqual(rowExpect.GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    try
                    {
                        l_Syn.GetRow(Search.FieldGreater("ID", 0L));
                    }
                    catch (Exception ex)
                    {
                        Assert.IsInstanceOf<Exception>(ex);
                    }
                }
                // get rows
                {
                    var ids = l_Syn.GetRows("I", 1);
                    Assert.AreEqual(ids.Count, 2);
                    //CollectionAssert.AreEqual(ids, new long[] { 1, 11 });
                }
                {
                    var ids = l_Syn.GetRows(Search.FieldGreater("ID", 0L));
                    Assert.AreEqual(ids.Count, 20);

                    ids = l_Syn.GetRows(Search.FieldGreater("ID", 0L) & Search.FieldSmaller("ID", 11L));
                    Assert.AreEqual(ids.Count, 10);

                    ids = l_Syn.GetRows(Search.FieldGreater("ID", 0L) & Search.FieldSmaller("ID", 11L), ResultOption.Limit(5));
                    Assert.AreEqual(ids.Count, 5);

                    ids = l_Syn.GetRows(Search.FieldGreater("ID", 0L), ResultOption.SortDescending("ID"));
                    Assert.AreEqual(ids.Count, 20);

                    var row = Row.Create(layout, TestStructClean.Create(10));
                    row = row.SetID(layout.IDFieldIndex, 20);
                    Assert.AreEqual(ids[0].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                }
                {
                    var rows = l_Syn.GetRows(new long[] { 1, 2, 3, 4, 5 });
                    var row = Row.Create(layout, TestStructClean.Create(1));
                    row = row.SetID(layout.IDFieldIndex, 1);
                    Assert.AreEqual(rows[0].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(2));
                    row = row.SetID(layout.IDFieldIndex, 2);
                    Assert.AreEqual(rows[1].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(3));
                    row = row.SetID(layout.IDFieldIndex, 3);
                    Assert.AreEqual(rows[2].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));

                    row = Row.Create(layout, TestStructClean.Create(4));
                    row = row.SetID(layout.IDFieldIndex, 4);
                    Assert.AreEqual(rows[3].GetStruct<TestStructClean>(layout), row.GetStruct<TestStructClean>(layout));
                }
            }
        }

        [Test]
        public void GetRowAt()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            var l_Memory = new MemoryTable(layout);

            var t1 = TestStructClean.Create(1);
            t1.ID = 1;
            var row1 = Row.Create(layout, t1);
            l_Memory.Insert(row1);

            var t2 = TestStructClean.Create(2);
            t2.ID = 2;
            var row2 = Row.Create(layout, t2);
            l_Memory.Insert(row2);

            var t3 = TestStructClean.Create(3);
            t3.ID = 3;
            var row3 = Row.Create(layout, t3);
            l_Memory.Insert(row3);
            // SynchronizedMemoryTable
            {
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                l_Syn.LoadTable(l_Memory);
                Assert.AreEqual(l_Syn.GetRowAt(0), row1);
                Assert.AreEqual(l_Syn.GetRowAt(1), row2);
                Assert.AreEqual(l_Syn.GetRowAt(2), row3);
                try
                {
                    l_Syn.GetRowAt(3);
                }
                catch (Exception ex)
                {
                    Assert.IsInstanceOf<ArgumentOutOfRangeException>(ex);
                }
            }
            // SynchronizedMemoryTable<T>
            {
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                l_Syn.LoadTable(l_Memory);
                Assert.AreEqual(l_Syn.GetRowAt(0), row1);
                Assert.AreEqual(l_Syn.GetRowAt(1), row2);
                Assert.AreEqual(l_Syn.GetRowAt(2), row3);
                try
                {
                    l_Syn.GetRowAt(3);
                }
                catch (Exception ex)
                {
                    Assert.IsInstanceOf<ArgumentOutOfRangeException>(ex);
                }
            }
        }

        [Test]
        public void Set()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            var l_Memory = new MemoryTable(layout);

            var t1 = TestStructClean.Create(1);
            t1.ID = 1;
            var row1 = Row.Create(layout, t1);
            l_Memory.Insert(row1);

            var t2 = TestStructClean.Create(2);
            t2.ID = 2;
            var row2 = Row.Create(layout, t2);
            l_Memory.Insert(row2);

            var t3 = TestStructClean.Create(3);
            t3.ID = 3;
            var row3 = Row.Create(layout, t3);
            l_Memory.Insert(row3);

            var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
            l_Syn.LoadTable(l_Memory);
            try
            {
                l_Syn.SetValue("ID", 10);
            }
            catch (Exception ex)
            {
                Assert.IsInstanceOf<ArgumentException>(ex);
            }
            l_Syn.SetValue("I", 10);
            Assert.AreEqual(l_Syn.Count(Search.FieldEquals("I", 10)), 3);
        }

        [Test]
        public void Exist()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            var l_Memory = new MemoryTable(layout);

            var t1 = TestStructClean.Create(1);
            t1.ID = 1;
            var row1 = Row.Create(layout, t1);
            l_Memory.Insert(row1);

            var t2 = TestStructClean.Create(2);
            t2.ID = 2;
            var row2 = Row.Create(layout, t2);
            l_Memory.Insert(row2);

            var t3 = TestStructClean.Create(3);
            t3.ID = 3;
            var row3 = Row.Create(layout, t3);
            l_Memory.Insert(row3);

            var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
            l_Syn.LoadTable(l_Memory);

            Assert.AreEqual(l_Syn.Exist(1), true);
            Assert.AreEqual(l_Syn.Exist(2), true);
            Assert.AreEqual(l_Syn.Exist(3), true);
            Assert.AreEqual(l_Syn.Exist(4), false);

        }

        [Test]
        public void Insert()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));


            var t1 = TestStructClean.Create(1);
            t1.ID = 1;
            var row1 = Row.Create(layout, t1);

            var t2 = TestStructClean.Create(2);
            t2.ID = 2;
            var row2 = Row.Create(layout, t2);

            var t3 = TestStructClean.Create(3);
            t3.ID = 3;
            var row3 = Row.Create(layout, t3);

            var t4 = TestStructClean.Create(4);
            var row4 = Row.Create(layout, t4);
            {
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));

                Assert.AreEqual(l_Syn.Insert(row1), 1);
                Assert.AreEqual(l_Syn.Insert(row2), 2);
                Assert.AreEqual(l_Syn.Insert(row3), 3);

                var id = l_Syn.Insert(row4);
                row4 = row4.SetID(layout.IDFieldIndex, id);
                Row rowExpect = l_Syn.GetRow(id);
                for (var i = 0; i < layout.FieldCount; i++)
                {
                    Assert.AreEqual(rowExpect.GetValue(i), row4.GetValue(i));
                }
                Assert.AreEqual(row4.GetStruct<TestStructClean>(layout), l_Syn.GetRow(id).GetStruct<TestStructClean>(layout));
            }
            {
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                l_Syn.Insert(new Row[] { row1, row2, row3, row4 });
                Assert.AreEqual(l_Syn.RowCount, 4);
                Assert.AreEqual(l_Syn.GetRow(1), row1);
                Assert.AreEqual(l_Syn.GetRow(2), row2);
                Assert.AreEqual(l_Syn.GetRow(3), row3);

                var id = l_Syn.GetRowAt(3).GetID(layout.IDFieldIndex);
                row4 = row4.SetID(layout.IDFieldIndex, id);
                Assert.AreEqual(row4.GetStruct<TestStructClean>(layout), l_Syn.GetRow(id).GetStruct<TestStructClean>(layout));
            }

        }

        [Test]
        public void Update()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            var l_Memory = new MemoryTable(layout);

            var t1 = TestStructClean.Create(1);
            t1.ID = 1;
            var row1 = Row.Create(layout, t1);
            l_Memory.Insert(row1);

            var t2 = TestStructClean.Create(2);
            t2.ID = 2;
            var row2 = Row.Create(layout, t2);
            l_Memory.Insert(row2);

            var t3 = TestStructClean.Create(3);
            t3.ID = 3;
            var row3 = Row.Create(layout, t3);
            l_Memory.Insert(row3);

            // SynchronizedMemoryTable
            {
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                l_Syn.LoadTable(l_Memory);
                {

                    row1 = Row.Create(layout, TestStructClean.Create(5));
                    row1 = row1.SetID(layout.IDFieldIndex, 1);
                    l_Syn.Update(row1);
                    Row rowExpect = l_Syn.GetRow(row1.GetID(layout.IDFieldIndex));
                    for (var i = 0; i < layout.FieldCount; i++)
                    {
                        Assert.AreEqual(row1.GetValue(i), rowExpect.GetValue(i));
                    }
                }
                {
                    row1 = Row.Create(layout, TestStructClean.Create(6));
                    row1 = row1.SetID(layout.IDFieldIndex, 1);

                    row2 = Row.Create(layout, TestStructClean.Create(7));
                    row2 = row2.SetID(layout.IDFieldIndex, 2);

                    row3 = Row.Create(layout, TestStructClean.Create(8));
                    row3 = row3.SetID(layout.IDFieldIndex, 3);

                    l_Syn.Update(new Row[] { row1, row2, row3 });
                    Row rowExpect1 = l_Syn.GetRow(1);
                    Row rowExpect2 = l_Syn.GetRow(2);
                    Row rowExpect3 = l_Syn.GetRow(3);

                    for (var i = 0; i < layout.FieldCount; i++)
                    {
                        Assert.AreEqual(row1.GetValue(i), rowExpect1.GetValue(i));
                        Assert.AreEqual(row2.GetValue(i), rowExpect2.GetValue(i));
                        Assert.AreEqual(row3.GetValue(i), rowExpect3.GetValue(i));
                    }
                }
            }
            // SynchronizedMemoryTable<T>
            {
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                l_Syn.LoadTable(l_Memory);
                {

                    row1 = Row.Create(layout, TestStructClean.Create(5));
                    row1 = row1.SetID(layout.IDFieldIndex, 1);
                    l_Syn.Update(row1);
                    Row rowExpect = l_Syn.GetRow(row1.GetID(layout.IDFieldIndex));
                    for (var i = 0; i < layout.FieldCount; i++)
                    {
                        Assert.AreEqual(row1.GetValue(i), rowExpect.GetValue(i));
                    }
                }
                {
                    row1 = Row.Create(layout, TestStructClean.Create(6));
                    row1 = row1.SetID(layout.IDFieldIndex, 1);

                    row2 = Row.Create(layout, TestStructClean.Create(7));
                    row2 = row2.SetID(layout.IDFieldIndex, 2);

                    row3 = Row.Create(layout, TestStructClean.Create(8));
                    row3 = row3.SetID(layout.IDFieldIndex, 3);

                    l_Syn.Update(new Row[] { row1, row2, row3 });
                    Row rowExpect1 = l_Syn.GetRow(1);
                    Row rowExpect2 = l_Syn.GetRow(2);
                    Row rowExpect3 = l_Syn.GetRow(3);

                    for (var i = 0; i < layout.FieldCount; i++)
                    {
                        Assert.AreEqual(row1.GetValue(i), rowExpect1.GetValue(i));
                        Assert.AreEqual(row2.GetValue(i), rowExpect2.GetValue(i));
                        Assert.AreEqual(row3.GetValue(i), rowExpect3.GetValue(i));
                    }
                }
                {
                    var u1 = TestStructClean.Create(5);
                    u1.ID = 1;
                    l_Syn.Update(u1);
                    Assert.AreEqual(l_Syn.GetStruct(1), u1);
                }
            }
        }

        [Test]
        public void Delete()
        {
            // SynchronizedMemoryTable
            {
                var layout = RowLayout.CreateTyped(typeof(TestStructClean));
                var l_Memory = new MemoryTable(layout);

                var t1 = TestStructClean.Create(1);
                t1.ID = 1;
                var row1 = Row.Create(layout, t1);
                l_Memory.Insert(row1);

                var t2 = TestStructClean.Create(2);
                t2.ID = 2;
                var row2 = Row.Create(layout, t2);
                l_Memory.Insert(row2);

                var t3 = TestStructClean.Create(3);
                t3.ID = 3;
                var row3 = Row.Create(layout, t3);
                l_Memory.Insert(row3);

                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(1);
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.RowCount, 2);

                    l_Syn.Delete(2);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.RowCount, 1);

                    l_Syn.Delete(3);
                    Assert.AreEqual(l_Syn.Exist(3), false);
                    Assert.AreEqual(l_Syn.RowCount, 0);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(new long[] { 1 });
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), true);
                    Assert.AreEqual(l_Syn.Exist(3), true);
                    Assert.AreEqual(l_Syn.RowCount, 2);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(new long[] { 1, 2 });
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.Exist(3), true);
                    Assert.AreEqual(l_Syn.RowCount, 1);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(new long[] { 1, 2, 3 });
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.Exist(3), false);
                    Assert.AreEqual(l_Syn.RowCount, 0);
                }
                /* TODO REPAIR
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(Search.FieldEquals("ID", 1L));
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), true);
                    Assert.AreEqual(l_Syn.Exist(3), true);
                    Assert.AreEqual(l_Syn.RowCount, 2);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(Search.FieldEquals("ID", 1L) | Search.FieldEquals("ID", 2L));
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.Exist(3), true);
                    Assert.AreEqual(l_Syn.RowCount, 1);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(Search.FieldGreater("ID", 0L));
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.Exist(3), false);
                    Assert.AreEqual(l_Syn.RowCount, 0);
                }
                */
            }
            // SynchronizedMemoryTable<T>
            {
                var l_Memory = new MemoryTable<TestStructClean>();

                var t1 = TestStructClean.Create(1);
                t1.ID = 1;
                l_Memory.Insert(t1);

                var t2 = TestStructClean.Create(2);
                t2.ID = 2;
                l_Memory.Insert(t2);

                var t3 = TestStructClean.Create(3);
                t3.ID = 3;
                l_Memory.Insert(t3);

                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(1);
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.RowCount, 2);

                    l_Syn.Delete(2);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.RowCount, 1);

                    l_Syn.Delete(3);
                    Assert.AreEqual(l_Syn.Exist(3), false);
                    Assert.AreEqual(l_Syn.RowCount, 0);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(new long[] { 1 });
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), true);
                    Assert.AreEqual(l_Syn.Exist(3), true);
                    Assert.AreEqual(l_Syn.RowCount, 2);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(new long[] { 1, 2 });
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.Exist(3), true);
                    Assert.AreEqual(l_Syn.RowCount, 1);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(new long[] { 1, 2, 3 });
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.Exist(3), false);
                    Assert.AreEqual(l_Syn.RowCount, 0);
                }
                /* TODO REPAIR
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(Search.FieldEquals("ID", 1L));
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), true);
                    Assert.AreEqual(l_Syn.Exist(3), true);
                    Assert.AreEqual(l_Syn.RowCount, 2);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(Search.FieldEquals("ID", 1L) | Search.FieldEquals("ID", 2L));
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.Exist(3), true);
                    Assert.AreEqual(l_Syn.RowCount, 1);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Delete(Search.FieldGreater("ID", 0L));
                    Assert.AreEqual(l_Syn.Exist(1), false);
                    Assert.AreEqual(l_Syn.Exist(2), false);
                    Assert.AreEqual(l_Syn.Exist(3), false);
                    Assert.AreEqual(l_Syn.RowCount, 0);
                }
                */
            }
        }

        [Test]
        public void Replace()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            // SynchronizedMemoryTable
            {
                var l_Memory = new MemoryTable(layout);

                var t1 = TestStructClean.Create(1);
                t1.ID = 1;
                var row1 = Row.Create(layout, t1);
                l_Memory.Insert(row1);

                var t2 = TestStructClean.Create(2);
                t2.ID = 2;
                var row2 = Row.Create(layout, t2);
                l_Memory.Insert(row2);

                var t3 = TestStructClean.Create(3);
                t3.ID = 3;
                var row3 = Row.Create(layout, t3);
                l_Memory.Insert(row3);

                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));

                var row4 = Row.Create(layout, TestStructClean.Create(4));
                row4 = row4.SetID(layout.IDFieldIndex, 1);

                var row5 = Row.Create(layout, TestStructClean.Create(5));
                row5 = row5.SetID(layout.IDFieldIndex, 2);

                var row6 = Row.Create(layout, TestStructClean.Create(6));
                row6 = row6.SetID(layout.IDFieldIndex, 3);

                var row7 = Row.Create(layout, TestStructClean.Create(7));
                row7 = row7.SetID(layout.IDFieldIndex, 4);
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Replace(row4);
                    l_Syn.Replace(row5);
                    l_Syn.Replace(row6);

                    Assert.AreEqual(l_Syn.GetRow(1), row4);
                    Assert.AreEqual(l_Syn.GetRow(2), row5);
                    Assert.AreEqual(l_Syn.GetRow(3), row6);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Replace(new Row[] { row4, row5, row6, row7 });

                    Assert.AreEqual(l_Syn.GetRow(1), row4);
                    Assert.AreEqual(l_Syn.GetRow(2), row5);
                    Assert.AreEqual(l_Syn.GetRow(3), row6);
                    Assert.AreEqual(l_Syn.GetRow(4), row7);
                }
            }
            // SynchronizedMemoryTable<T>
            {
                var l_Memory = new MemoryTable<TestStructClean>();

                var t1 = TestStructClean.Create(1);
                t1.ID = 1;
                l_Memory.Insert(t1);

                var t2 = TestStructClean.Create(2);
                t2.ID = 2;
                l_Memory.Insert(t2);

                var t3 = TestStructClean.Create(3);
                t3.ID = 3;
                l_Memory.Insert(t3);

                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");

                var row4 = Row.Create(layout, TestStructClean.Create(4));
                row4 = row4.SetID(layout.IDFieldIndex, 1);

                var row5 = Row.Create(layout, TestStructClean.Create(5));
                row5 = row5.SetID(layout.IDFieldIndex, 2);

                var row6 = Row.Create(layout, TestStructClean.Create(6));
                row6 = row6.SetID(layout.IDFieldIndex, 3);

                var row7 = Row.Create(layout, TestStructClean.Create(7));
                row7 = row7.SetID(layout.IDFieldIndex, 4);
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Replace(row4);
                    l_Syn.Replace(row5);
                    l_Syn.Replace(row6);

                    Assert.AreEqual(l_Syn.GetRow(1), row4);
                    Assert.AreEqual(l_Syn.GetRow(2), row5);
                    Assert.AreEqual(l_Syn.GetRow(3), row6);
                }
                {
                    l_Syn.LoadTable(l_Memory);
                    l_Syn.Replace(new Row[] { row4, row5, row6, row7 });

                    Assert.AreEqual(l_Syn.GetRow(1), row4);
                    Assert.AreEqual(l_Syn.GetRow(2), row5);
                    Assert.AreEqual(l_Syn.GetRow(3), row6);
                    Assert.AreEqual(l_Syn.GetRow(4), row7);
                }
            }
        }

        [Test]
        public void FindRow()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            // SynchronizedMemoryTable
            {
                var l_Memory = new MemoryTable(layout);
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Syn.Insert(row);
                }
                //find row
                {
                    Assert.AreEqual(l_Syn.FindRow("ID", 0L), -1);
                }
                {
                    Assert.AreEqual(l_Syn.FindRow("ID", 1L), 1);
                    Assert.AreEqual(l_Syn.FindRow("UI", (uint)1), 1);
                    Assert.AreEqual(l_Syn.FindRow("ID", 2L), 2);
                }
                //Search
                {
                    Assert.AreEqual(l_Syn.FindRow(Search.FieldEquals("ID", 1L)), 1);
                    // not exist
                    Assert.AreEqual(l_Syn.FindRow(Search.FieldEquals("ID", 0L)), -1);
                }
                // Search + ResultOption
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Syn.Insert(row);
                }
                {
                    Assert.AreEqual(l_Syn.FindRow(Search.FieldGreater("ID", 0L), ResultOption.Limit(1)), 1);
                    Assert.AreEqual(l_Syn.FindRow(Search.FieldGreater("ID", 0L), ResultOption.Limit(1) + ResultOption.SortDescending("ID")), 20);

                    try
                    {
                        l_Syn.FindRow(Search.FieldGreater("ID", 0L));
                    }
                    catch (Exception ex)
                    {
                        Assert.IsInstanceOf<Exception>(ex);
                    }
                }
                // findrows
                {
                    var ids = l_Syn.FindRows("I", 1);
                    Assert.AreEqual(ids.Count, 2);
                    CollectionAssert.AreEqual(ids, new long[] { 1, 11 });
                }
                {
                    var ids = l_Syn.FindRows(Search.FieldGreater("ID", 0L));
                    Assert.AreEqual(ids.Count, 20);

                    ids = l_Syn.FindRows(Search.FieldGreater("ID", 0L) & Search.FieldSmaller("ID", 11L));
                    Assert.AreEqual(ids.Count, 10);

                    ids = l_Syn.FindRows(Search.FieldGreater("ID", 0L) & Search.FieldSmaller("ID", 11L), ResultOption.Limit(5));
                    Assert.AreEqual(ids.Count, 5);

                    ids = l_Syn.FindRows(Search.FieldGreater("ID", 0L), ResultOption.SortDescending("ID"));
                    Assert.AreEqual(ids.Count, 20);
                    Assert.AreEqual(ids[0], 20);
                }
            }
            // SynchronizedMemoryTable<T>
            {
                var l_Memory = new MemoryTable<TestStructClean>();
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Syn.Insert(row);
                }
                //find row
                {
                    Assert.AreEqual(l_Syn.FindRow("ID", 0L), -1);
                }
                {
                    Assert.AreEqual(l_Syn.FindRow("ID", 1L), 1);
                    Assert.AreEqual(l_Syn.FindRow("UI", (uint)1), 1);
                    Assert.AreEqual(l_Syn.FindRow("ID", 2L), 2);
                }
                //Search
                {
                    Assert.AreEqual(l_Syn.FindRow(Search.FieldEquals("ID", 1L)), 1);
                    // not exist
                    Assert.AreEqual(l_Syn.FindRow(Search.FieldEquals("ID", 0L)), -1);
                }
                // Search + ResultOption
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Syn.Insert(row);
                }
                {
                    Assert.AreEqual(l_Syn.FindRow(Search.FieldGreater("ID", 0L), ResultOption.Limit(1)), 1);
                    Assert.AreEqual(l_Syn.FindRow(Search.FieldGreater("ID", 0L), ResultOption.Limit(1) + ResultOption.SortDescending("ID")), 20);

                    try
                    {
                        l_Syn.FindRow(Search.FieldGreater("ID", 0L));
                    }
                    catch (Exception ex)
                    {
                        Assert.IsInstanceOf<Exception>(ex);
                    }
                }
                // findrows
                {
                    var ids = l_Syn.FindRows("I", 1);
                    Assert.AreEqual(ids.Count, 2);
                    CollectionAssert.AreEqual(ids, new long[] { 1, 11 });
                }
                {
                    var ids = l_Syn.FindRows(Search.FieldGreater("ID", 0L));
                    Assert.AreEqual(ids.Count, 20);

                    ids = l_Syn.FindRows(Search.FieldGreater("ID", 0L) & Search.FieldSmaller("ID", 11L));
                    Assert.AreEqual(ids.Count, 10);

                    ids = l_Syn.FindRows(Search.FieldGreater("ID", 0L) & Search.FieldSmaller("ID", 11L), ResultOption.Limit(5));
                    Assert.AreEqual(ids.Count, 5);

                    ids = l_Syn.FindRows(Search.FieldGreater("ID", 0L), ResultOption.SortDescending("ID"));
                    Assert.AreEqual(ids.Count, 20);
                    Assert.AreEqual(ids[0], 20);
                }
            }
        }

        [Test]
        public void GetValues()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            // SynchronizedMemoryTable
            {
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                var l_Memory = new MemoryTable(layout);
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Memory.Insert(row);
                }
                l_Syn.LoadTable(l_Memory);
                {
                    CollectionAssert.AreEqual(l_Syn.GetValues<long>("ID", true), l_Syn.IDs);
                    CollectionAssert.AreEqual(l_Syn.GetValues<byte>("B", true), l_Memory.GetValues<byte>("B"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<sbyte>("SB", true), l_Memory.GetValues<sbyte>("SB"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<char>("C", true), l_Memory.GetValues<char>("C"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<short>("S", true), l_Memory.GetValues<short>("S"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<ushort>("US", true), l_Memory.GetValues<ushort>("US"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<int>("I", true), l_Memory.GetValues<int>("I"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<uint>("UI", true), l_Memory.GetValues<uint>("UI"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<byte[]>("Arr", true), l_Memory.GetValues<byte[]>("Arr"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<string>("Text", true), l_Memory.GetValues<string>("Text"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<TimeSpan>("Time", true), l_Memory.GetValues<TimeSpan>("Time"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<DateTime>("Date", true), l_Memory.GetValues<DateTime>("Date"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<double>("D", true), l_Memory.GetValues<double>("D"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<float>("F", true), l_Memory.GetValues<float>("F"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<decimal>("Dec", true), l_Memory.GetValues<decimal>("Dec"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<Uri>("Uri", true), l_Memory.GetValues<Uri>("Uri"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<ConnectionString>("ConStr", true), l_Memory.GetValues<ConnectionString>("ConStr"));
                }
                {
                    CollectionAssert.AreEqual(l_Syn.GetValues<long>("ID"), l_Syn.IDs);
                    CollectionAssert.AreEqual(l_Syn.GetValues<byte>("B"), l_Memory.GetValues<byte>("B"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<sbyte>("SB"), l_Memory.GetValues<sbyte>("SB"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<char>("C"), l_Memory.GetValues<char>("C"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<short>("S"), l_Memory.GetValues<short>("S"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<ushort>("US"), l_Memory.GetValues<ushort>("US"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<int>("I"), l_Memory.GetValues<int>("I"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<uint>("UI"), l_Memory.GetValues<uint>("UI"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<byte[]>("Arr"), l_Memory.GetValues<byte[]>("Arr"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<string>("Text"), l_Memory.GetValues<string>("Text"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<TimeSpan>("Time"), l_Memory.GetValues<TimeSpan>("Time"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<DateTime>("Date"), l_Memory.GetValues<DateTime>("Date"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<double>("D"), l_Memory.GetValues<double>("D"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<float>("F"), l_Memory.GetValues<float>("F"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<decimal>("Dec"), l_Memory.GetValues<decimal>("Dec"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<Uri>("Uri"), l_Memory.GetValues<Uri>("Uri"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<ConnectionString>("ConStr"), l_Memory.GetValues<ConnectionString>("ConStr"));
                }
            }
            // SynchronizedMemoryTable
            {
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                var l_Memory = new MemoryTable(layout);
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Memory.Insert(row);
                }
                l_Syn.LoadTable(l_Memory);
                {
                    CollectionAssert.AreEqual(l_Syn.GetValues<long>("ID", true), l_Syn.IDs);
                    CollectionAssert.AreEqual(l_Syn.GetValues<byte>("B", true), l_Memory.GetValues<byte>("B"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<sbyte>("SB", true), l_Memory.GetValues<sbyte>("SB"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<char>("C", true), l_Memory.GetValues<char>("C"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<short>("S", true), l_Memory.GetValues<short>("S"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<ushort>("US", true), l_Memory.GetValues<ushort>("US"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<int>("I", true), l_Memory.GetValues<int>("I"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<uint>("UI", true), l_Memory.GetValues<uint>("UI"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<byte[]>("Arr", true), l_Memory.GetValues<byte[]>("Arr"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<string>("Text", true), l_Memory.GetValues<string>("Text"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<TimeSpan>("Time", true), l_Memory.GetValues<TimeSpan>("Time"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<DateTime>("Date", true), l_Memory.GetValues<DateTime>("Date"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<double>("D", true), l_Memory.GetValues<double>("D"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<float>("F", true), l_Memory.GetValues<float>("F"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<decimal>("Dec", true), l_Memory.GetValues<decimal>("Dec"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<Uri>("Uri", true), l_Memory.GetValues<Uri>("Uri"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<ConnectionString>("ConStr", true), l_Memory.GetValues<ConnectionString>("ConStr"));
                }
                {
                    CollectionAssert.AreEqual(l_Syn.GetValues<long>("ID"), l_Syn.IDs);
                    CollectionAssert.AreEqual(l_Syn.GetValues<byte>("B"), l_Memory.GetValues<byte>("B"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<sbyte>("SB"), l_Memory.GetValues<sbyte>("SB"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<char>("C"), l_Memory.GetValues<char>("C"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<short>("S"), l_Memory.GetValues<short>("S"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<ushort>("US"), l_Memory.GetValues<ushort>("US"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<int>("I"), l_Memory.GetValues<int>("I"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<uint>("UI"), l_Memory.GetValues<uint>("UI"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<byte[]>("Arr"), l_Memory.GetValues<byte[]>("Arr"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<string>("Text"), l_Memory.GetValues<string>("Text"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<TimeSpan>("Time"), l_Memory.GetValues<TimeSpan>("Time"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<DateTime>("Date"), l_Memory.GetValues<DateTime>("Date"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<double>("D"), l_Memory.GetValues<double>("D"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<float>("F"), l_Memory.GetValues<float>("F"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<decimal>("Dec"), l_Memory.GetValues<decimal>("Dec"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<Uri>("Uri"), l_Memory.GetValues<Uri>("Uri"));
                    CollectionAssert.AreEqual(l_Syn.GetValues<ConnectionString>("ConStr"), l_Memory.GetValues<ConnectionString>("ConStr"));
                }
            }
        }

        [Test]
        public void ToMemory()
        {

            // SynchronizedMemoryTable
            {
                var layout = RowLayout.CreateTyped(typeof(TestStructClean));
                var table = new MemoryTable(layout);
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    table.Insert(row);
                }
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                l_Syn.LoadTable(table);

                MemoryTable l_MemoryExpect = l_Syn.ToMemory();
                RowLayout.CheckLayout(l_MemoryExpect.Layout, table.Layout);
                Assert.AreEqual(table.RowCount, l_MemoryExpect.RowCount);
                foreach (Row row in table.GetRows())
                {
                    Assert.AreEqual(row, l_MemoryExpect.GetRow(row.GetID(layout.IDFieldIndex)));
                }
            }
            // SynchronizedMemoryTable<T>
            {

                var table = new MemoryTable<TestStructClean>();
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    t.ID = i;
                    table.Insert(t);
                }
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                l_Syn.LoadTable(table);

                MemoryTable<TestStructClean> l_MemoryExpect = l_Syn.ToTypedMemory();
                RowLayout.CheckLayout(l_MemoryExpect.Layout, table.Layout);
                Assert.AreEqual(table.RowCount, l_MemoryExpect.RowCount);
                foreach (TestStructClean item in table.GetStructs())
                {
                    Assert.AreEqual(item, l_MemoryExpect.GetStruct(item.ID));
                }
            }
        }

        [Test]
        public void SetRows()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            // SynchronizedMemoryTable
            {
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                var l_List = new List<Row>();
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    row = row.SetID(layout.IDFieldIndex, i);
                    l_List.Add(row);
                }
                l_Syn.SetRows(l_List);
                Assert.AreEqual(l_Syn.RowCount, 10);
                CollectionAssert.AreEqual(l_Syn.GetRows(), l_List.ToArray());
            }
            // SynchronizedMemoryTable<T>
            {
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                var l_List = new List<Row>();
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    t.ID = i;
                    var row = Row.Create(layout, t);
                    row = row.SetID(layout.IDFieldIndex, i);
                    l_List.Add(row);
                }
                l_Syn.SetRows(l_List);
                Assert.AreEqual(l_Syn.RowCount, 10);
                CollectionAssert.AreEqual(l_Syn.GetRows(), l_List.ToArray());
            }
        }

        [Test]
        public void GetNextUsedID()
        {
            // SynchronizedMemoryTable
            {
                var layout = RowLayout.CreateTyped(typeof(TestStructClean));
                var l_Syn = new SynchronizedMemoryTable(RowLayout.CreateTyped(typeof(TestStructClean)));
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    var row = Row.Create(layout, t);
                    l_Syn.Insert(row);
                    // get free id
                    Assert.AreEqual(l_Syn.GetNextFreeID(), i + 1);
                }

                // get used id
                for (var i = 0; i < 10; i++)
                {
                    Assert.AreEqual(l_Syn.GetNextUsedID(i), i + 1);
                }
                Assert.AreEqual(l_Syn.GetNextUsedID(10), -1);
            }
            // SynchronizedMemoryTable<T>
            {
                var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
                for (var i = 1; i <= 10; i++)
                {
                    var t = TestStructClean.Create(i);
                    l_Syn.Insert(t);
                    // get free id
                    Assert.AreEqual(l_Syn.GetNextFreeID(), i + 1);
                }

                // get used id
                for (var i = 0; i < 10; i++)
                {
                    Assert.AreEqual(l_Syn.GetNextUsedID(i), i + 1);
                }
                Assert.AreEqual(l_Syn.GetNextUsedID(10), -1);
            }
        }

        [Test]
        public void GetStructs()
        {
            var l_Syn = new SynchronizedMemoryTable<TestStructClean>("Table");
            var l_List = new List<TestStructClean>();
            for (var i = 1; i <= 10; i++)
            {
                var t = TestStructClean.Create(i);
                t.ID = i;
                l_List.Add(t);
            }
            l_Syn.SetStructs(l_List.ToArray());

            Assert.AreEqual(l_Syn.GetStruct(Search.FieldEquals("ID", 1L), ResultOption.SortAscending("ID")), l_List.First(p => p.ID == 1));
            Assert.AreEqual(l_Syn.GetStruct(Search.None, ResultOption.SortAscending("ID") + ResultOption.Limit(1)), l_List.First());
            Assert.AreEqual(l_Syn.GetStruct(Search.None, ResultOption.SortDescending("ID") + ResultOption.Limit(1)), l_List.Last());


            var result = l_Syn.GetStructs(new long[] { 1, 2, 3 });
            var expect = l_List.Where(p => p.ID <= 3);
            CollectionAssert.AreEqual(result, expect);

            result = l_Syn.GetStructs(new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
            expect = l_List.Where(p => p.ID <= 9).ToArray();
            CollectionAssert.AreEqual(result, expect);
        }
    }
}