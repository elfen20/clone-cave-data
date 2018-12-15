﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using Cave;
using Cave.Data;
using Test.Cave;

namespace Test.Cave.Data
{
    [TestFixture]
    public class Test_RowLayout
    {
        [Test]
        public void Test_RowLayout_Check()
        {
            RowLayout layoutA = RowLayout.CreateTyped(typeof(TestStructBug));
            List<FieldProperties> fields = new List<FieldProperties>();
            fields.Add(new FieldProperties("TestStructBug", FieldFlags.ID, DataType.UInt64, "IDField"));
            fields.Add(new FieldProperties("TestStructBug", FieldFlags.Index, DataType.UInt32, "IndexedField"));
            fields.Add(new FieldProperties("TestStructBug", FieldFlags.Unique, DataType.Int16, "UniqueField"));
            fields.Add(new FieldProperties("TestStructBug", FieldFlags.AutoIncrement, DataType.UInt16, "AutoIncField"));
            fields.Add(new FieldProperties("TestStructBug", FieldFlags.Index | FieldFlags.AutoIncrement, DataType.UInt8, "AutoIncIndexField"));
            fields.Add(new FieldProperties("TestStructBug", FieldFlags.Unique | FieldFlags.Index, DataType.Int8, "UniqueIndexedField"));
            fields.Add(new FieldProperties("TestStructBug", FieldFlags.Index | FieldFlags.AutoIncrement | FieldFlags.Unique, DataType.Int64, "AutoIncUniqueIndexedField"));
            fields.Add(new FieldProperties("TestStructBug", FieldFlags.None, DataType.Enum, typeof(PlatformType), "SomeEnum"));
            fields.Add(new FieldProperties("TestStructBug", FieldFlags.None, DataType.String, "BuggyField"));

            RowLayout layoutB = RowLayout.CreateUntyped("TestStruct", fields.ToArray());
            RowLayout.CheckLayout(layoutB, layoutA);
        }

        [Test]
        public void Text_RowLayout_TypedCheck()
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(TestStructClean));
            Assert.AreEqual(layout.IDField.Name, layout.IDField.NameAtDatabase);
            Assert.AreEqual(layout.IDFieldIndex, 0);

        }
    }
}
