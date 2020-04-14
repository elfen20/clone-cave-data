using System;
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
    public class RowLayoutTests
    {
        static void CreateField(ref List<FieldProperties> fields, FieldFlags flags, DataType dataType, string name, Type valueType=null)
        {
            fields.Add(new FieldProperties()
            {
                Index = fields.Count + 1,
                Name = name,
                Flags = flags,
                DataType = dataType,
                ValueType = valueType,
            });
        }

        [Test]
        public void CheckLayout()
        {
            var layoutA = RowLayout.CreateTyped(typeof(TestStructBug));
            var fields = new List<FieldProperties>();
            CreateField(ref fields, FieldFlags.ID, DataType.UInt64, "IDField");
            CreateField(ref fields, FieldFlags.Index, DataType.UInt32, "IndexedField");
            CreateField(ref fields, FieldFlags.Unique, DataType.Int16, "UniqueField");
            CreateField(ref fields, FieldFlags.AutoIncrement, DataType.UInt16, "AutoIncField");
            CreateField(ref fields, FieldFlags.Index | FieldFlags.AutoIncrement, DataType.UInt8, "AutoIncIndexField");
            CreateField(ref fields, FieldFlags.Unique | FieldFlags.Index, DataType.Int8, "UniqueIndexedField");
            CreateField(ref fields, FieldFlags.Index | FieldFlags.AutoIncrement | FieldFlags.Unique, DataType.Int64, "AutoIncUniqueIndexedField");
            CreateField(ref fields, FieldFlags.None, DataType.Enum, "SomeEnum", typeof(Environment.SpecialFolder));
            CreateField(ref fields, FieldFlags.None, DataType.String, "BuggyField");

            var layoutB = RowLayout.CreateUntyped("TestStruct", fields.ToArray());
            RowLayout.CheckLayout(layoutB, layoutA);
        }

        [Test]
        public void TypedCheck()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            foreach (var field in layout.Fields)
            {
                Assert.AreEqual(field.Name, field.NameAtDatabase);
            }
        }
    }
}
