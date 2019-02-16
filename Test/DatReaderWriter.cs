using Cave.Data;
using NUnit.Framework;
using System.IO;

namespace Test.Cave.Data
{
    [TestFixture]
    public class DatReaderWriter
    {
        [Test]
        public void StructReadWrite()
        {
            MemoryStream stream = new MemoryStream();
            DatWriter writer = new DatWriter(RowLayout.CreateTyped(typeof(TestStructClean)), stream);
            for (int i = 0; i < 100; i++)
            {
                TestStructClean t = TestStructClean.Create(i);
                writer.Write(t);
            }
            stream.Seek(0, SeekOrigin.Begin);
            DatReader reader = new DatReader(stream);
            for (int i = 0; i < 100; i++)
            {
                TestStructClean t = new TestStructClean();
                Assert.IsTrue(reader.ReadRow<TestStructClean>(true, out t));
                Assert.AreEqual(t, TestStructClean.Create(i));
            }
        }
    }
}
