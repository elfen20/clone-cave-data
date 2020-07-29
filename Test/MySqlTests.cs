using System;
using System.Collections.Generic;
using NUnit.Framework;
using Cave;
using Cave.Data;
using Cave.Data.Mysql;

namespace Test.Cave.Data
{
#if NET45
    [TestFixture]
    class MySqlTests
    {
        IStorage mySqlStore = null;

        [OneTimeSetUp]
        public void ClassInit()
        {
            using (var conn = new MySql.Data.MySqlClient.MySqlConnection()) {}
            mySqlStore = new MySqlStorage("connection");
        }

        [Test]
        public void CheckLayout()
        {
            if (mySqlStore != null)
            {

            }
        }

    }
#endif
}
