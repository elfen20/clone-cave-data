using NUnit.Framework;
using System;
using System.Reflection;

namespace Test.Cave.Data
{
    public class TestRunner
    {
        [STAThread]
        public static int Main2()
        {
            int errorCount = 0;
            foreach (Type t in typeof(TestRunner).Assembly.GetTypes())
            {
                if (!t.IsClass) continue;
                foreach (Attribute a in t.GetCustomAttributes(false))
                {
                    if (a is TestFixtureAttribute)
                    {
                        Console.WriteLine(t.FullName);
                        object o = Activator.CreateInstance(t);
                        foreach (MethodInfo m in t.GetMethods())
                        {
                            foreach (Attribute ma in m.GetCustomAttributes(false))
                            {
                                if (ma is TestAttribute)
                                {
                                    Console.Write("+ " + m);
                                    try
                                    {
                                        m.Invoke(o, new object[0]);
                                        Console.WriteLine(" ok");
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine(" error");
                                        Console.WriteLine("! " + ex.ToString());
                                        errorCount++;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return errorCount;
        }
    }
}
