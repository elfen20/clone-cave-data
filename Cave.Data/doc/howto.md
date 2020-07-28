# Packages supported and tested
To connect to a database server a working IDbConnection implementation needs to be used.
Currently the library supports the following implementations:
- nuget: npgsql **pgsql://user:pass@server)**
- nuget or gac: mysql.data **mysql://user:pass@server)**
- nuget: mysqlconnector **mysql://user:pass@server)**
- nuget or gac: sqlite **sqlite://localhost/path/filename**
- gac: mssql **mssql://server** (supports windows auth)

# Connect to server and list databases and tables
```csharp
// connect to a postgresql server
var store = Connector.ConnectStorage("pgsql://user:pass@server")
foreach (var databaseName in store.DatabaseNames)
{
    var database = store.GetDatabase(databaseName);
    foreach (var tableName in database.TableNames)
    {
        var table = database.GetTable(tableName);
        Console.WriteLine(table);
    }
}
```

# Create a strong typed result dataset
```csharp
// connect to mysql database
var connection = new MySqlStorage("mysql://user:pass@server");
// run a query and retrieve the layout of the result set.
RowLayout layout = null;
connection.Query(ref layout, null, null, "SHOW MASTER STATUS;");
// generate structure code
var code = layout.BuildTableStruct("mysql", "MasterStatus");
// write to file
File.WriteAllText("MysqlMasterStatus.cs", code);
```

# Use a strong typed table instance
```csharp
var table = store["mysql"]["user"];
var users = new Table<MysqlUser>(table);
Console.WriteLine("user; host;");
foreach (var row in users.GetStructs())
{
    Console.WriteLine($"{row.User}; {row.Host};");
}
```

# Compile a strong typed layout directly

```csharp
var code = table.BuildTableStruct();
var ccp = new CSharpCodeProvider();
var par = new CompilerParameters() { GenerateInMemory = true, };            
par.ReferencedAssemblies.Add(typeof(Cave.Data.Table).Assembly.GetAssemblyFilePath());
par.ReferencedAssemblies.Add(typeof(Cave.IO.StringEncoding).Assembly.GetAssemblyFilePath());
var res = ccp.CompileAssemblyFromSource(par, code);
if (res.Errors.HasErrors)
{
    throw new AggregateException(res.Errors.Cast<CompilerError>().Select(e => new Exception(e.ToString())));
}
var asm =  ccp.CompiledAssembly;
```
