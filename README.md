# Note
您拥有在本地自由使用及修改本源代码的权限，但如果本源代码及其修改代码在被编译时被使用了代码混淆功能，则视为您主动放弃这个权限。

# Utility.Storage
C# ORM工具。内部使用[Dapper](https://github.com/StackExchange/Dapper)，为了减少外部依赖，直接使用了Dapper源代码(早期版本，在Dapper还是一个源文件的时候)。

# Usage

using Utility.Storage.StorageHelper;

...  
static Storage storage = new Storage("Server=*;Database=*;UID=*;PWD=*");  
// 默认连接sql server, 若是其他类型数据库，需提供ConnectionProvider，如mysql：  
// storage.ConnectionProvider = connectionString => new MySqlConnection(connectionString);  
...

1. 查询  
// 多条记录  
storage.Query&lt;User&gt;("select * from [user]");  
storage.Query&lt;User&gt;("select * from [user] where [id] > @id", x => x["id", 1]); // 传参  
// 单条记录  
storage.Single&lt;User&gt;("select * from [user] where [id] = @id", x => x["id", 1]);  

2. 操作  
int rows = storage.Execute("update [user] set [name] = @name where [id] = @id", x => x["id", 1]["name", "tom", 20]);  

3. 事务  
using(var t = Storage.NewTransaction()) // 不支持跨库事务  
{  
   &nbsp;&nbsp;&nbsp;&nbsp;...  
   &nbsp;&nbsp;&nbsp;&nbsp;t.Complete();  
}  
