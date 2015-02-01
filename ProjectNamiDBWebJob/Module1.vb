Imports System
Imports System.Text
Imports System.Configuration.ConfigurationManager
Imports System.Linq
Imports Microsoft.WindowsAzure
Imports System.Threading

Module Module1

    Sub Main()
        Console.Out.WriteLine("Begin Job")
        Dim SqlVer As Integer = GetSqlVer()
        If SqlVer >= 12 Then
            Console.Out.WriteLine("SQL V12 or greater detected")
        End If
        Dim tables As DataTable = GetTables(SqlVer)

        For Each tablerow As DataRow In tables.Rows
            Console.Out.WriteLine("Table - " & tablerow("table_name"))

            Dim RebuildThread As New Rebuilder
            RebuildThread.TableName = tablerow("table_name")
            RebuildThread.SqlVer = SqlVer
            Dim ts As New ThreadStart(AddressOf RebuildThread.ExecuteRebuild)
            Dim wrkThread As New Thread(ts)
            RebuildThread.CurrentThread = wrkThread
            wrkThread.Start()
            While True
                Thread.Sleep(15000)
                If wrkThread.IsAlive Then
                    Console.Out.WriteLine(tablerow("table_name") & " - rebuild in progress")
                Else
                    Exit While
                End If
            End While
        Next

        Console.Out.WriteLine("End Job")

    End Sub


    Function SQLCmd() As System.Data.SqlClient.SqlCommand
        Dim SqlConnString As New System.Data.SqlClient.SqlConnectionStringBuilder
        SqlConnString.DataSource = System.Configuration.ConfigurationManager.AppSettings("ProjectNami.DBHost")
        SqlConnString.InitialCatalog = System.Configuration.ConfigurationManager.AppSettings("ProjectNami.DBName")
        SqlConnString.UserID = System.Configuration.ConfigurationManager.AppSettings("ProjectNami.DBUser")
        SqlConnString.Password = System.Configuration.ConfigurationManager.AppSettings("ProjectNami.DBPass")

        Dim SQLConn As New System.Data.SqlClient.SqlConnection
        SQLConn.ConnectionString = SqlConnString.ConnectionString
        SQLConn.Open()

        SQLCmd = New System.Data.SqlClient.SqlCommand
        SQLCmd.Connection = SQLConn
        SQLCmd.CommandTimeout = 0

    End Function

    Function GetTables(SqlVer As Integer) As DataTable
        Dim thisDataTable As New DataTable
        Dim thisCmd As SqlClient.SqlCommand = SQLCmd()
        Dim thisAdapter As New SqlClient.SqlDataAdapter
        If SqlVer >= 12 Then
            thisCmd.CommandText = "SELECT * FROM information_schema.tables where table_type = 'BASE TABLE' order by table_name"
        Else
            thisCmd.CommandText = "SELECT * FROM information_schema.tables where table_type = 'BASE TABLE' order by newid()"
        End If
        thisCmd.CommandType = CommandType.Text
        thisAdapter.SelectCommand = thisCmd
        thisAdapter.Fill(thisDataTable)
        thisCmd.Connection.Close()
        thisCmd.Connection.Dispose()
        thisCmd.Dispose()
        thisAdapter.Dispose()
        Return thisDataTable
    End Function

    Function GetSqlVer() As Integer
        Dim thisDataTable As New DataTable
        Dim thisCmd As SqlClient.SqlCommand = SQLCmd()
        Dim thisAdapter As New SqlClient.SqlDataAdapter
        thisCmd.CommandText = "SELECT convert(varchar,SERVERPROPERTY('productversion')) as 'version'"
        thisCmd.CommandType = CommandType.Text
        thisAdapter.SelectCommand = thisCmd
        thisAdapter.Fill(thisDataTable)
        thisCmd.Connection.Close()
        thisCmd.Connection.Dispose()
        thisCmd.Dispose()
        thisAdapter.Dispose()

        If thisDataTable.Rows.Count <> 1 Then
            Return 0
        Else
            Dim SqlVerString As String() = thisDataTable.Rows(0).Item(0).ToString.Split(".")
            If Not IsNumeric(SqlVerString(0)) Then
                Return 0
            Else
                Return CLng(SqlVerString(0))
            End If
        End If

    End Function

    Class Rebuilder
        Public TableName As String
        Public SqlVer As Integer
        Public CurrentThread As Thread

        Sub ExecuteRebuild()
            Dim ThisCmd As System.Data.SqlClient.SqlCommand = SQLCmd()
            If SqlVer >= 12 Then
                ThisCmd.CommandText = "ALTER INDEX ALL ON " & TableName & " REBUILD WITH (ONLINE = ON)"
            Else
                ThisCmd.CommandText = "ALTER INDEX ALL ON " & TableName & " REBUILD"
            End If
            ThisCmd.CommandType = CommandType.Text
            ThisCmd.ExecuteNonQuery()
            ThisCmd.Connection.Close()
            ThisCmd.Connection.Dispose()
            ThisCmd.Dispose()

            CurrentThread.Abort()
            CurrentThread = Nothing
        End Sub

    End Class

End Module
