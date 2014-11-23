﻿Imports System
Imports System.Text
Imports System.Configuration.ConfigurationManager
Imports System.Linq
Imports Microsoft.WindowsAzure
Imports System.Threading

Module Module1

    Sub Main()
        Console.Out.WriteLine("Begin Job")
        Dim tables As DataTable = GetTables()

        For Each tablerow As DataRow In tables.Rows
            Console.Out.WriteLine("Table - " & tablerow("table_name"))

            Dim RebuildThread As New Rebuilder
            RebuildThread.TableName = tablerow("table_name")
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

            'ExecuteRebuild(tablerow("table_name"))
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

    Function GetTables() As DataTable
        Dim thisDataTable As New DataTable
        Dim thisCmd As SqlClient.SqlCommand = SQLCmd()
        Dim thisAdapter As New SqlClient.SqlDataAdapter
        thisCmd.CommandText = "SELECT table_name FROM information_schema.tables order by newid()"
        thisCmd.CommandType = CommandType.Text
        thisAdapter.SelectCommand = thisCmd
        thisAdapter.Fill(thisDataTable)
        thisCmd.Connection.Close()
        thisCmd.Connection.Dispose()
        thisCmd.Dispose()
        thisAdapter.Dispose()
        Return thisDataTable
    End Function


    Class Rebuilder
        Public TableName As String
        Public CurrentThread As Thread

        Sub ExecuteRebuild()
            Dim ThisCmd As System.Data.SqlClient.SqlCommand = SQLCmd()
            ThisCmd.CommandText = "ALTER INDEX ALL ON " & TableName & " REBUILD" ' WITH (ONLINE = ON)
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
