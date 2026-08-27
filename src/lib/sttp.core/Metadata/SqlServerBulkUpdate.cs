//******************************************************************************************************
//  SqlServerBulkUpdate.cs - Gbtc
//
//  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may
//  not use this file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://www.opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  08/16/2026 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

#if NET
using Microsoft.Data.SqlClient;
#else
using System.Data.SqlClient;
#endif

namespace sttp;

/// <summary>
/// Applies bulk updates to a SQL Server table by staging rows in a temporary table and issuing a single
/// set based <c>UPDATE ... FROM</c> per batch.
/// </summary>
/// <remarks>
/// <para>
/// Combining update statements into one command reduces round trips but not trigger cost: semicolon
/// separated statements are still separate statements, so a statement level trigger fires once for each of
/// them. Measured against a 193,000 measurement re-synchronization, that left the update path dominated by
/// trigger work - the .NET Framework schema's change tracking trigger creates and drops two temporary
/// tables every time it fires.
/// </para>
/// <para>
/// Staging the rows and joining to them collapses an entire batch into one statement, so the trigger fires
/// once per batch rather than once per row, while still recording the same change tracking rows.
/// </para>
/// </remarks>
internal sealed class SqlServerBulkUpdate : IDisposable
{
    private readonly MetadataSyncContext m_context;
    private readonly string m_targetTable;
    private readonly string m_stagingTableName;
    private readonly string m_updateSql;
    private readonly DataTable m_stagingTable;
    private readonly int m_batchSize;
    private bool m_stagingTableCreated;
    private bool m_disposed;

    /// <summary>
    /// Creates a new <see cref="SqlServerBulkUpdate"/>.
    /// </summary>
    /// <param name="context">Current synchronization context.</param>
    /// <param name="targetTable">Table to update.</param>
    /// <param name="keyColumn">Column used to match staged rows to target rows.</param>
    /// <param name="updateColumns">Columns to assign, in the order values are added after the key.</param>
    public SqlServerBulkUpdate(MetadataSyncContext context, string targetTable, string keyColumn, string[] updateColumns)
    {
        m_context = context;
        m_targetTable = targetTable;
        m_stagingTableName = $"#{targetTable}BulkUpdate";
        m_batchSize = context.BatchSize > 0 ? context.BatchSize : 10000;

        string columnList = string.Join(", ", new[] { keyColumn }.Concat(updateColumns));

        // Take the staging schema from the destination table so that column types match exactly
        m_stagingTable = context.RetrieveData($"SELECT TOP 0 {columnList} FROM {targetTable}");
        m_stagingTable.TableName = m_stagingTableName;

        string assignments = string.Join(", ", updateColumns.Select(column => $"target.{column} = staged.{column}"));
        m_updateSql = $"UPDATE target SET {assignments} FROM {targetTable} AS target INNER JOIN {m_stagingTableName} AS staged ON target.{keyColumn} = staged.{keyColumn}";
    }

    /// <summary>
    /// Adds a row to the batch, flushing if the batch is now full.
    /// </summary>
    public void Add(params object?[] values)
    {
        if (values.Length != m_stagingTable.Columns.Count)
            throw new ArgumentException($"Expected {m_stagingTable.Columns.Count} values to match bulk update column list, received {values.Length}.", nameof(values));

        DataRow row = m_stagingTable.NewRow();

        for (int i = 0; i < values.Length; i++)
            row[i] = SqlServerBulkInsert.Coerce(values[i], m_stagingTable.Columns[i].DataType);

        m_stagingTable.Rows.Add(row);

        if (m_stagingTable.Rows.Count >= m_batchSize)
            Flush();
    }

    /// <summary>
    /// Applies any accumulated rows to the target table.
    /// </summary>
    public void Flush()
    {
        if (m_stagingTable.Rows.Count == 0)
            return;

        EnsureStagingTable();

        // Triggers are irrelevant on the staging table itself, so the fastest copy options apply here; the
        // subsequent set based update fires the target table's triggers normally
        using (SqlBulkCopy bulkCopy = new((SqlConnection)m_context.Database.Connection, SqlBulkCopyOptions.TableLock, (SqlTransaction?)m_context.Command.Transaction))
        {
            bulkCopy.DestinationTableName = m_stagingTableName;
            bulkCopy.BulkCopyTimeout = m_context.Timeout;
            bulkCopy.BatchSize = m_batchSize;

            foreach (DataColumn column in m_stagingTable.Columns)
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);

            bulkCopy.WriteToServer(m_stagingTable);
        }

        m_context.StatementCount++;
        m_context.ExecuteNonQuery(m_updateSql);
        m_context.ExecuteNonQuery($"TRUNCATE TABLE {m_stagingTableName}");

        m_stagingTable.Rows.Clear();
    }

    /// <summary>
    /// Creates the session scoped staging table on first use.
    /// </summary>
    private void EnsureStagingTable()
    {
        if (m_stagingTableCreated)
            return;

        string columnList = string.Join(", ", m_stagingTable.Columns.Cast<DataColumn>().Select(column => column.ColumnName));

        // Deriving the staging table from the target guarantees identical column types without hard coding them
        m_context.ExecuteNonQuery($"SELECT TOP 0 {columnList} INTO {m_stagingTableName} FROM {m_targetTable}");
        m_stagingTableCreated = true;
    }

    /// <summary>
    /// Releases resources held by this <see cref="SqlServerBulkUpdate"/>.
    /// </summary>
    public void Dispose()
    {
        if (m_disposed)
            return;

        if (m_stagingTableCreated)
        {
            try
            {
                m_context.ExecuteNonQuery($"DROP TABLE {m_stagingTableName}");
            }
            catch
            {
                // Staging table is session scoped and will be reclaimed when the connection closes
            }
        }

        m_stagingTable.Dispose();
        m_disposed = true;
    }
}
