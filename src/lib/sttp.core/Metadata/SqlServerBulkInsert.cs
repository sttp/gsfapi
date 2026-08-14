//******************************************************************************************************
//  SqlServerBulkInsert.cs - Gbtc
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
//  08/13/2026 - J. Ritchie Carroll
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
/// Writes rows to a SQL Server table using <see cref="SqlBulkCopy"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is the fastest available insert path for SQL Server and is used for measurement records, which
/// dominate meta-data volume. Device and phasor records continue to use ordinary batched statements: their
/// volumes are orders of magnitude lower, and device inserts drive a trigger that maintains the
/// <c>Runtime</c> table, which the run-time views depend upon.
/// </para>
/// <para>
/// Triggers are explicitly enabled. This costs some throughput but is required for correctness - the
/// .NET Framework schema maintains change tracking through an insert trigger on <c>Measurement</c>, and
/// silently skipping it would leave the rest of the system unaware that configuration had changed. Because
/// the trigger is statement level, a single bulk copy fires it once rather than once per batch.
/// </para>
/// <para>
/// The staging <see cref="DataTable"/> takes its schema from the destination table itself rather than
/// assuming column types. The two supported schemas differ here: the .NET Framework schema stores signal IDs
/// as <c>uniqueidentifier</c> while the .NET schema stores them as <c>nvarchar(36)</c>, and
/// <see cref="SqlBulkCopy"/> is far less forgiving about type mismatches than a parameterized statement.
/// </para>
/// </remarks>
internal sealed class SqlServerBulkInsert : IDisposable
{
    private readonly MetadataSyncContext m_context;
    private readonly string m_tableName;
    private readonly DataTable m_stagingTable;
    private readonly int m_batchSize;
    private readonly bool m_keepIdentity;
    private bool m_disposed;

    /// <summary>
    /// Creates a new <see cref="SqlServerBulkInsert"/> for the given table and column set.
    /// </summary>
    /// <param name="context">Current synchronization context.</param>
    /// <param name="tableName">Destination table name.</param>
    /// <param name="columns">Columns that will be supplied, in the order values are added.</param>
    /// <param name="keepIdentity">Determines if supplied identity column values should be preserved.</param>
    public SqlServerBulkInsert(MetadataSyncContext context, string tableName, string[] columns, bool keepIdentity)
    {
        m_context = context;
        m_tableName = tableName;
        m_keepIdentity = keepIdentity;
        m_batchSize = context.BatchSize > 0 ? context.BatchSize : 10000;

        // Take the staging schema from the destination table so that column types match exactly
        m_stagingTable = context.RetrieveData($"SELECT TOP 0 {string.Join(", ", columns)} FROM {tableName}");
        m_stagingTable.TableName = tableName;
    }

    /// <summary>
    /// Gets the number of rows currently awaiting a write.
    /// </summary>
    public int PendingRows => m_stagingTable.Rows.Count;

    /// <summary>
    /// Adds a row to the batch, flushing if the batch is now full.
    /// </summary>
    /// <remarks>
    /// Values are coerced to the destination column type, which allows callers to supply a
    /// <see cref="Guid"/> without knowing whether the target column is a native unique identifier or text.
    /// </remarks>
    public void Add(params object?[] values)
    {
        if (values.Length != m_stagingTable.Columns.Count)
            throw new ArgumentException($"Expected {m_stagingTable.Columns.Count} values to match bulk insert column list, received {values.Length}.", nameof(values));

        DataRow row = m_stagingTable.NewRow();

        for (int i = 0; i < values.Length; i++)
            row[i] = Coerce(values[i], m_stagingTable.Columns[i].DataType);

        m_stagingTable.Rows.Add(row);

        if (m_stagingTable.Rows.Count >= m_batchSize)
            Flush();
    }

    /// <summary>
    /// Writes any accumulated rows to the database.
    /// </summary>
    public void Flush()
    {
        if (m_stagingTable.Rows.Count == 0)
            return;

        SqlBulkCopyOptions options = SqlBulkCopyOptions.FireTriggers;

        if (m_keepIdentity)
            options |= SqlBulkCopyOptions.KeepIdentity;

        using (SqlBulkCopy bulkCopy = new((SqlConnection)m_context.Database.Connection, options, (SqlTransaction?)m_context.Command.Transaction))
        {
            bulkCopy.DestinationTableName = m_tableName;
            bulkCopy.BulkCopyTimeout = m_context.Timeout;
            bulkCopy.BatchSize = m_batchSize;

            // Map by name so that column order in the staging table is irrelevant
            foreach (DataColumn column in m_stagingTable.Columns)
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);

            bulkCopy.WriteToServer(m_stagingTable);
        }

        m_context.StatementCount++;
        m_stagingTable.Rows.Clear();
    }

    /// <summary>
    /// Converts a value supplied by the caller to the type expected by the destination column.
    /// </summary>
    private static object Coerce(object? value, Type columnType)
    {
        if (value is null || value == DBNull.Value)
            return DBNull.Value;

        if (columnType == typeof(string) && value is Guid guidValue)
            return guidValue.ToString();

        if (columnType == typeof(Guid) && value is string stringValue)
            return Guid.Parse(stringValue);

        if (value.GetType() == columnType)
            return value;

        return Convert.ChangeType(value, columnType);
    }

    /// <summary>
    /// Releases resources held by this <see cref="SqlServerBulkInsert"/>.
    /// </summary>
    public void Dispose()
    {
        if (m_disposed)
            return;

        m_stagingTable.Dispose();
        m_disposed = true;
    }

    /// <summary>
    /// Determines whether the bulk insert path may be used against the connected database.
    /// </summary>
    /// <remarks>
    /// The optional audit log schema shipped with the .NET Framework applications defines triggers whose
    /// bodies assign from an arbitrary single row of the <c>inserted</c> pseudo-table, e.g.,
    /// <c>SELECT @id = CONVERT(NVARCHAR(MAX), SignalID) FROM #inserted</c>. Those triggers only produce
    /// correct output when exactly one row is affected, so multi-row writes are declined outright rather
    /// than silently recording misleading audit history.
    /// </remarks>
    public static bool IsSupported(MetadataSyncContext context, out string? declineReason)
    {
        declineReason = null;

        if (!context.Database.IsSQLServer)
        {
            declineReason = "database is not SQL Server";
            return false;
        }

        if (context.Database.Connection is not SqlConnection)
        {
            declineReason = "connection is not a SQL Server client connection";
            return false;
        }

        try
        {
            object? auditTriggerCount = context.ExecuteScalar(
                "SELECT COUNT(*) FROM sys.triggers t INNER JOIN sys.tables tb ON t.parent_id = tb.object_id " +
                "WHERE tb.name IN ('Device', 'Measurement', 'Phasor') AND t.is_disabled = 0 AND t.name LIKE '%Audit%'");

            if (auditTriggerCount is not null && auditTriggerCount != DBNull.Value && Convert.ToInt32(auditTriggerCount) > 0)
            {
                declineReason = "audit log triggers are installed, and they only record correct history for single row writes";
                return false;
            }
        }
        catch (Exception ex)
        {
            declineReason = $"audit trigger check failed: {ex.Message}";
            return false;
        }

        return true;
    }
}
