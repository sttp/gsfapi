//******************************************************************************************************
//  MetadataSyncDialect.cs - Gbtc
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

namespace sttp;

/// <summary>
/// Describes the statement batching capabilities and limits of a particular database type.
/// </summary>
/// <remarks>
/// <para>
/// Meta-data synchronization issues large numbers of very similar statements. Combining them into fewer
/// commands is the single largest remaining cost reduction, but the safe combining strategy and its limits
/// vary by database. This type isolates that variation so the per-table synchronization operations can stay
/// database agnostic.
/// </para>
/// <para>
/// Databases that support neither multi-row value lists nor multi-statement commands - Oracle in particular -
/// simply report a batch size of one, which produces exactly the same statement stream as before.
/// </para>
/// </remarks>
internal class MetadataSyncDialect
{
    #region [ Properties ]

    /// <summary>
    /// Gets the maximum number of parameters permitted in a single command.
    /// </summary>
    public virtual int MaxParametersPerCommand => 999;

    /// <summary>
    /// Gets the maximum number of rows permitted in a single <c>INSERT ... VALUES</c> statement.
    /// </summary>
    public virtual int MaxRowsPerValuesClause => 1;

    /// <summary>
    /// Gets the maximum number of statements that may be combined into a single command.
    /// </summary>
    public virtual int MaxStatementsPerCommand => 1;

    /// <summary>
    /// Gets flag that determines if the database supports multi-row <c>INSERT ... VALUES</c> syntax.
    /// </summary>
    public bool SupportsMultiRowValues => MaxRowsPerValuesClause > 1;

    /// <summary>
    /// Gets flag that determines if the database supports semicolon separated multi-statement commands.
    /// </summary>
    public bool SupportsMultiStatementCommands => MaxStatementsPerCommand > 1;

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Determines how many rows of a given width may be combined into a single command.
    /// </summary>
    /// <param name="parametersPerRow">Number of parameters required by a single row.</param>
    /// <param name="maxRows">Batching limit reported by this dialect for the statement form in use.</param>
    /// <param name="requestedBatchSize">User configured batch size, where zero selects the dialect default.</param>
    public int GetBatchSize(int parametersPerRow, int maxRows, int requestedBatchSize)
    {
        if (parametersPerRow < 1)
            parametersPerRow = 1;

        int batchSize = Math.Min(maxRows, MaxParametersPerCommand / parametersPerRow);

        if (requestedBatchSize > 0)
            batchSize = Math.Min(batchSize, requestedBatchSize);

        return Math.Max(1, batchSize);
    }

    #endregion

    #region [ Static ]

    /// <summary>
    /// Creates the <see cref="MetadataSyncDialect"/> appropriate for the supplied database connection.
    /// </summary>
    public static MetadataSyncDialect Create(AdoDataConnection database)
    {
        return database.DatabaseType switch
        {
            DatabaseType.SQLServer => new SqlServerMetadataSyncDialect(),
            DatabaseType.PostgreSQL => new PostgreSqlMetadataSyncDialect(),
            DatabaseType.SQLite => new SqliteMetadataSyncDialect(),
            DatabaseType.MySQL => new MySqlMetadataSyncDialect(),

            // Oracle supports neither multi-row value lists nor semicolon separated commands without wrapping
            // statements in an anonymous PL/SQL block, and Access supports neither at all - both fall back to
            // the unbatched behavior provided by this base implementation
            _ => new MetadataSyncDialect()
        };
    }

    #endregion
}

/// <summary>
/// Statement batching limits for SQL Server.
/// </summary>
internal sealed class SqlServerMetadataSyncDialect : MetadataSyncDialect
{
    /// <summary>
    /// SQL Server permits a hard maximum of 2,100 parameters per command.
    /// </summary>
    public override int MaxParametersPerCommand => 2000;

    /// <summary>
    /// SQL Server permits a hard maximum of 1,000 rows per <c>INSERT ... VALUES</c> statement.
    /// </summary>
    public override int MaxRowsPerValuesClause => 1000;

    /// <inheritdoc/>
    public override int MaxStatementsPerCommand => 250;
}

/// <summary>
/// Statement batching limits for PostgreSQL.
/// </summary>
internal sealed class PostgreSqlMetadataSyncDialect : MetadataSyncDialect
{
    /// <summary>
    /// PostgreSQL permits 65,535 parameters per command; a lower value is used to keep individual
    /// commands small enough to parse and plan quickly.
    /// </summary>
    public override int MaxParametersPerCommand => 8000;

    /// <inheritdoc/>
    public override int MaxRowsPerValuesClause => 1000;

    /// <inheritdoc/>
    public override int MaxStatementsPerCommand => 250;
}

/// <summary>
/// Statement batching limits for SQLite.
/// </summary>
/// <remarks>
/// SQLite benefits from batching more than any other supported database. Its per-row insert triggers issue
/// follow-up update statements against the same table, and without an enclosing transaction every statement
/// is separately committed to disk.
/// </remarks>
internal sealed class SqliteMetadataSyncDialect : MetadataSyncDialect
{
    /// <summary>
    /// Recent SQLite builds permit 32,766 parameters, but builds compiled with the older default permit only
    /// 999, so the conservative limit is used.
    /// </summary>
    public override int MaxParametersPerCommand => 900;

    /// <inheritdoc/>
    public override int MaxRowsPerValuesClause => 500;

    /// <inheritdoc/>
    public override int MaxStatementsPerCommand => 250;
}

/// <summary>
/// Statement batching limits for MySQL and MariaDB.
/// </summary>
internal sealed class MySqlMetadataSyncDialect : MetadataSyncDialect
{
    /// <inheritdoc/>
    public override int MaxParametersPerCommand => 8000;

    /// <inheritdoc/>
    public override int MaxRowsPerValuesClause => 1000;

    /// <summary>
    /// Multi-statement commands are rejected unless the connection was opened with the option enabled, so
    /// only value list batching is used.
    /// </summary>
    public override int MaxStatementsPerCommand => 1;
}
