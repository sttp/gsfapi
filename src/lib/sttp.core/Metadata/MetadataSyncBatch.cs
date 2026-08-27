//******************************************************************************************************
//  MetadataSyncBatch.cs - Gbtc
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
/// Accumulates rows for a multi-row <c>INSERT ... VALUES</c> statement, flushing automatically once the
/// configured batch size is reached.
/// </summary>
/// <remarks>
/// <para>
/// The row template uses <c>?</c> to mark each parameterized value, allowing literal values to be expressed
/// inline, e.g., <c>"?, ?, 0, 1"</c>. Placeholders are substituted with generated parameter names at flush
/// time.
/// </para>
/// <para>
/// Batched statements deliberately bypass the framework parameter helpers. Both frameworks re-parse the
/// statement text on every call to infer parameter names, and the .NET Framework tokenizer recognizes only
/// space, parenthesis, comma and equals as delimiters - a parameter adjacent to a semicolon or line break is
/// silently dropped, which would fail with a parameter count mismatch. Parameters are therefore constructed
/// directly against the command. See <see cref="MetadataSyncContext.ExecuteBatch"/>.
/// </para>
/// </remarks>
internal sealed class InsertBatch
{
    private readonly MetadataSyncContext m_context;
    private readonly string m_insertPrefix;
    private readonly string m_rowTemplate;
    private readonly int m_parametersPerRow;
    private readonly int m_batchSize;
    private readonly List<object?> m_values;
    private int m_rowCount;

    /// <summary>
    /// Creates a new <see cref="InsertBatch"/>.
    /// </summary>
    /// <param name="context">Current synchronization context.</param>
    /// <param name="insertPrefix">Statement text up to and including the column list, e.g., <c>"INSERT INTO Phasor(DeviceID, Label)"</c>.</param>
    /// <param name="rowTemplate">Single row value list using <c>?</c> for each parameterized value, e.g., <c>"?, ?, 0"</c>.</param>
    public InsertBatch(MetadataSyncContext context, string insertPrefix, string rowTemplate)
    {
        m_context = context;
        m_insertPrefix = insertPrefix;
        m_rowTemplate = rowTemplate;
        m_parametersPerRow = rowTemplate.Count(character => character == '?');
        m_batchSize = context.Dialect.GetBatchSize(m_parametersPerRow, context.Dialect.MaxRowsPerValuesClause, context.BatchSize);
        m_values = new List<object?>(m_parametersPerRow * m_batchSize);
    }

    /// <summary>
    /// Adds a row to the batch, flushing if the batch is now full.
    /// </summary>
    public void Add(params object?[] rowValues)
    {
        if (rowValues.Length != m_parametersPerRow)
            throw new ArgumentException($"Expected {m_parametersPerRow} values to match insert row template, received {rowValues.Length}.", nameof(rowValues));

        m_values.AddRange(rowValues);
        m_rowCount++;

        if (m_rowCount >= m_batchSize)
            Flush();
    }

    /// <summary>
    /// Writes any accumulated rows to the database.
    /// </summary>
    public void Flush()
    {
        if (m_rowCount == 0)
            return;

        StringBuilder sql = new(m_insertPrefix);
        sql.Append(" VALUES ");

        int parameterIndex = 0;

        for (int row = 0; row < m_rowCount; row++)
        {
            if (row > 0)
                sql.Append(", ");

            sql.Append('(');
            MetadataSyncContext.AppendRowTemplate(sql, m_rowTemplate, ref parameterIndex);
            sql.Append(')');
        }

        m_context.ExecuteBatch(sql.ToString(), m_values);

        m_values.Clear();
        m_rowCount = 0;
    }
}

/// <summary>
/// Accumulates repeated executions of a single statement, combining them into semicolon separated commands
/// where the database supports it.
/// </summary>
/// <remarks>
/// Used for the update and delete statement forms, which cannot be expressed as a multi-row value list. On
/// databases that do not support multi-statement commands the batch size collapses to one and the resulting
/// statement stream is identical to issuing each statement individually.
/// </remarks>
internal sealed class StatementBatch
{
    private readonly MetadataSyncContext m_context;
    private readonly string m_statementTemplate;
    private readonly int m_parametersPerStatement;
    private readonly int m_batchSize;
    private readonly List<object?> m_values;
    private int m_statementCount;

    /// <summary>
    /// Creates a new <see cref="StatementBatch"/>.
    /// </summary>
    /// <param name="context">Current synchronization context.</param>
    /// <param name="statementTemplate">Statement text using <c>?</c> for each parameterized value.</param>
    public StatementBatch(MetadataSyncContext context, string statementTemplate)
    {
        m_context = context;
        m_statementTemplate = statementTemplate;
        m_parametersPerStatement = statementTemplate.Count(character => character == '?');
        m_batchSize = context.Dialect.GetBatchSize(m_parametersPerStatement, context.Dialect.MaxStatementsPerCommand, context.BatchSize);
        m_values = new List<object?>(m_parametersPerStatement * m_batchSize);
    }

    /// <summary>
    /// Adds an execution of the statement to the batch, flushing if the batch is now full.
    /// </summary>
    public void Add(params object?[] values)
    {
        if (values.Length != m_parametersPerStatement)
            throw new ArgumentException($"Expected {m_parametersPerStatement} values to match statement template, received {values.Length}.", nameof(values));

        m_values.AddRange(values);
        m_statementCount++;

        if (m_statementCount >= m_batchSize)
            Flush();
    }

    /// <summary>
    /// Writes any accumulated statements to the database.
    /// </summary>
    public void Flush()
    {
        if (m_statementCount == 0)
            return;

        StringBuilder sql = new();
        int parameterIndex = 0;

        for (int statement = 0; statement < m_statementCount; statement++)
        {
            if (statement > 0)
                sql.Append("; ");

            MetadataSyncContext.AppendRowTemplate(sql, m_statementTemplate, ref parameterIndex);
        }

        m_context.ExecuteBatch(sql.ToString(), m_values);

        m_values.Clear();
        m_statementCount = 0;
    }
}
