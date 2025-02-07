//******************************************************************************************************
//  SignalIndexCache.cs - Gbtc
//
//  Copyright © 2012, Grid Protection Alliance.  All Rights Reserved.
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
//  05/15/2011 - J. Ritchie Carroll
//       Generated original version of source code.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//
//******************************************************************************************************

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using GSF;
using GSF.Collections;
using GSF.Parsing;
using GSF.TimeSeries;
using sttp.tssc;

namespace sttp
{
    /// <summary>
    /// Represents a serializable <see cref="Guid"/> signal ID to <see cref="int"/> index cross-reference.
    /// </summary>
    /// <remarks>
    /// This class is used to create a runtime index to be used for data exchange so that a 32-bit integer
    /// is exchanged in the data packets for signal identification instead of the 128-bit Guid signal ID
    /// to reduce bandwidth required for signal exchange. This means the total number of unique signal
    /// IDs that could be exchanged using this method in a single session is 2,147,483,647*. This number
    /// seems reasonable for the currently envisioned use cases, however, multiple sessions each with
    /// their own runtime signal index cache could be established if this is a limitation for a given
    /// data set.
    /// <para>
    /// * This maximum value is for a signed integer - the value is large enough that even approaching
    /// this value would swamp most systems; note that most all collections defined in .NET also use a
    /// signed integer. That said, this could easily move to an unsigned integer in the future as systems
    /// move towards the ability to manage in-memory collections of this magnitude. In this case the
    /// upper limit would increase to 4,294,967,295.
    /// </para>
    /// </remarks>
    [Serializable]
    public class SignalIndexCache : ISupportBinaryImage
    {
        #region [ Members ]

        // Fields
        private Guid m_subscriberID;

        // Since measurement keys are statically cached as a global system optimization and the keys
        // can be different between two parties exchanging data, the raw measurement key elements are
        // cached and exchanged instead of actual measurement key values
        private ConcurrentDictionary<int, MeasurementKey> m_reference;
        private Guid[] m_unauthorizedSignalIDs;

        /// <summary>
        /// Lookups MeasurementKey.RuntimeID and returns int SignalIndex. -1 means it does not exist.
        /// </summary>
        [NonSerialized] // SignalID reverse lookup runtime cache (used to speed deserialization)
        private IndexedArray<int> m_signalIDCache;

        [NonSerialized]
        private Encoding m_encoding;

        [NonSerialized]
        internal TsscDecoder TsscDecoder;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Creates a new <see cref="SignalIndexCache"/> instance.
        /// </summary>
        public SignalIndexCache()
        {
            m_reference = new ConcurrentDictionary<int, MeasurementKey>();
            m_signalIDCache = new IndexedArray<int>(-1);
        }

        /// <summary>
        /// Creates a new local system cache from one that was received remotely.
        /// </summary>
        /// <param name="dataSource"><see cref="DataSet"/> based data source used to interpret local measurement keys.</param>
        /// <param name="remoteCache">Deserialized remote signal index cache.</param>
        public SignalIndexCache(DataSet dataSource, SignalIndexCache remoteCache)
        {
            m_subscriberID = remoteCache.SubscriberID;

            // If active measurements are defined, interpret signal cache in context of current measurement key definitions
            if (dataSource is null || !dataSource.Tables.Contains("ActiveMeasurements"))
            {
                // Just use remote signal index cache as-is if no local configuration exists
                Reference = remoteCache.Reference;
                m_unauthorizedSignalIDs = remoteCache.UnauthorizedSignalIDs;
            }
            else
            {
                DataTable activeMeasurements = dataSource.Tables["ActiveMeasurements"];
                ConcurrentDictionary<int, MeasurementKey> reference = new();

                foreach (KeyValuePair<int, MeasurementKey> signalIndex in remoteCache.Reference)
                {
                    Guid signalID = signalIndex.Value.SignalID;
                    DataRow[] filteredRows = activeMeasurements.Select($"SignalID = '{signalID}'");

                    if (filteredRows.Length == 0)
                        continue;

                    DataRow row = filteredRows[0];
                    MeasurementKey key = MeasurementKey.LookUpOrCreate(signalID, row["ID"].ToNonNullString(MeasurementKey.Undefined.ToString()));
                    reference.TryAdd(signalIndex.Key, key);
                }

                m_unauthorizedSignalIDs = remoteCache.UnauthorizedSignalIDs;
                Reference = reference;
            }
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets or sets the <see cref="Guid"/> based subscriber ID of this <see cref="SignalIndexCache"/>.
        /// </summary>
        public Guid SubscriberID
        {
            get => m_subscriberID;
            set => m_subscriberID = value;
        }

        /// <summary>
        /// Gets the total number of times the signal index cache has been refreshed.
        /// </summary>
        public long RefreshCount { get; private set; }

        /// <summary>
        /// Gets or sets integer signal index cross-reference dictionary.
        /// </summary>
        public ConcurrentDictionary<int, MeasurementKey> Reference
        {
            get => m_reference;
            set
            {
                m_reference = value;
                IndexedArray<int> signalIDCache = new(-1);

                foreach (KeyValuePair<int, MeasurementKey> pair in value)
                    signalIDCache[pair.Value.RuntimeID] = pair.Key;

                m_signalIDCache = signalIDCache;
                RefreshCount++;
            }
        }

        /// <summary>
        /// Gets reference to array of requested input measurement signal IDs that were authorized.
        /// </summary>
        public Guid[] AuthorizedSignalIDs => m_reference.Select(kvp => kvp.Value.SignalID).ToArray();

        /// <summary>
        /// Gets or sets reference to array of requested input measurement signal IDs that were unauthorized.
        /// </summary>
        public Guid[] UnauthorizedSignalIDs
        {
            get => m_unauthorizedSignalIDs;
            set => m_unauthorizedSignalIDs = value;
        }

        /// <summary>
        /// Gets the current maximum integer signal index.
        /// </summary>
        public int MaximumIndex => m_reference.Count == 0 ? 0 : m_reference.Max(kvp => kvp.Key) + 1;

        /// <summary>
        /// Gets or sets character encoding used to convert strings to binary.
        /// </summary>
        public Encoding Encoding
        {
            get => m_encoding;
            set => m_encoding = value;
        }

        /// <summary>
        /// Gets the length of the binary image.
        /// </summary>
        public int BinaryLength
        {
            get
            {
                if (m_encoding is null)
                    throw new InvalidOperationException("Attempt to get binary length of signal index cache without setting a character encoding.");

                // Byte size of cache
                int binaryLength = 4;

                // Subscriber ID
                binaryLength += 16;

                // Number of references
                binaryLength += 4;

                // Each reference                    index id  len source                                     key.id
                binaryLength += m_reference.Sum(kvp => 4 + 16 + 4 + m_encoding.GetByteCount(kvp.Value.Source) + 8);

                // Number of unauthorized IDs
                binaryLength += 4;

                // Each unauthorized ID
                binaryLength += 16 * (m_unauthorizedSignalIDs ?? []).Length;

                return binaryLength;
            }
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Gets runtime signal index for given <see cref="Guid"/> signal ID.
        /// </summary>
        /// <param name="key">The <see cref="MeasurementKey"/> used to lookup associated runtime signal index.</param>
        /// <returns>Runtime signal index for given <see cref="MeasurementKey"/> <paramref name="key"/>.</returns>
        public int GetSignalIndex(MeasurementKey key)
        {
            int value = m_signalIDCache[key.RuntimeID];
            return value < 0 ? int.MaxValue : value;
        }

        /// <summary>
        /// Generates binary image of the <see cref="SignalIndexCache"/> and copies it into the given buffer, for <see cref="BinaryLength"/> bytes.
        /// </summary>
        /// <param name="buffer">Buffer used to hold generated binary image of the source object.</param>
        /// <param name="startIndex">0-based starting index in the <paramref name="buffer"/> to start writing.</param>
        /// <returns>The number of bytes written to the <paramref name="buffer"/>.</returns>
        public int GenerateBinaryImage(byte[] buffer, int startIndex)
        {
            if (m_encoding is null)
                throw new InvalidOperationException("Attempt to generate binary image of signal index cache without setting a character encoding.");

            Guid[] unauthorizedSignalIDs = m_unauthorizedSignalIDs ?? [];
            int binaryLength = BinaryLength;
            int offset = startIndex;

            buffer.ValidateParameters(startIndex, binaryLength);

            // Byte size of cache
            byte[] bigEndianBuffer = BigEndian.GetBytes(binaryLength);
            Buffer.BlockCopy(bigEndianBuffer, 0, buffer, offset, bigEndianBuffer.Length);
            offset += bigEndianBuffer.Length;

            // Subscriber ID
            bigEndianBuffer = m_subscriberID.ToRfcBytes();
            Buffer.BlockCopy(bigEndianBuffer, 0, buffer, offset, bigEndianBuffer.Length);
            offset += bigEndianBuffer.Length;

            // Number of references
            bigEndianBuffer = BigEndian.GetBytes(m_reference.Count);
            Buffer.BlockCopy(bigEndianBuffer, 0, buffer, offset, bigEndianBuffer.Length);
            offset += bigEndianBuffer.Length;

            foreach (KeyValuePair<int, MeasurementKey> kvp in m_reference)
            {
                // Signal index
                bigEndianBuffer = BigEndian.GetBytes(kvp.Key);
                Buffer.BlockCopy(bigEndianBuffer, 0, buffer, offset, bigEndianBuffer.Length);
                offset += bigEndianBuffer.Length;

                // Signal ID
                bigEndianBuffer = kvp.Value.SignalID.ToRfcBytes();
                Buffer.BlockCopy(bigEndianBuffer, 0, buffer, offset, bigEndianBuffer.Length);
                offset += bigEndianBuffer.Length;

                // Source
                byte[] unicodeBuffer = m_encoding.GetBytes(kvp.Value.Source);
                bigEndianBuffer = BigEndian.GetBytes(unicodeBuffer.Length);
                Buffer.BlockCopy(bigEndianBuffer, 0, buffer, offset, bigEndianBuffer.Length);
                offset += bigEndianBuffer.Length;
                Buffer.BlockCopy(unicodeBuffer, 0, buffer, offset, unicodeBuffer.Length);
                offset += unicodeBuffer.Length;

                // ID
                bigEndianBuffer = BigEndian.GetBytes(kvp.Value.ID);
                Buffer.BlockCopy(bigEndianBuffer, 0, buffer, offset, bigEndianBuffer.Length);
                offset += bigEndianBuffer.Length;
            }

            // Number of unauthorized IDs
            bigEndianBuffer = BigEndian.GetBytes(unauthorizedSignalIDs.Length);
            Buffer.BlockCopy(bigEndianBuffer, 0, buffer, offset, bigEndianBuffer.Length);
            offset += bigEndianBuffer.Length;

            foreach (Guid signalID in unauthorizedSignalIDs)
            {
                // Unauthorized ID
                bigEndianBuffer = signalID.ToRfcBytes();
                Buffer.BlockCopy(bigEndianBuffer, 0, buffer, offset, bigEndianBuffer.Length);
                offset += bigEndianBuffer.Length;
            }

            return binaryLength;
        }

        /// <summary>
        /// Initializes the <see cref="SignalIndexCache"/> by parsing the specified <paramref name="buffer"/> containing a binary image.
        /// </summary>
        /// <param name="buffer">Buffer containing binary image to parse.</param>
        /// <param name="startIndex">0-based starting index in the <paramref name="buffer"/> to start parsing.</param>
        /// <param name="length">Valid number of bytes within <paramref name="buffer"/> to read from <paramref name="startIndex"/>.</param>
        /// <returns>The number of bytes used for initialization in the <paramref name="buffer"/> (i.e., the number of bytes parsed).</returns>
        public int ParseBinaryImage(byte[] buffer, int startIndex, int length)
        {
            if (m_encoding is null)
                throw new InvalidOperationException("Attempt to parse binary image of signal index cache without setting a character encoding.");

            buffer.ValidateParameters(startIndex, length);

            if (length < 4)
                return 0;

            int offset = startIndex;

            // Byte size of cache
            int binaryLength = BigEndian.ToInt32(buffer, offset);
            offset += 4;

            if (length < binaryLength)
                return 0;

            // We know we have enough data so we can empty the reference cache
            m_reference.Clear();

            // Subscriber ID
            m_subscriberID = buffer.ToRfcGuid(offset);
            offset += 16;

            // Number of references
            int referenceCount = BigEndian.ToInt32(buffer, offset);
            offset += 4;

            for (int i = 0; i < referenceCount; i++)
            {
                // Signal index
                int signalIndex = BigEndian.ToInt32(buffer, offset);
                offset += 4;

                // Signal ID
                Guid signalID = buffer.ToRfcGuid(offset);
                offset += 16;

                // Source
                int sourceSize = BigEndian.ToInt32(buffer, offset);
                offset += 4;

                string source = m_encoding.GetString(buffer, offset, sourceSize);
                offset += sourceSize;

                // ID
                ulong id = BigEndian.ToUInt64(buffer, offset);
                offset += 8;

                m_reference[signalIndex] = MeasurementKey.LookUpOrCreate(signalID, source, id);
            }

            // Number of unauthorized IDs
            int unauthorizedIDCount = BigEndian.ToInt32(buffer, offset);
            m_unauthorizedSignalIDs = new Guid[unauthorizedIDCount];
            offset += 4;

            for (int i = 0; i < unauthorizedIDCount; i++)
            {
                // Unauthorized ID
                m_unauthorizedSignalIDs[i] = buffer.ToRfcGuid(offset);
                offset += 16;
            }

            return binaryLength;
        }

        #endregion
    }
}
