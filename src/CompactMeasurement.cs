﻿//******************************************************************************************************
//  CompactMeasurement.cs - Gbtc
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
//  08/23/2010 - J. Ritchie Carroll
//       Generated original version of source code.
//  05/15/2011 - J. Ritchie Carroll
//       Added runtime size optimizations.
//  06/07/2011 - J. Ritchie Carroll
//       Implemented initialize issue fix as found and proposed by Luc Cezard.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//
//******************************************************************************************************

using System;
using GSF;
using GSF.Parsing;
using GSF.TimeSeries;
using GSF.TimeSeries.Transport;

namespace sttp
{
    #region [ Enumerations ]

    /// <summary>
    /// <see cref="CompactMeasurement"/> state flags.
    /// </summary>
    [Flags]
    internal enum CompactMeasurementStateFlags : byte
    {
        /// <summary>
        /// A data range flag was set.
        /// </summary>
        DataRange = (byte)Bits.Bit00,
        /// <summary>
        /// A data quality flag was set.
        /// </summary>
        DataQuality = (byte)Bits.Bit01,
        /// <summary>
        /// A time quality flag was set.
        /// </summary>
        TimeQuality = (byte)Bits.Bit02,
        /// <summary>
        /// A system flag was set.
        /// </summary>
        SystemIssue = (byte)Bits.Bit03,
        /// <summary>
        /// Calculated value bit was set.
        /// </summary>
        CalculatedValue = (byte)Bits.Bit04,
        /// <summary>
        /// Discarded value bit was set.
        /// </summary>
        DiscardedValue = (byte)Bits.Bit05,
        /// <summary>
        /// Compact measurement timestamp was serialized using base time offset when set.
        /// </summary>
        BaseTimeOffset = (byte)Bits.Bit06,
        /// <summary>
        /// Use odd time index (i.e., 1) when bit is set; even time index (i.e., 0) when bit is clear.
        /// </summary>
        TimeIndex = (byte)Bits.Bit07,
        /// <summary>
        /// No flags.
        /// </summary>
        NoFlags = (byte)Bits.Nil
    }

    /// <summary>
    /// Defines static methods for mapping between compact and full measurement states.
    /// </summary>
    internal static class MeasurementStateMappingExtensions
    {
        private const MeasurementStateFlags DataRangeMask = MeasurementStateFlags.OverRangeError | MeasurementStateFlags.UnderRangeError | MeasurementStateFlags.AlarmHigh | MeasurementStateFlags.AlarmLow | MeasurementStateFlags.WarningHigh | MeasurementStateFlags.WarningLow;
        private const MeasurementStateFlags DataQualityMask = MeasurementStateFlags.BadData | MeasurementStateFlags.SuspectData | MeasurementStateFlags.FlatlineAlarm | MeasurementStateFlags.ComparisonAlarm | MeasurementStateFlags.ROCAlarm | MeasurementStateFlags.ReceivedAsBad | MeasurementStateFlags.CalculationError | MeasurementStateFlags.CalculationWarning | MeasurementStateFlags.ReservedQualityFlag;
        private const MeasurementStateFlags TimeQualityMask = MeasurementStateFlags.BadTime | MeasurementStateFlags.SuspectTime | MeasurementStateFlags.LateTimeAlarm | MeasurementStateFlags.FutureTimeAlarm | MeasurementStateFlags.UpSampled | MeasurementStateFlags.DownSampled | MeasurementStateFlags.ReservedTimeFlag;
        private const MeasurementStateFlags SystemIssueMask = MeasurementStateFlags.SystemError | MeasurementStateFlags.SystemWarning | MeasurementStateFlags.MeasurementError;
        private const MeasurementStateFlags CalculatedValueMask = MeasurementStateFlags.CalculatedValue;
        private const MeasurementStateFlags DiscardedValueMask = MeasurementStateFlags.DiscardedValue;

        /// <summary>
        /// Maps <see cref="MeasurementStateFlags"/> to <see cref="CompactMeasurementStateFlags"/>.
        /// </summary>
        /// <param name="stateFlags">Flags to map.</param>
        /// <returns><see cref="CompactMeasurementStateFlags"/> mapped from <see cref="MeasurementStateFlags"/>.</returns>
        public static CompactMeasurementStateFlags MapToCompactFlags(this MeasurementStateFlags stateFlags)
        {
            CompactMeasurementStateFlags mappedStateFlags = CompactMeasurementStateFlags.NoFlags;

            if ((stateFlags & DataRangeMask) > 0)
                mappedStateFlags |= CompactMeasurementStateFlags.DataRange;

            if ((stateFlags & DataQualityMask) > 0)
                mappedStateFlags |= CompactMeasurementStateFlags.DataQuality;

            if ((stateFlags & TimeQualityMask) > 0)
                mappedStateFlags |= CompactMeasurementStateFlags.TimeQuality;

            if ((stateFlags & SystemIssueMask) > 0)
                mappedStateFlags |= CompactMeasurementStateFlags.SystemIssue;

            if ((stateFlags & CalculatedValueMask) > 0)
                mappedStateFlags |= CompactMeasurementStateFlags.CalculatedValue;

            if ((stateFlags & DiscardedValueMask) > 0)
                mappedStateFlags |= CompactMeasurementStateFlags.DiscardedValue;

            return mappedStateFlags;
        }

        /// <summary>
        /// Maps <see cref="CompactMeasurementStateFlags"/> to <see cref="MeasurementStateFlags"/>.
        /// </summary>
        /// <param name="stateFlags">Flags to map.</param>
        /// <returns><see cref="MeasurementStateFlags"/> mapped from <see cref="CompactMeasurementStateFlags"/>.</returns>
        public static MeasurementStateFlags MapToFullFlags(this CompactMeasurementStateFlags stateFlags)
        {
            MeasurementStateFlags mappedStateFlags = MeasurementStateFlags.Normal;

            if ((stateFlags & CompactMeasurementStateFlags.DataRange) > 0)
                mappedStateFlags |= DataRangeMask;

            if ((stateFlags & CompactMeasurementStateFlags.DataQuality) > 0)
                mappedStateFlags |= DataQualityMask;

            if ((stateFlags & CompactMeasurementStateFlags.TimeQuality) > 0)
                mappedStateFlags |= TimeQualityMask;

            if ((stateFlags & CompactMeasurementStateFlags.SystemIssue) > 0)
                mappedStateFlags |= SystemIssueMask;

            if ((stateFlags & CompactMeasurementStateFlags.CalculatedValue) > 0)
                mappedStateFlags |= CalculatedValueMask;

            if ((stateFlags & CompactMeasurementStateFlags.DiscardedValue) > 0)
                mappedStateFlags |= DiscardedValueMask;

            return mappedStateFlags;
        }
    }

    #endregion

    /// <summary>
    /// Represents a <see cref="IMeasurement"/> that can be serialized with minimal size.
    /// </summary>
    /// <remarks>
    /// This measurement implementation is serialized through <see cref="ISupportBinaryImage"/>
    /// to allow complete control of binary format. Only critical measurements properties are
    /// serialized and every attempt is made to optimize the binary image for purposes of size
    /// reduction.
    /// </remarks>
    public class CompactMeasurement : Measurement, IBinaryMeasurement
    {
        #region [ Members ]

        // Constants

        /// <summary>
        /// Fixed byte length of a <see cref="CompactMeasurement"/>.
        /// </summary>
        public const int FixedLength = 9;

        // Members
        private readonly SignalIndexCache m_signalIndexCache;
        private readonly long[] m_baseTimeOffsets;
        private int m_timeIndex;
        private readonly bool m_useMillisecondResolution;
        private bool m_usingBaseTimeOffset;
        private readonly bool m_includeTime;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Creates a new <see cref="CompactMeasurement"/>.
        /// </summary>
        /// <param name="signalIndexCache">Signal index cache used to serialize or deserialize runtime information.</param>
        /// <param name="includeTime">Set to <c>true</c> to include time in serialized packet; otherwise <c>false</c>.</param>
        /// <param name="baseTimeOffsets">Base time offset array - set to <c>null</c> to use full fidelity measurement time.</param>
        /// <param name="timeIndex">Time index to use for base offset.</param>
        /// <param name="useMillisecondResolution">Flag that determines if millisecond resolution is in use for this serialization.</param>
        public CompactMeasurement(SignalIndexCache signalIndexCache, bool includeTime = true, long[] baseTimeOffsets = null, int timeIndex = 0, bool useMillisecondResolution = false)
        {
            m_signalIndexCache = signalIndexCache;
            m_includeTime = includeTime;

            // We keep a clone of the base time offsets, if provided, since array contents can change at any time
            if ((object)baseTimeOffsets == null)
                m_baseTimeOffsets = s_emptyBaseTimeOffsets;
            else
                m_baseTimeOffsets = new[] { baseTimeOffsets[0], baseTimeOffsets[1] };

            m_timeIndex = timeIndex;
            m_useMillisecondResolution = useMillisecondResolution;
        }

        /// <summary>
        /// Creates a new <see cref="CompactMeasurement"/> from an existing <see cref="IMeasurement"/> value.
        /// </summary>
        /// <param name="measurement">Source <see cref="IMeasurement"/> value.</param>
        /// <param name="signalIndexCache">Signal index cache used to serialize or deserialize runtime information.</param>
        /// <param name="includeTime">Set to <c>true</c> to include time in serialized packet; otherwise <c>false</c>.</param>
        /// <param name="baseTimeOffsets">Base time offset array - set to <c>null</c> to use full fidelity measurement time.</param>
        /// <param name="timeIndex">Time index to use for base offset.</param>
        /// <param name="useMillisecondResolution">Flag that determines if millisecond resolution is in use for this serialization.</param>
        public CompactMeasurement(IMeasurement measurement, SignalIndexCache signalIndexCache, bool includeTime = true, long[] baseTimeOffsets = null, int timeIndex = 0, bool useMillisecondResolution = false)
        {
            Metadata = measurement.Metadata;
            Value = measurement.Value;
            Timestamp = measurement.Timestamp;
            StateFlags = measurement.StateFlags;

            m_signalIndexCache = signalIndexCache;
            m_includeTime = includeTime;

            // We keep a clone of the base time offsets, if provided, since array contents can change at any time
            if ((object)baseTimeOffsets == null)
                m_baseTimeOffsets = s_emptyBaseTimeOffsets;
            else
                m_baseTimeOffsets = new[] { baseTimeOffsets[0], baseTimeOffsets[1] };

            m_timeIndex = timeIndex;
            m_useMillisecondResolution = useMillisecondResolution;
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets flag that determines if time is serialized into measurement binary image.
        /// </summary>
        public bool IncludeTime
        {
            get
            {
                return m_includeTime;
            }
        }

        /// <summary>
        /// Gets the length of the <see cref="CompactMeasurement"/>.
        /// </summary>
        public int BinaryLength
        {
            get
            {
                int length = FixedLength;

                if (m_includeTime)
                {
                    long baseTimeOffset = m_baseTimeOffsets[m_timeIndex];

                    if (baseTimeOffset > 0)
                    {
                        // See if timestamp will fit within space allowed for active base offset. We cache result so that post call
                        // to binary length, result will speed other subsequent parsing operations by not having to reevaluate.
                        long difference = (long)Timestamp - m_baseTimeOffsets[m_timeIndex];

                        if (difference > 0)
                            m_usingBaseTimeOffset = m_useMillisecondResolution ? difference / Ticks.PerMillisecond < ushort.MaxValue : difference < uint.MaxValue;
                        else
                            m_usingBaseTimeOffset = false;

                        if (m_usingBaseTimeOffset)
                        {
                            if (m_useMillisecondResolution)
                                length += 2;    // Use two bytes for millisecond resolution timestamp with valid offset
                            else
                                length += 4;    // Use four bytes for tick resolution timestamp with valid offset
                        }
                        else
                        {
                            // Use eight bytes for full fidelity time
                            length += 8;
                        }
                    }
                    else
                    {
                        // Use eight bytes for full fidelity time
                        length += 8;
                    }
                }

                return length;
            }
        }

        /// <summary>
        /// Gets offset compressed millisecond-resolution 2-byte timestamp.
        /// </summary>
        public ushort TimestampC2
        {
            get
            {
                return (ushort)(Timestamp - m_baseTimeOffsets[m_timeIndex]).ToMilliseconds();
            }
        }

        /// <summary>
        /// Gets offset compressed tick-resolution 4-byte timestamp.
        /// </summary>
        public uint TimestampC4
        {
            get
            {
                return (uint)((long)Timestamp - m_baseTimeOffsets[m_timeIndex]);
            }
        }

        /// <summary>
        /// Gets or sets byte level compact state flags with encoded time index and base time offset bits.
        /// </summary>
        public byte CompactStateFlags
        {
            get
            {
                // Encode compact state flags
                CompactMeasurementStateFlags flags = StateFlags.MapToCompactFlags();

                if (m_timeIndex != 0)
                    flags |= CompactMeasurementStateFlags.TimeIndex;

                if (m_usingBaseTimeOffset)
                    flags |= CompactMeasurementStateFlags.BaseTimeOffset;

                return (byte)flags;
            }
            set
            {
                // Decode compact state flags
                CompactMeasurementStateFlags flags = (CompactMeasurementStateFlags)value;
                StateFlags = flags.MapToFullFlags();
                m_timeIndex = (flags & CompactMeasurementStateFlags.TimeIndex) > 0 ? 1 : 0;
                m_usingBaseTimeOffset = (flags & CompactMeasurementStateFlags.BaseTimeOffset) > 0;
            }
        }

        /// <summary>
        /// Gets or sets the 4-byte run-time signal index for this measurement.
        /// </summary>
        public int RuntimeID
        {
            get
            {
                return m_signalIndexCache.GetSignalIndex(Key);
            }
            set
            {
                // Attempt to restore signal identification
                MeasurementKey key;

                if (m_signalIndexCache.Reference.TryGetValue(value, out key))
                {
                    Metadata = key.Metadata;
                }
                else
                    throw new InvalidOperationException("Failed to find associated signal identification for runtime ID " + value);
            }
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Initializes <see cref="CompactMeasurement"/> from the specified binary image.
        /// </summary>
        /// <param name="buffer">Buffer containing binary image to parse.</param>
        /// <param name="startIndex">0-based starting index in the <paramref name="buffer"/> to start parsing.</param>
        /// <param name="length">Valid number of bytes within <paramref name="buffer"/> from <paramref name="startIndex"/>.</param>
        /// <returns>The number of bytes used for initialization in the <paramref name="buffer"/> (i.e., the number of bytes parsed).</returns>
        /// <exception cref="InvalidOperationException">Not enough buffer available to deserialize measurement.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="startIndex"/> or <paramref name="length"/> is less than 0 -or- 
        /// <paramref name="startIndex"/> and <paramref name="length"/> will exceed <paramref name="buffer"/> length.
        /// </exception>
        public int ParseBinaryImage(byte[] buffer, int startIndex, int length)
        {
            buffer.ValidateParameters(startIndex, length);

            if (length < 1)
                throw new InvalidOperationException("Not enough buffer available to deserialize measurement.");

            int index = startIndex;

            // Decode state flags
            CompactStateFlags = buffer[index++];

            // Decode runtime ID
            RuntimeID = BigEndian.ToInt32(buffer, index);
            index += 4;

            // Decode value
            Value = BigEndian.ToSingle(buffer, index);
            index += 4;

            if (m_includeTime)
            {
                if (m_usingBaseTimeOffset)
                {
                    long baseTimeOffset = m_baseTimeOffsets[m_timeIndex];

                    if (m_useMillisecondResolution)
                    {
                        // Decode 2-byte millisecond offset timestamp
                        if (baseTimeOffset > 0)
                            Timestamp = baseTimeOffset + BigEndian.ToUInt16(buffer, index) * Ticks.PerMillisecond;

                        index += 2;
                    }
                    else
                    {
                        // Decode 4-byte tick offset timestamp
                        if (baseTimeOffset > 0)
                            Timestamp = baseTimeOffset + BigEndian.ToUInt32(buffer, index);

                        index += 4;
                    }
                }
                else
                {
                    // Decode 8-byte full fidelity timestamp
                    Timestamp = BigEndian.ToInt64(buffer, index);
                    index += 8;
                }
            }

            return index - startIndex;
        }

        /// <summary>
        /// Generates binary image of the <see cref="CompactMeasurement"/> and copies it into the given buffer, for <see cref="ISupportBinaryImage.BinaryLength"/> bytes.
        /// </summary>
        /// <param name="buffer">Buffer used to hold generated binary image of the source object.</param>
        /// <param name="startIndex">0-based starting index in the <paramref name="buffer"/> to start writing.</param>
        /// <returns>The number of bytes written to the <paramref name="buffer"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="startIndex"/> or <see cref="ISupportBinaryImage.BinaryLength"/> is less than 0 -or- 
        /// <paramref name="startIndex"/> and <see cref="ISupportBinaryImage.BinaryLength"/> will exceed <paramref name="buffer"/> length.
        /// </exception>
        /// <remarks>
        /// <para>
        /// Field:     Bytes: <br/>
        /// --------   -------<br/>
        ///  Flags        1   <br/>
        ///   ID          4   <br/>
        ///  Value        4   <br/>
        ///  [Time]       2?  <br/>
        /// </para>
        /// <para>
        /// Constant Length = 9<br/>
        /// Variable Length = 0, 2, 4 or 8 (i.e., total size is 9, 11, 13 or 17)
        /// </para>
        /// </remarks>
        public int GenerateBinaryImage(byte[] buffer, int startIndex)
        {
            // Call to binary length property caches result of m_usingBaseTimeOffset
            int length = BinaryLength;

            buffer.ValidateParameters(startIndex, length);

            // Added encoded compact state flags to beginning of buffer
            buffer[startIndex++] = CompactStateFlags;

            // Encode runtime ID
            startIndex += BigEndian.CopyBytes(RuntimeID, buffer, startIndex);

            // Encode adjusted value (accounts for adder and multiplier)
            startIndex += BigEndian.CopyBytes((float)AdjustedValue, buffer, startIndex);

            if (m_includeTime)
            {
                if (m_usingBaseTimeOffset)
                {
                    if (m_useMillisecondResolution)
                    {
                        // Encode 2-byte millisecond offset timestamp
                        BigEndian.CopyBytes(TimestampC2, buffer, startIndex);
                    }
                    else
                    {
                        // Encode 4-byte ticks offset timestamp
                        BigEndian.CopyBytes(TimestampC4, buffer, startIndex);
                    }
                }
                else
                {
                    // Encode 8-byte full fidelity timestamp
                    BigEndian.CopyBytes((long)Timestamp, buffer, startIndex);
                }
            }

            return length;
        }

        #endregion

        #region [ Static ]

        // Static Fields
        private static readonly long[] s_emptyBaseTimeOffsets = new long[] { 0, 0 };

        #endregion
    }
}