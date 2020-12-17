//******************************************************************************************************
//  Common.cs - Gbtc
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
//  05/24/2011 - J. Ritchie Carroll
//       Generated original version of source code.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//
//******************************************************************************************************

using System.Security.Cryptography;
using GSF.Threading;

#if !MONO
using Microsoft.Win32;
#endif

namespace sttp
{
    /// <summary>
    /// Common static methods and extensions for transport library.
    /// </summary>
    public static class Common
    {
        static Common()
        {
        #if MONO
            UseManagedEncryption = true;
        #else
            const string FipsKeyOld = "HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Control\\Lsa";
            const string FipsKeyNew = "HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Control\\Lsa\\FipsAlgorithmPolicy";

            // Determine if the operating system configuration to set to use FIPS-compliant algorithms
            UseManagedEncryption = (Registry.GetValue(FipsKeyNew, "Enabled", 0) ?? Registry.GetValue(FipsKeyOld, "FipsAlgorithmPolicy", 0)).ToString() == "0";
        #endif

            TimerScheduler = new SharedTimerScheduler();
        }

        /// <summary>
        /// Gets flag that determines if managed encryption should be used.
        /// </summary>
        public static bool UseManagedEncryption { get; }

        /// <summary>
        /// Gets an AES symmetric algorithm to use for encryption or decryption.
        /// </summary>
        public static SymmetricAlgorithm SymmetricAlgorithm
        {
            get
            {
                Aes symmetricAlgorithm = UseManagedEncryption ? (Aes)new AesManaged() : new AesCryptoServiceProvider();
                
                symmetricAlgorithm.KeySize = 256;
                
                return symmetricAlgorithm;
            }
        }

        internal static readonly SharedTimerScheduler TimerScheduler;
    }
}