/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.IO;
using System.Text;

namespace org.apache.jute
{

/**
 * Various utility functions for Hadoop record I/O runtime.
 */
public class Utils {
    
    /** Cannot create a new instance of Utils */
    /*
    private Utils() {
        super();
    }
     */
   
    /**
     * equals function that actually compares two buffers.
     *
     * @param onearray First buffer
     * @param twoarray Second buffer
     * @return true if one and two contain exactly the same content, else false.
     */
    public static bool bufEquals(byte[] onearray, byte[] twoarray) {
    	if (onearray == twoarray) return true;
        bool ret = (onearray.Length == twoarray.Length);
        if (!ret) {
            return ret;
        }
        for (int idx = 0; idx < onearray.Length; idx++) {
            if (onearray[idx] != twoarray[idx]) {
                return false;
            }
        }
        return true;
    }
    
    private static readonly char[] hexchars = { '0', '1', '2', '3', '4', '5',
                                            '6', '7', '8', '9', 'A', 'B',
                                            'C', 'D', 'E', 'F' };
    /**
     * 
     * @param s 
     * @return 
     */
    static string toXMLstring(string s) {
        if (s == null)
            return "";

        StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < s.Length; idx++) {
          char ch = s[idx];
          if (ch == '<') {
            sb.Append("&lt;");
          } else if (ch == '&') {
            sb.Append("&amp;");
          } else if (ch == '%') {
            sb.Append("%25");
          } else if (ch < 0x20) {
            sb.Append("%");
            sb.Append(hexchars[ch/16]);
            sb.Append(hexchars[ch%16]);
          } else {
            sb.Append(ch);
          }
        }
        return sb.ToString();
    }
    
    static private int h2c(char ch) {
      if (ch >= '0' && ch <= '9') {
        return ch - '0';
      } else if (ch >= 'A' && ch <= 'F') {
        return ch - 'A';
      } else if (ch >= 'a' && ch <= 'f') {
        return ch - 'a';
      }
      return 0;
    }
    
    /**
     * 
     * @param s 
     * @return 
     */
    static string fromXMLstring(string s) {
        StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < s.Length;) {
          char ch = s[idx++];
          if (ch == '%') {
            char ch1 = s[idx++];
            char ch2 = s[idx++];
            char res = (char)(h2c(ch1)*16 + h2c(ch2));
            sb.Append(res);
          } else {
            sb.Append(ch);
          }
        }
        
        return sb.ToString();
    }
    
    /**
     * 
     * @param s 
     * @return 
     */
    public static string toCSVstring(string s) {
        if (s == null)
            return "";

        StringBuilder sb = new StringBuilder(s.Length+1);
        sb.Append('\'');
        int len = s.Length;
        for (int i = 0; i < len; i++) {
            char c = s[i];
            switch(c) {
                case '\0':
                    sb.Append("%00");
                    break;
                case '\n':
                    sb.Append("%0A");
                    break;
                case '\r':
                    sb.Append("%0D");
                    break;
                case ',':
                    sb.Append("%2C");
                    break;
                case '}':
                    sb.Append("%7D");
                    break;
                case '%':
                    sb.Append("%25");
                    break;
                default:
                    sb.Append(c);
                    break;
            }
        }
        return sb.ToString();
    }
    
    /**
     * 
     * @param s 
     * @throws java.io.IOException 
     * @return 
     */
    static string fromCSVstring(string s) {
        if (s[0] != '\'') {
            throw new System.IO.IOException("Error deserializing string.");
        }
        int len = s.Length;
        StringBuilder sb = new StringBuilder(len-1);
        for (int i = 1; i < len; i++) {
            char c = s[i];
            if (c == '%') {
                char ch1 = s[i+1];
                char ch2 = s[i+2];
                i += 2;
                if (ch1 == '0' && ch2 == '0') { sb.Append('\0'); }
                else if (ch1 == '0' && ch2 == 'A') { sb.Append('\n'); }
                else if (ch1 == '0' && ch2 == 'D') { sb.Append('\r'); }
                else if (ch1 == '2' && ch2 == 'C') { sb.Append(','); }
                else if (ch1 == '7' && ch2 == 'D') { sb.Append('}'); }
                else if (ch1 == '2' && ch2 == '5') { sb.Append('%'); }
                else {throw new IOException("Error deserializing string.");}
            } else {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }
    
    /**
     * 
     * @param buf 
     * @return 
     */
    
    public static string toCSVBuffer(byte[] barr) {
        if (barr == null || barr.Length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(barr.Length + 1);
        sb.Append('#');
        for(int idx = 0; idx < barr.Length; idx++) {
            sb.Append(Convert.ToString(barr[idx], 16));
        }
        return sb.ToString();
    }

    public static int compareBytes(byte[] b1, int off1, int len1, byte[] b2, int off2, int len2) {
    	int i;
    	for(i=0; i < len1 && i < len2; i++) {
    		if (b1[off1+i] != b2[off2+i]) {
    			return b1[off1+i] < b2[off2+1] ? -1 : 1;
    		}
    	}
    	if (len1 != len2) {
    		return len1 < len2 ? -1 : 1;
    	}
    	return 0;
    }
}

}
