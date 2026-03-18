package org.monetdb.spark.util;

import org.junit.jupiter.api.function.Executable;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class Assertions {
    /**
     * Check that the given code throws a SQLException or at least another exception
     * with a SQLException in the cause chain
     *
     * @param executable code to run
     * @return SQLException found
     */
    public static SQLException assertThrowsSQLException(Executable executable) {
        Exception exception = assertThrows(Exception.class, executable);
        for (Throwable e = exception; e != null; e = e.getCause()) {
            if (e instanceof SQLException se) {
                return se;
            }
        }
        fail(exception);
        // fail() does not return
        return null;
    }

    public static String dollarEscape(byte[] bytes) {
        StringBuilder buf = new StringBuilder();
        for (byte b : bytes) {
            if (b == '$') {
                buf.append("$$");
            } else if (b >= ' ' && b < 127) {
                buf.append((char) b);
            } else {
                int bb = (b + 256) & 0xFF;
                final String hex = "0123456789abcdef";
                char hi = hex.charAt(bb / 16);
                char lo = hex.charAt(bb % 16);
                buf.append('$');
                buf.append(hi);
                buf.append(lo);
            }
        }
        return buf.toString();
    }
}
