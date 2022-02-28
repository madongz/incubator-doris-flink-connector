package org.apache.doris.flink.sink;

import java.util.regex.Pattern;

/**
 * ResponsePattern
 */
public class ResponsePattern {
    public static final Pattern LABEL_EXIST_PATTERN =
            Pattern.compile("errCode = 2, detailMessage = Label \\[(.*)\\] " +
                    "has already been used, relate to txn \\[(\\d+)\\]");
    public static final Pattern COMMITTED_PATTERN =
            Pattern.compile("errCode = 2, detailMessage = transaction \\[(\\d+)\\] " +
                    "is already visible, not pre-committed.");
}
