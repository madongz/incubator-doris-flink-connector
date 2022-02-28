package org.apache.doris.flink.sink;

import java.util.Properties;
import java.util.logging.Handler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

/**
 * Handler for escape in properties.
 */
public class EscapeHandler {
    public static final String ESCAPE_DELIMITERS_FLAGS = "\\x";
    public static final Pattern escapePattern = Pattern.compile("\\\\x([0-9|a-f|A-F]{2})");
    public String escapeString(String source) {
        if(source.contains(ESCAPE_DELIMITERS_FLAGS)) {
            Matcher m = escapePattern.matcher(source);
            StringBuffer buf = new StringBuffer();
            while (m.find()) {
                m.appendReplacement(buf, String.format("%s", (char)Integer.parseInt(m.group(1), 16)));
            }
            m.appendTail(buf);
            return buf.toString();
        }
        return source;
    }

    public void handle(Properties properties) {
        String fieldDelimiter = properties.getProperty(FIELD_DELIMITER_KEY,
                FIELD_DELIMITER_DEFAULT);
        if (fieldDelimiter.contains(ESCAPE_DELIMITERS_FLAGS)) {
            properties.setProperty(FIELD_DELIMITER_KEY, escapeString(fieldDelimiter));
        }
        String lineDelimiter = properties.getProperty(LINE_DELIMITER_KEY,
                LINE_DELIMITER_DEFAULT);
        if(lineDelimiter.contains(ESCAPE_DELIMITERS_FLAGS)) {
            properties.setProperty(LINE_DELIMITER_KEY, escapeString(lineDelimiter));
        }
    }

    public static void handleEscape(Properties properties) {
        EscapeHandler handler = new EscapeHandler();
        handler.handle(properties);
    }
}
