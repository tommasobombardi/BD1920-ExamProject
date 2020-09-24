package common;

import java.util.Calendar;
import java.util.regex.PatternSyntaxException;

public class ParserUtility {

    public static final String pipe = "|";
    public static final String quotes  = "\"";
    public static final String slash = "/";
    public static final String doubleSlash = "//";
    public static final String noGenresListed = "(no genres listed)";

    private static final String commaRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private static final String pipeRegex = "\\|(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private static final String slashRegex = "/(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static String getParsedTitle(String title) {
        String res = title.trim();
        if (title.startsWith(quotes)) res = res.substring(1);
        if (title.endsWith(quotes)) res = res.substring(0, res.length() - 1);
        return res;
    }

    public static int getYearFromTimestamp(String timestamp) throws NumberFormatException {
        long ts = Long.parseLong(timestamp.trim()) * 1000L;
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(ts);
        return cal.get(Calendar.YEAR);
    }

    public static String[] splitComma(String input) throws PatternSyntaxException {
        return input.split(commaRegex);
    }

    public static String[] splitPipe(String input) throws PatternSyntaxException {
        return input.split(pipeRegex);
    }

    public static String[] splitSlash(String input) throws PatternSyntaxException {
        return input.split(slashRegex);
    }

}
