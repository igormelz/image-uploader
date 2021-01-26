package ru.openfs.function;

import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Random;

public class StringUtils {
    public static String generateName(String prefix, int length, String suffix) {
        Random random = new Random();
        String generatedString = random.ints(48, 122).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        return prefix + generatedString + "." + suffix;
    }

    public static String generateNumber(String prefix, int length, String suffix) {
        Random random = new Random();
        String generatedString = random.ints(48, 58)
                .limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        return prefix + generatedString + suffix;
    }

    public static String decodeTitle(String title) {
        String decodedTitle = URLDecoder.decode(title, Charset.forName("UTF-8"));
        return decodedTitle.contains(".") ? decodedTitle.substring(0, decodedTitle.lastIndexOf(".")) : decodedTitle;
    }
}
