package com.db.dataplatform.techtest.common;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Md5Hasher {

    private Md5Hasher() {}

    public static String generateHash(String input) {
        Objects.requireNonNull(input);

        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(input.getBytes(UTF_8));
            byte[] bytes = digest.digest();
            return DatatypeConverter.printHexBinary(bytes);
        } catch (NoSuchAlgorithmException e) {
            // This will never happen if JVM configured correctly
            throw new RuntimeException();
        }
    }
}
