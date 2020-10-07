package com.db.dataplatform.techtest.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Md5HasherTest {

    @Test
    void itShouldThrowAnNPEIfInputIsNull() {
        Assertions.assertThrows(NullPointerException.class, () -> Md5Hasher.generateHash(null));
    }

    @Test
    void itShouldWorkIfInputEmpty() throws Exception {
        String result = Md5Hasher.generateHash("");
        assertThat(result).isEqualTo("D41D8CD98F00B204E9800998ECF8427E");
    }

    @Test
    void nonEmptyInput() throws Exception {
        String result = Md5Hasher.generateHash("The quick brown fox jumps over the lazy dog");
        assertThat(result).isEqualTo("9E107D9D372BB6826BD81D3542A419D6");
    }
}