package com.db.dataplatform.techtest.server.persistence;

import java.util.HashMap;
import java.util.Map;

public enum BlockTypeEnum {
    BLOCKTYPEA("blocktypea"),
    BLOCKTYPEB("blocktypeb");

    private static final Map<String, BlockTypeEnum> map = new HashMap<>();

    static {
        for (BlockTypeEnum blockTypeEnum : values()) {
            map.put(blockTypeEnum.type.toLowerCase(), blockTypeEnum);
        }
    }

    public static BlockTypeEnum of(String name) {
        BlockTypeEnum result = map.get(name.toLowerCase());
        if (result == null) {
            throw new IllegalArgumentException("Invalid block type name: " + name);
        }
        return result;
    }


    private final String type;

    BlockTypeEnum(String type) {
        this.type = type;
    }

}
