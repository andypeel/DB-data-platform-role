package com.db.dataplatform.techtest;

import com.db.dataplatform.techtest.common.Md5Hasher;
import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;

import java.time.Instant;

public class TestDataHelper {

    public static final String TEST_NAME = "Test";
    public static final String TEST_NAME_EMPTY = "";
    public static final String DUMMY_DATA = "AKCp5fU4WNWKBVvhXsbNhqk33tawri9iJUkA5o4A6YqpwvAoYjajVw8xdEw6r9796h1wEp29D";

    public static DataHeaderEntity createTestDataHeaderEntity(Instant expectedTimestamp) {
        DataHeaderEntity dataHeaderEntity = new DataHeaderEntity();
        dataHeaderEntity.setName(TEST_NAME);
        dataHeaderEntity.setBlocktype(BlockTypeEnum.BLOCKTYPEA);
        dataHeaderEntity.setCreatedTimestamp(expectedTimestamp);
        return dataHeaderEntity;
    }

    public static DataBodyEntity createTestDataBodyEntity(DataHeaderEntity dataHeaderEntity) {
        DataBodyEntity dataBodyEntity = new DataBodyEntity();
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);
        dataBodyEntity.setDataBody(DUMMY_DATA);
        return dataBodyEntity;
    }

    public static DataEnvelope createTestDataEnvelopeApiObject() {
        return createTestDataEnvelopeApiObject(TEST_NAME, DUMMY_DATA, Md5Hasher.generateHash(DUMMY_DATA), BlockTypeEnum.BLOCKTYPEA);
    }

    public static DataEnvelope createTestDataEnvelopeApiObjectWithEmptyName() {
        return createTestDataEnvelopeApiObject(TEST_NAME_EMPTY, DUMMY_DATA, Md5Hasher.generateHash(DUMMY_DATA), BlockTypeEnum.BLOCKTYPEA);
    }

    public static DataEnvelope createTestDataEnvelopeApiObject(String name, String body, String checksum, BlockTypeEnum blockType) {
        DataBody dataBody = new DataBody(body, checksum);
        DataHeader dataHeader = new DataHeader(name, blockType);
        DataEnvelope dataEnvelope = new DataEnvelope(dataHeader, dataBody);
        return dataEnvelope;
    }
}
