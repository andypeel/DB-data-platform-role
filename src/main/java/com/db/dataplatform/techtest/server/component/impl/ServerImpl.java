package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.common.Md5Hasher;
import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.mapper.ServerMapperConfiguration;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.modelmapper.TypeToken;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;

    /**
     * @param envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope) {
        // Requirement: invalid data is wasted space, i.e. don't persist if invalid
        String recalculatedHash = Md5Hasher.generateHash(envelope.getDataBody().getDataBody());
        if (recalculatedHash.equalsIgnoreCase(envelope.getDataBody().getChecksum())) {
            persist(envelope);
            log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
            return true;
        }

        log.info("Incoming checksum didn't match rehash of content, data name: {}. Incoming={} but recalculated={}",
                envelope.getDataHeader().getName(),
                envelope.getDataBody().getChecksum(),
                recalculatedHash);
        return false;
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        BlockTypeEnum blockTypeEnum = BlockTypeEnum.of(blockType);
        List<DataBodyEntity> entities = dataBodyServiceImpl.getDataByBlockType(blockTypeEnum);
        List<DataEnvelope> result = map(entities);
        return result;
    }

    private List<DataEnvelope> map(List<DataBodyEntity> entities) {
        List<DataEnvelope> result = new ArrayList<>(entities.size());
        for (DataBodyEntity entity : entities) {
            DataHeader header = modelMapper.map(entity.getDataHeaderEntity(), DataHeader.class);
            DataBody body = modelMapper.map(entity, DataBody.class);
            result.add(new DataEnvelope(header, body));
        }
        return result;
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

}
