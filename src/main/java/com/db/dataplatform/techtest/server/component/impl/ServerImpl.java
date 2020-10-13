package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.common.Md5Hasher;
import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;
    private final String hadoopLocation;
    private final ArrayBlockingQueue<DataEnvelope> hadoopQueue;
    private final Executor hadoopQueueExecutor;
    private final RestTemplate restTemplate;

    // Spring @Value injection doesn't work with Lombok, so use an explicit constructor
    public ServerImpl(DataBodyService dataBodyServiceImpl,
               ModelMapper modelMapper,
               @Value("${hadoop.location}") String hadoopLocation,
               RestTemplate restTemplate) {
        this.dataBodyServiceImpl = dataBodyServiceImpl;
        this.modelMapper = modelMapper;
        this.hadoopLocation = hadoopLocation;
        this.restTemplate = restTemplate;

        hadoopQueue = new ArrayBlockingQueue<>(1000);
        hadoopQueueExecutor = Executors.newFixedThreadPool(1);
        hadoopQueueExecutor.execute(() -> sendToHadoop());
    }

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
            queueForSendingToHadoop(envelope);
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

    @Override
    public boolean updateBlockType(String blockName, String newBlockType) {

        //NOTE: I've taken the preexisting DataBodyService.getDataByBlockName() method signature as a hint
        //and used it in my implementation. But really this would be better updated using an optimistic lock
        Optional<DataBodyEntity> bodyEntityOptional = dataBodyServiceImpl.getDataByBlockName(blockName);
        return bodyEntityOptional.map(entity -> {
            entity.getDataHeaderEntity().setBlocktype(BlockTypeEnum.of(newBlockType));
            return true;
        }).orElse(false);
    }

    // Probably the result here should be paged, depending on expected number of matching blocks
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

    /**
     * Decouple the send to Hadoop, so our API users don't have to wait for it to complete before can
     * receive a response from us. Do so in a bounded way, to avoid running out of memory.
     * @param envelope
     */
    private void queueForSendingToHadoop(DataEnvelope envelope) {
        if (!hadoopQueue.offer(envelope)) {
            // There is some kind of problem with the thread sending data to Hadoop. Log and move on as we
            // don't want to be tightly coupled to this new experimental system.
            log.warn("Unable to add {} to Hadoop queue as the queue is full", envelope);
        }
    }

    private void sendToHadoop() {
        while (true) {
            try {
                DataEnvelope envelope = hadoopQueue.take();
                doHttpSend(envelope);
            } catch (InterruptedException e) {
                log.info("Hadoop queue consumer thread interrupted");
                // Assume we're being stopped - propagate and exit
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void doHttpSend(DataEnvelope envelope) {
        // Could do a back off and retry with an increasing period, for a limited number of attempts.
        // What's the requirement?
        ResponseEntity<Void> result = restTemplate.postForEntity(hadoopLocation, envelope, Void.class);
        if (result.getStatusCode() != HttpStatus.OK) {
            log.error("Failed to send envelope to Hadoop. Status code {}",
                    result.getStatusCode());
        } else {
            log.info("Successfully posted envelope to Hadoop {}", envelope);
        }
    }
}
