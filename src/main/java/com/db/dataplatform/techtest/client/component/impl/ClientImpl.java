package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataBody;
import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.net.URI;
import java.util.List;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    private final RestTemplate restTemplate;

    @Override
    public boolean pushData(DataEnvelope dataEnvelope) {
        log.debug("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);
        Boolean hashMatch = restTemplate.postForObject(URI.create(URI_PUSHDATA), dataEnvelope, Boolean.class);
        log.info("Pushed data {} to {} with hashMatch {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA, hashMatch);
        return hashMatch;
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.debug("Querying for data with header block type {}", blockType);

        ResponseEntity<List<DataEnvelope>> result = restTemplate.exchange(
                URI_GETDATA.expand(blockType),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<DataEnvelope>>() {
                });
        log.info("Queried for data with header block type {} and found {} results", blockType, result.getBody().size());
        return result.getBody();
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blocktype to {} for block with name {}", newBlockType, blockName);
        return true;
    }
}
