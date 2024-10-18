package org.openg2p.reporting.kafka.connect.transforms.util;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.Versions;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.module.loaders.BuiltinModuleLoader;

public class JqUtil {

    private ObjectMapper objectMapper;
    private JsonQuery jqQuery;
    private Scope scope;

    public JqUtil(String expression, Scope scope, Version jqVersion, ObjectMapper objectMapper) throws JsonQueryException {
        if(jqVersion == null) {
            jqVersion = Versions.JQ_1_6;
        }
        if(scope == null) {
            scope = Scope.newEmptyScope();
            BuiltinFunctionLoader.getInstance().loadFunctions(jqVersion, scope);
            scope.setModuleLoader(BuiltinModuleLoader.getInstance());
        }
        if(objectMapper == null){
            objectMapper = new ObjectMapper();
        }
        this.jqQuery = JsonQuery.compile(expression, jqVersion);
        this.scope = scope;
        this.objectMapper = objectMapper;
    }

    public JqUtil(String expression, ObjectMapper objectMapper) throws JsonQueryException {
        this(expression, null, null, objectMapper);
    }

    public JqUtil(String expression) throws JsonQueryException {
        this(expression, null);
    }

    public ObjectMapper getObjectMapper(){
        return this.objectMapper;
    }

    public List<JsonNode> applyRaw(JsonNode input) throws JsonQueryException {
        List<JsonNode> outputNodes = new ArrayList<>();
        jqQuery.apply(scope, input, outputNodes::add);
        return outputNodes;
    }

    public JsonNode firstRaw(JsonNode input) throws JsonQueryException {
        List<JsonNode> output = this.applyRaw(input);
        if(output.size() < 1){
            return null;
        }
        return output.get(0);
    }

    public <T> List<T> apply(Object input, Class<T> outputType) throws JsonQueryException, JsonProcessingException {
        JsonNode jsonInput = objectMapper.valueToTree(input);
        List<JsonNode> outputNodes = applyRaw(jsonInput);
        List<T> output = new ArrayList<>();
        for(JsonNode outputNode: outputNodes) {
            output.add(objectMapper.treeToValue(outputNode, outputType));
        }
        return output;
    }

    public <T> T first(Object input, Class<T> outputType) throws JsonQueryException, JsonProcessingException{
        List<T> output = this.apply(input, outputType);
        if(output.size() < 1){
            return null;
        }
        return output.get(0);
    }

    public List<Object> apply(Object input) throws JsonQueryException, JsonProcessingException {
        return this.apply(input, Object.class);
    }

    public Object first(Object input) throws JsonQueryException, JsonProcessingException {
        return this.first(input, Object.class);
    }
}
