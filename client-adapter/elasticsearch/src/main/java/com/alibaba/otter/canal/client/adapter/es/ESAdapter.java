package com.alibaba.otter.canal.client.adapter.es;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SqlParser;
import com.alibaba.otter.canal.client.adapter.es.monitor.ESConfigMonitor;
import com.alibaba.otter.canal.client.adapter.es.service.ESEtlService;
import com.alibaba.otter.canal.client.adapter.es.service.ESSyncService;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;


/**
 * ES外部适配器
 *
 * @author rewerma 2018-10-20
 * @version 1.0.0
 */
@SPI("es")
public class ESAdapter implements OuterAdapter {

    private Map<String, ESSyncConfig>              esSyncConfig        = new ConcurrentHashMap<>(); // 文件名对应配置
    private Map<String, Map<String, ESSyncConfig>> dbTableEsSyncConfig = new ConcurrentHashMap<>(); // schema-table对应配置

    private RestHighLevelClient elasticSearchClient;

    private ESSyncService                          esSyncService;

    private ESConfigMonitor                        esConfigMonitor;

    private Properties                             envProperties;

    public RestHighLevelClient getElasticSearchClient() {
        return elasticSearchClient;
    }

    public ESSyncService getEsSyncService() {
        return esSyncService;
    }

    public Map<String, ESSyncConfig> getEsSyncConfig() {
        return esSyncConfig;
    }

    public Map<String, Map<String, ESSyncConfig>> getDbTableEsSyncConfig() {
        return dbTableEsSyncConfig;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            this.envProperties = envProperties;
            Map<String, ESSyncConfig> esSyncConfigTmp = ESSyncConfigLoader.load(envProperties);
            // 过滤不匹配的key的配置
            esSyncConfigTmp.forEach((key, config) -> {
                if ((config.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (config.getOuterAdapterKey() != null
                        && config.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {
                    esSyncConfig.put(key, config);
                }
            });

            for (Map.Entry<String, ESSyncConfig> entry : esSyncConfig.entrySet()) {
                String configName = entry.getKey();
                ESSyncConfig config = entry.getValue();
                SchemaItem schemaItem = SqlParser.parse(config.getEsMapping().getSql());
                config.getEsMapping().setSchemaItem(schemaItem);

                DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
                if (dataSource == null || dataSource.getUrl() == null) {
                    throw new RuntimeException("No data source found: " + config.getDataSourceKey());
                }
                Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
                Matcher matcher = pattern.matcher(dataSource.getUrl());
                if (!matcher.find()) {
                    throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
                }
                String schema = matcher.group(2);

                schemaItem.getAliasTableItems().values().forEach(tableItem -> {
                    Map<String, ESSyncConfig> esSyncConfigMap;
                    if (envProperties != null
                        && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                        esSyncConfigMap = dbTableEsSyncConfig
                            .computeIfAbsent(StringUtils.trimToEmpty(config.getDestination()) + "-"
                                             + StringUtils.trimToEmpty(config.getGroupId()) + "_" + schema + "-"
                                             + tableItem.getTableName(),
                                k -> new ConcurrentHashMap<>());
                    } else {
                        esSyncConfigMap = dbTableEsSyncConfig
                            .computeIfAbsent(StringUtils.trimToEmpty(config.getDestination()) + "_" + schema + "-"
                                             + tableItem.getTableName(),
                                k -> new ConcurrentHashMap<>());
                    }

                    esSyncConfigMap.put(configName, config);
                });
            }

            String[] hostArray = configuration.getHosts().split(",");
            HttpHost[] httpHosts = new HttpHost[hostArray.length];
            for (int idx = 0; idx < hostArray.length; idx ++) {
                String host = hostArray[idx];
                int i = host.indexOf(":");
                httpHosts[idx] = new HttpHost(host.substring(0, i),Integer.parseInt(host.substring(i + 1)));
            }
            RestClientBuilder builder = RestClient.builder(httpHosts)
                    .setRequestConfigCallback(requestConfigBuilder ->
                            requestConfigBuilder.setConnectTimeout(10000)
                                    .setSocketTimeout(10000)

                    );
            String accessId = configuration.getProperties().get("accessId");
            String accessKey = configuration.getProperties().get("accessKey");
            if(StringUtils.isNotBlank(accessId) && StringUtils.isNotBlank(accessKey)){
                final CredentialsProvider credentialsProvider =
                        new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(accessId,accessKey));
                builder.setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder .setDefaultCredentialsProvider(credentialsProvider)
                );
            }
            elasticSearchClient = new RestHighLevelClient(builder);
            ESTemplate esTemplate = new ESTemplate(elasticSearchClient);
            esSyncService = new ESSyncService(esTemplate);

            esConfigMonitor = new ESConfigMonitor();
            esConfigMonitor.init(this, envProperties);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        for (Dml dml : dmls) {
            if (!dml.getIsDdl()) {
                sync(dml);
            }
        }
        esSyncService.commit(); // 批次统一提交

    }

    private void sync(Dml dml) {
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> configMap;
        if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            configMap = dbTableEsSyncConfig
                .get(StringUtils.trimToEmpty(dml.getDestination()) + "-" + StringUtils.trimToEmpty(dml.getGroupId())
                     + "_" + database + "-" + table);
        } else {
            configMap = dbTableEsSyncConfig
                .get(StringUtils.trimToEmpty(dml.getDestination()) + "_" + database + "-" + table);
        }

        if (configMap != null && !configMap.values().isEmpty()) {
            esSyncService.sync(configMap.values(), dml);
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            ESEtlService esEtlService = new ESEtlService(elasticSearchClient, config);
            if (dataSource != null) {
                return esEtlService.importData(params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSuccess = true;
            // ds不为空说明传入的是datasourceKey
            for (ESSyncConfig configTmp : esSyncConfig.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    ESEtlService esEtlService = new ESEtlService(elasticSearchClient, configTmp);
                    EtlResult etlRes = esEtlService.importData(params);
                    if (!etlRes.getSucceeded()) {
                        resSuccess = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSuccess);
                if (resSuccess) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }


    @Override
    public void destroy() {
        if (esConfigMonitor != null) {
            esConfigMonitor.destroy();
        }
        if (elasticSearchClient != null) {
            IOUtils.closeQuietly(elasticSearchClient);
        }
    }

    @Override
    public String getDestination(String task) {
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }
//    @Override
//    public Map<String, Object> count(String task) {
//        ESSyncConfig config = esSyncConfig.get(task);
//        ESMapping mapping = config.getEsMapping();
//        CountRequest request = new CountRequest(mapping.get_index());
//        request.types(mapping.get_type());
//        SearchSourceBuilder searchSource = new SearchSourceBuilder();
//        searchSource.size(0);
//        long rowCount = 0;
//        try {
//            CountResponse response = elasticSearchClient.count(request, RequestOptions.DEFAULT);
//            rowCount = response.getCount();
//        }catch (IOException e){
//
//        }
//        Map<String, Object> res = new LinkedHashMap<>();
//        res.put("esIndex", mapping.get_index());
//        res.put("count", rowCount);
//        return res;
//    }
    @Override
    public Map<String, Object> count(String task) {
        if(StringUtils.isNotBlank(task) && task.equalsIgnoreCase("clear")){
            ESSyncService.insertFieldMap.clear();
        }
        return ESSyncService.insertFieldMap;
    }
}
