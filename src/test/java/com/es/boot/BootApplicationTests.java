package com.es.boot;

import com.alibaba.fastjson.JSON;
import com.es.boot.domain.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class BootApplicationTests {


    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;


    //创建索引
    @Test
    void testCreateIndex() throws IOException {
        CreateIndexRequest indexRequest = new CreateIndexRequest("xuehy");
        CreateIndexResponse response = client.indices().create(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    //测试索引是否存在
    @Test
    void testIndexExist() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("xuehy");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //删除指定的索引
    @Test
    void deleteIndex() throws IOException{
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("xuehy");
        AcknowledgedResponse acknowledgedResponse = client.indices().delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(acknowledgedResponse.isAcknowledged());
    }

    /****************关于文档的操作******************/

    //添加测试文档
    @Test
    public void testAddDocument() throws IOException {
        User user = new User("xhy",26);
        //创建请求
        IndexRequest request = new IndexRequest("xuehy");
        //相当于put xuehy/_doc/1
        request.id("1");
        request.timeout("1s");
        // 将我们的数据放入请求 json
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求,获取响应结果
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString());
        //获取返回的状态
        System.out.println(indexResponse.status());
    }

    //获取文档是否存在
    @Test
    public void testDocumentExists() throws IOException {
        GetRequest getRequest= new GetRequest("xuehy","1");
        // 不获取返回的 _source 的上下文了
        getRequest.fetchSourceContext((new FetchSourceContext(false)));
        getRequest.storedFields("_none_");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档的信息
    @Test
    public void testGetDocument() throws IOException{
        GetRequest getRequest = new GetRequest("xuehy","1");
        //显示指定的字段 按照age排序
        /*getRequest.fetchSourceContext((new FetchSourceContext(true,new String[]{"age"},new String[]{})));
        getRequest.storedFields("age");*/
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());//打印文档的内容 已json的格式输出
        System.out.println(getResponse); //返回的内容格式和命令是一样的
    }

    //更新文档的信息
    @Test
    public void testUpdateDocument() throws IOException{
        UpdateRequest updateRequest = new UpdateRequest("xuehy","1");
        updateRequest.timeout("1s");

        //POST xuehy/_doc/1/_update
        User user = new User("小薛");
        updateRequest.doc(JSON.toJSONString(user),XContentType.JSON);
        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }

    //删除文档信息
    @Test
    public void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("xuehy","1");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    //批量操作
    @Test
    public void testBulkRequest() throws IOException{
        BulkRequest bulkRequest = new BulkRequest();
        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("A",3));
        userList.add(new User("B",3));
        userList.add(new User("C",3));
        userList.add(new User("D",3));
        userList.add(new User("E",3));
        for (int i=0;i<userList.size();i++){
            bulkRequest.add(new IndexRequest("xhy")
                    .id(""+(i+1)) //如果不写id 这边的话就是自动递增
                    .source(JSON.toJSONString(userList.get(i)),XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());
    }

    //特殊查询
    @Test
    public void testSearch() throws IOException{
        SearchRequest searchRequest = new SearchRequest("xhy1");
        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        sourceBuilder.highlighter(highlightBuilder);
        //特别注意:使用精确查询的类型需要设置为 keyword格式 否则会进行分词拆分
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "这是一个小标题");
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);

        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(search.getHits()));
        System.out.println("==============");
        for (SearchHit documentFields : search.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }
    }
}
