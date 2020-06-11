package com.es.boot.service;

import com.alibaba.fastjson.JSON;
import com.es.boot.domain.Content;
import com.es.boot.utils.HtmlParseUtil;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class ContentService {

    @Autowired
    private HtmlParseUtil htmlParseUtil;
    
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //将数据放到到es的索引中
    public Boolean parseContent(String keyWord) throws  Exception{
        List<Content> contents = htmlParseUtil.parseJD(keyWord);
        BulkRequest bulkRequest = new BulkRequest("jd_index");
        for(int i=0;i<contents.size();i++){
            bulkRequest.add(new IndexRequest()
                            .source(JSON.toJSONString(contents.get(i)), XContentType.JSON));
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        return !bulk.hasFailures();
    }

    //获取这些数据 实现搜索功能
    public List<Map<String,Object>> searchPage(String keyword, int page, int pageSize) throws IOException {
        List<Map<String,Object>> resultList = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest("jd_index");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keyword);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.query(termQueryBuilder);

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            resultList.add(searchHit.getSourceAsMap());
        }
        return resultList;
    }

    //高亮显示
    public List<Map<String,Object>> searchHightPage(String keyword, int page, int pageSize) throws IOException {
        List<Map<String,Object>> resultList = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest("jd_index");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keyword);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.query(termQueryBuilder);

        //高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.requireFieldMatch(false);//不需要多个高亮 只需要高亮第一个
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        sourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            //解析高亮的字段
            Map<String, HighlightField> highlightFieldMap = searchHit.getHighlightFields();
            HighlightField title = highlightFieldMap.get("title");
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            if(title!=null){
                //取出高亮的字段
                Text[] fragments = title.fragments();
                String n_title ="";
                for (Text text : fragments) {
                    n_title+=text;
                }
                sourceAsMap.put("title",n_title);//替换
            }
            resultList.add(sourceAsMap);
        }
        return resultList;
    }
}
