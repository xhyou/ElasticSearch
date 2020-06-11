package com.es.boot.utils;

import com.es.boot.domain.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import javax.swing.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * 解析页面
 */

@Component
public class HtmlParseUtil {
    public static void main(String[] args) throws Exception {
       new HtmlParseUtil().parseJD("java").forEach(System.out::println);
    }

    public List<Content> parseJD(String keyword) throws Exception {
        List<Content> goodsList = new ArrayList<>();
        //京东的url
        String url ="https://search.jd.com/Search?keyword="+keyword;
        Document document = Jsoup.parse(new URL(url), 30000);
        //解析网页
        Element element = document.getElementById("J_goodsList");
        //获取所有的li标签
        Elements liElements = element.getElementsByTag("li");
        for(Element el:liElements){
            String img = el.getElementsByTag("img").eq(0).attr("src");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();
            Content content = new Content();
            content.setImg(img);
            content.setPrice(price);
            content.setTitle(title);
            goodsList.add(content);
        }
        return goodsList;
    }
}
