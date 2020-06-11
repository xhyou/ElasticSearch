package com.es.boot.controller;

import com.es.boot.service.ContentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Controller
public class ContentController {

    @Autowired
    private ContentService contentService;

    @RequestMapping("/parse/{keyword}")
    @ResponseBody
    public Boolean parse(@PathVariable("keyword")String keyword) throws Exception{
        return  contentService.parseContent(keyword);
    }

    @RequestMapping("/searchPage/{keyword}/{page}/{pageSize}")
    @ResponseBody
    public List<Map<String,Object>> searchPage(@PathVariable("keyword")String keyword,
                                               @PathVariable("page")int page,
                                               @PathVariable("pageSize")int pagSize) throws IOException {
        return contentService.searchPage(keyword,page,pagSize);
    }

    @RequestMapping("/searchHightPage/{keyword}/{page}/{pageSize}")
    @ResponseBody
    public List<Map<String,Object>> searchHightPage(@PathVariable("keyword")String keyword,
                                               @PathVariable("page")int page,
                                               @PathVariable("pageSize")int pagSize) throws IOException {
        return contentService.searchHightPage(keyword,page,pagSize);
    }
}
