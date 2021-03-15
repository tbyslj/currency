# -*- coding:utf-8 -*-
"""
@File: logger
@Time: 2021-01-01 14:37:16
@Auth: tang
"""
import sys
import os
import re
import time
import json
from functools import reduce
from hashlib import md5

import chardet
from urllib.parse import urlparse, urljoin
from pyquery import PyQuery

from gli.logger import Logger
from gli.connect.mongodb import MongoDB
from gli.http_request import HttpRequest

class GeneralCrawlSpider(HttpRequest):
    def __init__(self, retry: int=3, timeout: int=20, config_json: str=None):
        self.folder = re.sub("\.json", "", config_json)
        self.logger = Logger(folder=self.folder)
        super().__init__(self.logger, retry, timeout)
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "max-age=0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
        }
        self.mongo = MongoDB(log=self.logger)
        self.spider_writer = self.mongo.client["local_rw"]["currency"]
        file = os.path.join(os.getcwd(), "gli", "settings", config_json)
        self.config = json.load(open(file, "r"))
        self.url = self.config["url_config"]["source"]
        self.turn_page = self.config["url_config"]["turn_page"]
        self.classify = self.config["url_config"]["classify"]
        self.domain = self.config["url_config"]["domain"]
        self.turn_content = self.config["content_config"]["turn_page"]
        #url配置
        self.url_dict = {}
        self.url_list = []
        self.label_list = []
        self.classify_dict = {}
        self.classify_list = []
        #内容配置
        self.head_title = ""
        self.title_dict = {}
        self.content = []
        self.parse_dict = {}
        self.h_list = []

    def parse_url(self, url, type):
        """
        递归提取url
        """
        try:
            html, re_url = self.get_html(url)
            body = self.filter_html(html)
            a_list = body.xpath('.//a')
            for a in a_list:
                a_name = a.xpath('.//text()')
                if a_name and len("".join(a_name).strip()) <= 10:
                    a_name = "".join(a_name).strip()
                else:
                    continue
                for tp in type:
                    result = re.search(self.handle_text((tp["rex"],)), self.handle_text((a_name,)))
                    if result and result.group() in self.handle_text((a_name,)):
                        name = result.group()
                        a_href = a.xpath('./@href')
                        if not a_href:
                            continue
                        url = self.change_url(re_url, a_href[0])
                        if self.domain not in url or name != self.handle_text((a_name,)):
                            continue
                        self.url_dict[name] = url
                        self.label_list.append(name)
                        if "classify" in tp.keys():
                            self.parse_url(url, tp["classify"])
                        self.classify_dict["label"] = self.label_list
                        self.classify_dict["id"] = tp["xpath"]["id"]
                        self.classify_dict["class"] = tp["xpath"]["class"]
                        self.classify_dict["node"] = tp["xpath"]["node"]
                        self.classify_list.append(self.classify_dict)
                        self.label_list = self.label_list[:-1]
                        self.classify_dict = {}
                        data_dict = self.url_dict.copy()
                        self.url_list.append(data_dict)
                        del self.url_dict[list(self.url_dict)[-1]]
            return self.url_list
        except Exception as e:
            self.logger.info(e)

    def change_url(self, url1, url2):
        """
        url替换
        """
        try:
            url = urljoin(url1, url2.strip())
            return url
        except Exception as e:
            self.logger.info(e)

    def get_classify(self, res: list, url_list: list):
        """
        获取分类列表
        """
        try:
            classify = [k for k in res if k["label"]]
            for sify in classify:
                for new_classify in self.classify_list:
                    if sify["label"] == new_classify["label"]:
                        sify["id"] = new_classify["id"]
                        sify["class"] = new_classify["class"]
                        sify["node"] = new_classify["node"]
                        break
            for sify in classify:
                if len(sify["label"]) <= 1:
                    label = sify["label"][-1]
                    for url in url_list:
                        if label in url.keys():
                            sify["url"] = url[label]
                            break
                else:
                    label_first = sify["label"][0]
                    label_last = sify["label"][-1]
                    for url in url_list:
                        if label_first in url.keys() and label_last in url.keys():
                            sify["url"] = url[label_last]
                            break
            return classify
        except Exception as e:
            self.logger.info(e)

    def filter_classify(self, classify_list):
        """
        过滤分类
        """
        result_list = []
        try:
            label_list = []
            delete_list = []
            for classify in classify_list:
                label_list.append(classify["label"])
            for i in range(len(label_list)):
                for j in range(i + 1, len(label_list)):
                    ret = [x for x in label_list[i] if x in label_list[j]]
                    delete_list.append(ret)
            delete_list = set(tuple(s) for s in delete_list)
            delete_list = [list(t) for t in delete_list]
            for delete in delete_list:
                if delete in label_list:
                    label_list.remove(delete)
            for label in label_list:
                result_list.append({"label": label})
        except Exception as e:
            self.logger.info(e)
        finally:
            return result_list

    def filter_url(self, url_list: list):
        """
        过滤url
        """
        try:
            run_function = lambda x, y: x if y in x else x + [y]
            result = reduce(run_function, [[], ] + url_list)
            return result
        except Exception as e:
            self.logger.info(e)

    def handle_url(self, start, i, re_url):
        """
        处理url
        """
        turn_page = re.search('\.' + self.turn_page["type"], re_url)
        if self.turn_page["type"] and turn_page:
            base_url = re_url.replace(turn_page.group(), "{}" + turn_page.group())
            if self.turn_page["confirm"]:
                url = urljoin(re_url, self.turn_page["mark"] + self.turn_page["symbol"] + str(
                    start + i - 1) + "." + self.turn_page["type"])
            elif self.turn_page["mark"] in base_url:
                url = base_url.format(self.turn_page["symbol"] + str(start + i - 1))
            else:
                url = base_url.format(self.turn_page["mark"] + self.turn_page["symbol"] + str(start + i - 1))
        else:
            if self.turn_page["confirm"]:
                url = urljoin(re_url, self.turn_page["mark"]) + self.turn_page["symbol"] + str(start + i - 1) + "." + \
                      self.turn_page["type"]
            else:
                add_url = re_url
                if "&" not in self.turn_page["mark"] and "?" not in self.turn_page["mark"] \
                        and not str(re_url).endswith("/"):
                    add_url = add_url + "/"
                if self.turn_page["type"]:
                    url = add_url + self.turn_page["mark"] + self.turn_page["symbol"] + str(start + i - 1) + "." + \
                          self.turn_page["type"]
                else:
                    url = add_url + self.turn_page["mark"] + self.turn_page["symbol"] + str(start + i - 1)
        return url

    def filter_html(self, html, is_body: bool=True):
        """
        过滤html
        """
        try:
            text = re.sub(r"<!-[\s\S]*?-->", "", html)
            doc = PyQuery(text)
            doc.remove("script")
            doc.remove("style")
            if is_body:
                return list(doc("body"))[0]
            else:
                return list(doc("head"))[0]
        except Exception as e:
            self.logger.info(e)
            return None

    def get_html(self, url):
        """
        获取html
        """
        try:
            response = self.get(url, headers=self.headers, timeout=20)
            if response.status_code in [200, 301, 302]:
                if response.apparent_encoding == "Windows-1254":
                    response.encoding = "GB18030"
                else:
                    response.encoding = response.apparent_encoding
                return response.text, response.url
        except Exception as e:
            self.logger.info(f"{e}===网址错误")
            return None, None

    def get_content(self, sify):
        """
        获取文章url列表
        """
        try:
            url = sify["url"]
            a_list = []
            start = self.turn_page["start"]
            page = self.turn_page["page"]
            html, re_url = self.get_html(url)
            if re_url is None:
                return [], None
            for i in range(page):
                if i == 0:
                    url = re_url
                else:
                    url = self.handle_url(start, i, re_url)
                html, res_url = self.get_html(url)
                body = self.filter_html(html)
                if body is None:
                    continue
                # result = self.parseA(body)
                # node = self.max_node(result, res_url)
                if sify["id"]:
                    index = "id"
                else:
                    index = "class"
                xpath = sify[index]
                xpath_node = sify["node"]
                if "@contains" in xpath:
                    contains = xpath.split("+")[1]
                    nodes = body.xpath('//*[contains(@{}, "{}")]'.format(index, contains))
                elif xpath_node:
                    if re.search("\d+", xpath_node) and len(xpath_node) == 1:
                        nodes = body.xpath('//*[@{}="{}"]'.format(index, xpath))
                        nodes = nodes[int(xpath_node)]
                    else:
                        nodes = body.xpath('//*[@{}="{}"]/{}'.format(index, xpath, xpath_node))
                else:
                    nodes = body.xpath('//*[@{}="{}"]'.format(index, xpath))
                if nodes is not None:
                    for node in nodes:
                        a_node = list(set(node.xpath('.//a')))
                        a_list += a_node
            return a_list, re_url
        except Exception as e:
            self.logger.info(e)
            return None, None

    def parseA(self, body, a_dict: dict = {}):
        """
        解析特征值a
        """
        try:
            childs = body.xpath('./*')
            for child in childs:
                a_num = child.xpath('.//a')
                a_dict[child] = len(a_num)
            return a_dict
        except Exception as e:
            self.logger.info(e)

    def max_node(self, results, url):
        """
        过滤节点列表，获取文章节点
        """
        try:
            results = sorted(results.items(), key=lambda kv: kv[1], reverse=True)
            for result in results:
                href_list = []
                node = []
                a_node = result[0]
                a_res = a_node.xpath('.//a/@href')
                for a in a_res:
                    if "#" in a and "java" in a:
                        continue
                    a_href = self.change_url(url, a)
                    domain = urlparse(a_href)[1]
                    if self.domain in domain:
                        href_list.append(a)
                if len(href_list) <= 5:
                    continue
                if a_node.tag in 'table':
                    a_node = a_node.xpath('./tr')[0]
                res = a_node.xpath('./*')
                for r in res:
                    text = r.xpath('.//text()')
                    text = self.handle_text(text)
                    if text:
                        node.append(r)
                index = -self.node_num
                if len(node) <= 1:
                    return node[-1]
                else:
                    return node[index]
        except Exception as e:
            self.logger.info(e)
            return None

    def filter_a(self, a_list: list, re_url: str, label: list):
        """
        过滤文章url列表
        """
        try:
            a_href = []
            href_list = []
            content_list = []
            urls = []
            if a_list:
                for a in a_list:
                    a_text = a.xpath('.//text()')
                    a_link = a.xpath('./@href')
                    if a_link:
                        if a_text:
                            text = "".join(a_text).strip()
                        else:
                            text = ""
                        if text in label or text.startswith('[') and text.endswith(']'):
                            continue
                        else:
                            a_link = a_link[0].strip()
                            # url = re.search("(http.*)\..*", re_url).group(1)
                            if "#" in a_link or "java" in a_link or a_link == "/" or \
                                    not str(a_link).endswith(self.turn_content["mark"]) or \
                                    str(a_link).endswith(".pdf"):
                                continue
                            a_href.append(a_link)
                for content in a_href:
                    url = self.change_url(re_url, content)
                    if self.domain in url and url not in urls:
                        urls.append(url)
                return urls

        except Exception as e:
            self.logger.info(e)
            return None

    def analysis_content(self, url):
        """
        解析正文
        """
        try:
            filter_tag = ["p", "span", "i", "em", "li", "font", "strong", "sub"]
            self.parse_dict = {}
            self.content = []
            self.title_dict = {}
            self.h_list = []
            html, re_url = self.get_html(url)
            if html is None:
                return None
            head = self.filter_html(html, False)
            body = self.filter_html(html)
            self.head_title = self.handle_text(head.xpath('./title//text()'))
            res_dict = self.parseDOC(body)
            title, node = self.get_title()
            if not title:
                return {}
            # 时间、作者、来源父节点
            parent_node = self.get_parent(node)
            # parent_class = parent_node.xpath('./@class')
            # 时间
            publish_time = self.get_time(parent_node)
            if not publish_time:
                publish_time = self.get_time(parent_node.xpath('./..')[0])
            if self.content:
                node_dict = {}
                for content_node in self.content:
                    if content_node in res_dict and int(res_dict[content_node]) != 0 and \
                            content_node.tag.lower() not in filter_tag:
                        node_dict[content_node] = res_dict[content_node]
                if node_dict:
                    text_node = self.max_dict(node_dict)
                    while True:
                        self.parse_dict = {}
                        a_num = len(text_node.xpath('.//a'))
                        path_dict = {}
                        if a_num > 6:
                            resl_dict = self.parseDOC(text_node)
                            path = self.handle_data(resl_dict)
                            if not path:
                                break
                            for ph in path:
                                for k, v in ph.items():
                                    path_dict[k] = v
                            text_node = self.max_dict(path_dict)
                        else:
                            if text_node.tag.lower() in filter_tag:
                                text_node = text_node.xpath('./..')[0]
                            break
                    text, text_node = self.three_handle(text_node, title, publish_time)
                    if text and len(text) <= 10:
                        text, text_node = self.two_handle(title, res_dict, publish_time)
                    elif "[更多详情]" in text:
                        text, text_node = self.one_handle(title, res_dict, publish_time)
                else:
                    text, text_node = self.one_handle(title, res_dict, publish_time)
            else:
                text, text_node = self.two_handle(title, res_dict, publish_time)
            if text_node is None or not len(text_node):
                node_id = []
                node_class = []
            else:
                node_id = text_node.xpath('./@id')
                node_class = text_node.xpath('./@class')
            detail_node = {
                "xpath": {"id": node_id, "class": node_class},
                "text_len": len(text)}
            return detail_node
        except Exception as e:
            self.logger.info(e)

    def one_handle(self, title: str, data: dict, publish_time: str):
        """
        处理文本函数
        """
        try:
            if publish_time is None:
                publish_time = '1000-01-01'
            path = self.handle_data(data)
            text_node = self.get_text_node(path)
            text = self.handle_text("".join(text_node.xpath('.//text()')))
            if title in text or publish_time in text:
                text = text_node.xpath('.//p//text()')
                for txt in text:
                    if txt.strip() and len(txt.strip()) <= 4:
                        text.remove(txt)
                text = self.handle_text("".join(text))
            return text, text_node
        except Exception as e:
            self.logger.info(e)
            return '', None

    def two_handle(self, title: str, data: dict, publish_time: str):
        """
        处理文本函数
        """
        try:
            if publish_time is None:
                publish_time = '1000-01-01'
            path = self.handle_data(data)
            text_node = self.get_text_node(path)
            text = self.handle_text("".join(text_node.xpath('.//text()')))
            if title in text or publish_time in text:
                self.parse_dict = {}
                res_dict = self.parseDOC(text_node)
                path = self.handle_data(res_dict)
                text_node = self.get_text_node(path)
                if not len(text_node):
                    return '', None
                text = self.handle_text("".join(text_node.xpath('.//text()')))
            return text, text_node
        except Exception as e:
            self.logger.info(e)
            return '', None

    def three_handle(self, text_node: str, title: str, publish_time: str):
        """
        处理文本函数
        """
        try:
            if publish_time is None:
                publish_time = '1000-01-01'
            text = self.handle_text("".join(text_node.xpath('.//text()')))
            if title in text or publish_time in text:
                self.parse_dict = {}
                res_dict = self.parseDOC(text_node)
                path = self.handle_data(res_dict)
                text_node = self.get_text_node(path)
                if not text_node:
                    return '', None
                text = self.handle_text("".join(text_node.xpath('.//text()')))
            return text, text_node
        except Exception as e:
            self.logger.info(e)
            return '', None

    def get_parent(self, current_node: str):
        """
        获取时间、作者、来源父节点
        """
        try:
            node = current_node
            while True:
                parent_node = node.xpath('./..')[0]
                childs = parent_node.xpath('.//*')
                for child in childs:
                    if child.tag in ["br"]:
                        childs.remove(child)
                if len(childs) <= 3:
                    node = parent_node
                else:
                    return parent_node
        except Exception as e:
            self.logger.info(e)
            print(f"{e}==获取标题父节点失败")
            return None

    def get_time(self, parent: str):
        """
        获取发布时间
        """
        try:
            flag = False
            publish = ""
            time_type = ["time", "date"]
            childs = parent.xpath('.//*')
            for child in childs:
                child_class = child.xpath('./@class | ./@id')
                for type in time_type:
                    for cld in child_class:
                        cld = cld.lower()
                        if type in cld:
                            publish = child.xpath('.//text()')
                            publish = self.handle_text(publish)
                            flag = True
                            break
                if flag:
                    break
            if publish:
                publish_time = self.parse_time(publish)
                if publish_time:
                    return publish_time
            text_time = parent.xpath('.//text()')
            text_time = self.handle_text(text_time)
            publish_time = self.parse_time(text_time)
            return publish_time
        except Exception as e:
            self.logger.info(e)
            print(f"{e}==获取发布时间失败")

    def parse_time(self, text: str):
        """
        解析时间
        """
        publish_time = None
        try:
            if re.search(".*?(\d+年\d+月\d+日.*?\d+\:\d+\:\d+).*", text):
                publish_time = re.search(".*?(\d+年\d+月\d+日.*?\d+\:\d+\:\d+).*", text)
                publish_time = publish_time.group(1).strip()
            elif re.search(".*?(\d+年\d+月\d+日.*?\d+\:\d+).*", text):
                publish_time = re.search(".*?(\d+年\d+月\d+日.*?\d+\:\d+).*", text)
                publish_time = publish_time.group(1).strip()
            elif re.search(".*?(\d+\-\d+\-\d+.*?\d+\:\d+\:\d+).*", text):
                publish_time = re.search(".*?(\d+\-\d+\-\d+.*?\d+\:\d+\:\d+).*?", text)
                publish_time = publish_time.group(1).strip()
            elif re.search(".*?(\d+\-\d+\-\d+.*?\d+\:\d+).*", text):
                publish_time = re.search(".*?(\d+\-\d+\-\d+.*?\d+\:\d+).*?", text)
                publish_time = publish_time.group(1).strip()
            elif re.search(".*?(\d+\-\d+\-\d+).*", text):
                publish_time = re.search(".*?(\d+\-\d+\-\d+).*?", text)
                publish_time = publish_time.group(1).strip()
            elif re.search(".*?(\d+年\d+月\d+日).*", text):
                publish_time = re.search(".*?(\d+年\d+月\d+日).*", text)
                publish_time = publish_time.group(1).strip()
            elif re.search(".*?(\d+\/\d+\/\d+.*?\d+\:\d+\:\d+).*", text):
                publish_time = re.search(".*?(\d+\/\d+\/\d+.*?\d+\:\d+\:\d+).*", text)
                publish_time = publish_time.group(1).strip()
            elif re.search(".*?(\d+\/\d+\/\d+.*?\d+\:\d+).*", text):
                publish_time = re.search(".*?(\d+\/\d+\/\d+.*?\d+\:\d+).*", text)
                publish_time = publish_time.group(1).strip()
            elif re.search(".*?(\d+\/\d+\/\d+).*", text):
                publish_time = re.search(".*?(\d+\/\d+\/\d+).*", text)
                publish_time = publish_time.group(1).strip()
            elif re.search("\d{4}\n", text):
                publish_time = re.sub("\n", "-", text)
                publish_time = re.sub("\s", "", publish_time)
            return publish_time
        except Exception as e:
            self.logger.info(e)
        finally:
            return publish_time

    def get_author(self, parent: str, detail_text: str):
        """
        获取作者
        """
        author = ""
        try:
            authors = ""
            flag = False
            author_type = ["author"]
            author_name = ["作者", "发布者", "编辑"]
            childs = parent.xpath('./*')
            for child in childs:
                child_class = child.xpath('./@class | ./@id')
                for type in author_type:
                    for cld in child_class:
                        cld = cld.lower()
                        if type in cld:
                            authors = child.xpath('.//text()')
                            authors = self.handle_text(authors)
                            flag = True
                            break
                if flag:
                    break
            if authors:
                for name in author_name:
                    author = re.search(".*{}(.*?)\n.*".format(name), authors)
                    if author:
                        author = author.group(1)
                        if author:
                            author = re.sub(":|：", "", author)
                            if author.strip():
                                # author = author.strip().split(" ")
                                author = author.strip()
                            else:
                                author = ""
                        else:
                            author = ""
                        return author
                # return authors.strip().split(" ")
                return authors
            author = self.parse_author(parent, detail_text, author_name, author)
        except Exception as e:
            self.logger.info(e)
            print(f"{e}==获取作者失败")
        finally:
            return author

    def parse_author(self, parent, detail_text, author_name, author):
        """
        处理作者
        """
        text = parent.xpath('.//text()')
        text = self.handle_text(text)
        text = re.sub(detail_text, "", text)
        for name in author_name:
            if re.search(".*{}(.*?)来源.*".format(name), text):
                author = re.search(".*{}(.*?)来源.*".format(name), text)
                author = author.group(1)
            elif re.search(".*{}(.*?)\n.*".format(name), text):
                author = re.search(".*{}(.*?)\n.*".format(name), text)
                author = author.group(1)
            elif re.search(".*{}(.*?)\s.*".format(name), text):
                author = re.search(".*{}(.*?)\s.*".format(name), text)
                author = author.group(1)
            elif re.search(".*{}(.*?).*".format(name), text):
                author = re.search(".*{}(.*?).*".format(name), text)
                author = author.group(1)
            if author:
                author = re.sub(":|：", "", author)
                if author.strip():
                    # author = author.strip().split(" ")
                    author = author.strip()
                    result = self.handle_punctuation(author)
                    if result:
                        author = ""
                else:
                    author = ""
                break
            else:
                author = ""
        return author

    def handle_punctuation(self, text_str):
        """
        处理标点符号
        """
        #英文标点
        if re.search(r"[|]", text_str):
            return True
        #中文标点
        chna = ["\u3002", "\uff1f", "\uff01", "\uff0c", "\uff1b", "\uff1a", "\u201c", "\u201d", "\u2018", "\u2019",
               "\uff08", "\uff09", "\u300a", "\u300b", "\u3008", "\u3009", "\u300e", "\u300f", "\u300c", "\u300d",
               "\ufe43", "\ufe44", "\u3014", "\u3015", "\u2026", "\u2014", "\uff5e", "\ufe4f", "\uffe5"]
        for punctuation in chna:
            if re.search(punctuation, text_str):
                return True
        return False

    def get_source(self, parent: str, detail_text: str):
        """
        获取信息来源
        """
        source = ""
        sources = ""
        try:
            flag = False
            source_type = ["source", "origin"]
            source_name = "来源"
            childs = parent.xpath('./*')
            for child in childs:
                child_class = child.xpath('./@class | ./@id')
                for type in source_type:
                    for cld in child_class:
                        cld = cld.lower()
                        if type in cld:
                            source = child.xpath('.//text()')
                            source = self.handle_text(source)
                            flag = True
                            break
                if flag:
                    break
            if source:
                sources = re.search(".*{}(.*?)\n.*".format(source_name), source)
                if sources:
                    sources = sources.group(1).strip()
                    sources = re.sub(":|：", "", sources)
                    return sources
                return source
            source = self.parse_source(parent, detail_text, source_name, sources)
        except Exception as e:
            self.logger.info(e)
            print(f"{e}==获取来源失败")
        finally:
            return source

    def parse_source(self, parent, detail_text, source_name, sources):
        """
        处理来源
        """
        text = parent.xpath('.//text()')
        text = self.handle_text(text)
        text = re.sub(detail_text, "", text)
        if re.search(".*{}(.*?)作者.*".format(source_name), text):
            sources = re.search(".*{}(.*?)作者.*?".format(source_name), text)
            sources = sources.group(1).strip()
        elif re.search(".*{}(.*?)时间.*".format(source_name), text):
            sources = re.search(".*{}(.*?)时间.*".format(source_name), text)
            sources = sources.group(1).strip()
        elif re.search(".*{}(.*?)\s.*".format(source_name), text):
            sources = re.search(".*{}(.*?)\s.*".format(source_name), text)
            sources = sources.group(1).strip()
        elif re.search(".*{}(.*?)[a-zA-Z].*".format(source_name), text):
            sources = re.search(".*{}(.*?)[a-zA-Z].*".format(source_name), text)
            sources = sources.group(1).strip()
        elif re.search(".*{}(.*?)\d.*".format(source_name), text):
            sources = re.search(".*{}(.*?)\d.*".format(source_name), text)
            sources = sources.group(1).strip()
        elif re.search(".*{}(.*?)\n.*".format(source_name), text):
            sources = re.search(".*{}(.*?)\n.*".format(source_name), text)
            sources = sources.group(1).strip()
        if sources:
            sources = re.sub(":|：", "", sources)
            result = self.handle_punctuation(sources)
            if result:
                sources = ""
            else:
                sources = sources.split(" ")[0]
        return sources

    def get_images(self, parent: str, url: str):
        """
        获取图片
        """
        images = []
        if parent is None or not len(parent):
            return images
        try:
            is_p = parent.xpath('.//p')
            if is_p:
                imgs = parent.xpath('.//p//img/@src')
                if not imgs:
                    imgs = parent.xpath('.//img/@src')
            else:
                imgs = parent.xpath('.//img/@src')
            for img in imgs:
                if str(img).endswith(("jpg", "png")):
                    img = self.change_url(url, img)
                    images.append(img)
            return images
        except Exception as e:
            self.logger.info(e)
            print(f"{e}==获取图片失败")

    def get_title(self):
        """
        提取标题
        """
        title = ""
        node = ""
        try:
            if "-" in self.head_title:
                title_ch = self.head_title.split("-")
            elif "_" in self.head_title:
                title_ch = self.head_title.split("_")
            else:
                title_ch = self.head_title.split()
            title_ch = self._max_list(title_ch)
            if "|" in title_ch:
                title_ch = self._max_list(title_ch.split("|"))
            title_len = len(title_ch) / 2
            if self.h_list:
                sort_h = sorted(self.h_list, key=lambda k: k["sort_num"])
                for h in sort_h:
                    h_text = h["text"]
                    if h_text and h_text in self.head_title and len(h_text.strip()) >= title_len:
                        title = h_text
                        node = h["node"]
                        break
            if not title:
                for k, v in self.title_dict.items():
                    if len(v.strip()) >= title_len and k.tag != "a":
                        if v in title_ch:
                            title = v
                            node = k
                            break
                        else:
                            v_text = self.handle_text((v,))
                            title_text = self.handle_text((title_ch,))
                            if v_text and v_text in title_text:
                                title = v
                                node = k
                                break
            if not title and self.h_list:
                for h in self.h_list:
                    if h["text"]:
                        title = h["text"]
                        node = h["node"]
                        break
        except Exception as e:
            self.logger.info(e)
            print(f"{e}==提取标题失败")
        finally:
            return title, node

    def get_text_node(self, path: list):
        """
        获取正文
        params: path 最大路径
        return: text_node 正文节点
        """
        text_node = ""
        try:
            new_path = path[::-1]
            for node in new_path:
                if list(node.keys())[0].tag.lower() in ["p", "span", "ul", "i", "em", "a", "li", "font", "strong", "sub"]:
                    continue
                else:
                    text_node = list(node.keys())[0]
                    break
        except Exception as e:
            self.logger.info(f"{e}==获取正文节点失败")
            print(f"{e}==获取正文节点失败")
        finally:
            return text_node

    def get_diff(self, data: list):
        """
        计算相邻元素的最大差值
        params: data 数据列表
        return: diff_tuple 最大差值对元组
        """
        try:
            max_diff = 0
            diff_tuple = ()
            diff_list = []
            for i in range(len(data) - 1):
                diff_list.append((int(data[i]), int(data[i + 1])))
            for diff in diff_list:
                if diff[0] - diff[1] > max_diff:
                    diff_tuple = diff
                    max_diff = diff[0] - diff[1]
            return diff_tuple
        except Exception as e:
            self.logger.error(e)

    def parseDOC(self, node: str):
        """
        递归提取node节点中的特征值
        params: node 节点
        return:
        """
        try:
            self.is_content(node)
            self.parse_title(node)
            self.parseH(node)
            parse_all = node.xpath('.//text()')
            parse_a = node.xpath('.//a//text()')
            all_len = len(self.handle_text(text=(parse_all,)))
            a_len = len(self.handle_text(text=(parse_a,)))
            length = all_len - a_len
            self.parse_dict[node] = str(length)
            node_child = node.xpath('./*')
            for child in node_child:
                self.parseDOC(child)
            return self.parse_dict
        except Exception as e:
            self.logger.error(e)

    def parseH(self, node: str):
        """
        提取h类标签
        params: node_div节点
        return: list(h类标签)
        """
        try:
            h1_label = node.xpath('./h1')
            h2_label = node.xpath('./h2')
            h3_label = node.xpath('./h3')
            h4_label = node.xpath('./h4')
            if h1_label:
                for h1 in h1_label:
                    h1_text = h1.xpath('.//text()')
                    if h1_text:
                        h1_text = self.handle_text(h1_text)
                    else:
                        h1_text = ""
                    self.h_list.append({"text": h1_text, "node": h1, "sort_num": 1})
            if h2_label:
                for h2 in h2_label:
                    h2_text = h2.xpath('.//text()')
                    if h2_text:
                        h2_text = self.handle_text(h2_text)
                    else:
                        h2_text = ""
                    self.h_list.append({"text": h2_text, "node": h2, "sort_num": 2})
            if h3_label:
                for h3 in h3_label:
                    h3_text = h3.xpath('.//text()')
                    if h3_text:
                        h3_text = self.handle_text(h3_text)
                    else:
                        h3_text = ""
                    self.h_list.append({"text": h3_text, "node": h3, "sort_num": 3})
            if h4_label:
                for h4 in h4_label:
                    h4_text = h4.xpath('.//text()')
                    if h4_text:
                        h4_text = self.handle_text(h4_text)
                    else:
                        h4_text = ""
                    self.h_list.append({"text": h4_text, "node": h4, "sort_num": 4})
        except Exception as e:
            self.logger.info(f"{e}==提取h类标签错误")
            print(f"{e}==提取h类标签错误")

    def parse_title(self, node: str):
        """
        解析标题
        params: node节点
        return:
        """
        text = node.xpath('./text()')
        text = "".join(text).strip()
        if text:
            self.title_dict[node] = text

    def handle_data(self, datas: dict):
        """
        处理数据的特征值，找出最大p节点路径
        params: datas p字典
        return: max_path
        """
        max_path = []
        try:
            if isinstance(datas, dict):
                body_key = list(datas.keys())[0]
                while True:
                    max_dic = {}
                    childs = body_key.xpath("./*")
                    if len(childs) == 0:
                        break
                    for child in childs:
                        max_dic[child] = datas[child]
                    max_key = max(max_dic, key=lambda k: int(max_dic[k]) if isinstance(max_dic[k], str) else max_dic[k])
                    is_existence_node = int(datas[max_key])
                    if is_existence_node == 0:
                        break
                    max_path.append({max_key: datas[max_key]})
                    body_key = max_key
                return max_path
        except Exception as e:
            self.logger.info(f"{e}==处理特征值失败")
        finally:
            return max_path

    def handle_text(self, text):
        """
        处理文本,去除特殊字符
        params: text 待处理文本
        return: result 文本
        """
        try:
            if not text:
                return ""
            if isinstance(text, list):
                # result = "".join(text)
                result = re.sub(r"[\t \r \xa0 \u2003 \u3000 \u2022]|<.*?>", " ", "".join(text).strip())
            elif isinstance(text, str):
                text = text.split("\n")
                text = [t+"\n" for t in text if t.strip()]
                result = re.sub(r"[\t \r \xa0 \u2003 \u3000 \u2022]|<.*?>", " ", "".join(text).strip())
            else:
                text = "".join(text[0]).strip()
                result = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "", text)
            return result
        except Exception as e:
            self.logger.info(f"{e}==处理文本错误")

    def is_content(self, node: str):
        """
        判断节点中是否含有class并且是否含有特征类("content...")
        params: node 节点
        return:
        """
        try:
            content_class = node.xpath("./@class | ./@id")
            for label in content_class:
                for feature in ["content", "article", "text"]:
                    if feature in label.lower():
                        self.content.append(node)
        except Exception as e:
            self.logger.error(e)

    def max_dict(self, data: dict):
        """
        处理字典数据，返回最大value值的key
        """
        try:
            if not data:
                return []
            max_list = []
            max_key = max(data, key=lambda k: int(data[k]))
            max_value = data[max_key]
            for k, v in data.items():
                if v == max_value:
                    max_list.append(k)
            return max_list[-1]
        except Exception as e:
            self.logger.info(e)

    def _max_list(self, datas: list):
        """
        处理列表数据， 返回最大字符串值
        """
        max_length = 0
        max_str = ""
        try:
            for data in datas:
                if len(data) > max_length:
                    max_length = len(data)
                    max_str = data
        except Exception as e:
            self.logger.info(e)
            print(e)
        finally:
            return max_str

    def max_list(self, datas: list):
        """
        处理列表字典数据，返回最大value值
        """
        data_list = list()
        clear_list = list()
        try:
            list_data = sorted(datas, key=lambda k: k["text_len"], reverse=True)
            for data in list_data:
                xpath_id = data["xpath"]["id"]
                xpath_class = data["xpath"]["class"]
                if xpath_id or xpath_class:
                    if xpath_id not in clear_list and xpath_class not in clear_list:
                        data_list.append(data)
                        if xpath_id:
                            clear_list.append(xpath_id)
                        if xpath_class:
                            clear_list.append(xpath_class)
        except Exception as e:
            self.logger.info(e)
        finally:
            return data_list

    def parse_html(self, datas: list, url: str):
        """
        解析html，获取数据
        """
        try:
            self.parse_dict = {}
            self.title_dict = {}
            self.h_list = []
            text = ""
            text_node = None
            html, re_url = self.get_html(url)
            if html is None:
                return None
            head = self.filter_html(html, False)
            body = self.filter_html(html)
            self.parseDOC(body)
            self.head_title = self.handle_text(head.xpath('./title//text()'))
            for data in datas:
                if data["xpath"]["id"]:
                    id = data["xpath"]["id"][0]
                    xpath = '//*[@id="{}"]//text()'.format(id)
                    text = body.xpath(xpath)
                    text_node = body.xpath('//*[@id="{}"]'.format(id))
                    if text_node:
                        text_node = text_node[0]
                elif data["xpath"]["class"]:
                    classify = data["xpath"]["class"][0]
                    xpath = '//*[@class="{}"]//text()'.format(classify)
                    text = body.xpath(xpath)
                    text_node = body.xpath('//*[@class="{}"]'.format(classify))
                    if text_node:
                        text_node = text_node[0]
                if text:
                    break
            text = self.handle_text("".join(text))
            title, node = self.get_title()
            if not title:
                return None
            # 时间、作者、来源父节点
            parent_node = self.get_parent(node)
            # 时间
            publish_time = self.get_time(parent_node)
            if not publish_time:
                publish_time = self.get_time(parent_node.xpath('./..')[0])
            # 作者
            author = self.get_author(parent_node, text)
            # 来源
            source = self.get_source(parent_node, text)
            # 图片
            image = self.get_images(text_node, re_url)
            uid = md5(re_url.encode("utf-8")).hexdigest()
            datas = {
                "uid": uid,
                "title": title,
                "url": re_url,
                "publish_time": publish_time,
                "author": author,
                "source": source,
                "image": image,
                "text": text
            }
            return datas
        except Exception as e:
            self.logger.info(e)
            print(f"{e}==获取数据失败")
            return None

    def save_datas(self, datas, label: list, db: str):
        """
        保存数据
        """
        update_time = int(time.time() * 1000)
        self.spider_writer[db].update_one(
            {"uid": datas["uid"]},
            {"$set": {
                "label": label,
                **datas,
            },
                "$setOnInsert": {
                    "update_time": update_time
                }
            },
            upsert=True)

    def run(self):
        url_list = self.parse_url(self.url, self.classify)
        result = self.filter_url(self.classify_list)
        result_list = self.filter_classify(result)
        res = self.filter_url(url_list)
        classify = self.get_classify(result_list, res)
        for sify in classify:
            a_list, re_url = self.get_content(sify)
            href = self.filter_a(a_list, re_url, sify["label"])
            if href is None:
                continue
            detail_list = []
            for url in href:
                print(url)
                if not self.turn_content["type"] or not self.turn_content["symbol"]:
                    detail = self.analysis_content(url)
                    if not detail:
                        href.remove(url)
                        continue
                    detail_list.append(detail)
                    print(detail)
                else:
                    n = self.turn_content["start"]
                    base_url = re.sub(self.turn_content["type"], "{}"+self.turn_content["type"], url)
                    while True:
                        if n == 0:
                            url = base_url.format("")
                            detail = self.analysis_content(url)
                        else:
                            url = base_url.format(self.turn_content["symbol"]+str(n+1))
                            detail = self.analysis_content(url)
                        print(url)
                        if url not in href:
                            href.append(url)
                        if not detail:
                            href.remove(url)
                            break
                        if detail["text_len"] == 0:
                            break
                        detail_list.append(detail)
                        n += 1
                        print(detail)
            data_list = self.max_list(detail_list)
            for url in href:
                print(url)
                datas = self.parse_html(data_list, url)
                if not datas:
                    continue
                self.save_datas(datas, label=sify["label"], db=self.folder)
                try:
                    print(datas)
                except:
                    pass

if __name__ == "__main__":
    try:
        config_json = "zys.json"
        general = GeneralCrawlSpider(config_json=config_json)
        general.run()
    except Exception as err:
        frame = sys._getframe()
        current_time = time.time()
        asctime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(current_time))
        millisecond = (current_time - int(current_time)) * 1000
        filename = os.path.basename(frame.f_code.co_filename)
        print("%s,%03d %s[line:%d] ERROR: %s" % (asctime, millisecond, filename, frame.f_lineno, err))