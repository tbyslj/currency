# -*- coding: utf-8 -*-
"""
    Author:    tang
    EMail:     2692400059@qq.com
    Date:      2021-02-01
"""
import requests
from http import cookiejar
from gli.logger import Logger


class HttpRequest(object):

    def __init__(self, log=Logger(folder="http_request"), retry: int = 3, timeout: int = 5):
        """Initialize HttpRequest
        Args:
            log:     Log system.
            retry:   Retry count after the request failed, default value 3.
            timeout: Http request timeout, default value 5.
        """
        self.logger = log
        self.retry: int = retry
        self.timeout: int = timeout

    def request(self, method: str, url: str, **kwargs):
        """Sends a request.
        Args:
            :param method: Request method ("GET","POST","PUT","PATCH","DELETE").
            :param url:    URL for the request.
            :param kwargs: Optional arguments.
                session:   (optional) Session object.
                headers:   (optional) Dictionary of HTTP Headers to send.
                cookies:   (optional) Dict or CookieJar object to send.
                proxy:     (optional) Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
                params:    (optional) Dictionary or bytes to be sent.
                data:      (optional) Dictionary, list of tuples, bytes, or file-like object to send.
                json:      (optional) Json to send in the body.
                files:     (optional) Dictionary of "'filename': file-like-objects" for multipart encoding upload.
                timeout:   (optional) How long to wait for the server to send data before giving up, as a float, or a :ref:"(connect timeout, read timeout) <timeouts>" tuple.
                redirect:  (optional) Allow redirects.
        Returns:
            :return Success returns response, otherwise None.
        """
        for i in range(0, self.retry):
            headers = {}
            proxies = {}
            try:
                session = kwargs.get("session") if isinstance(kwargs.get("session"), requests.Session) else requests.session()
                # HTTP request
                response = session.request(method.upper(), url,
                    headers         = kwargs.get("headers")  if isinstance(kwargs.get("headers"),                       dict)  else None,
                    cookies         = kwargs.get("cookies")  if isinstance(kwargs.get("cookies"), (cookiejar.CookieJar, dict)) else None,
                    proxies         = kwargs.get("proxies")  if isinstance(kwargs.get("proxies"),                       dict)  else None,
                    params          = kwargs.get("params")   if isinstance(kwargs.get("params"),                        dict)  else None,
                    data            = kwargs.get("data")     if isinstance(kwargs.get("data"),                          dict)  else None,
                    json            = kwargs.get("json")     if isinstance(kwargs.get("json"),                          dict)  else None,
                    files           = kwargs.get("files")    if isinstance(kwargs.get("files"),                         dict)  else None,
                    timeout         = kwargs.get("timeout")  if isinstance(kwargs.get("timeout"),         (int, float, tuple)) else self.timeout,
                    allow_redirects = kwargs.get("redirect") if isinstance(kwargs.get("redirect"),                      bool)  else True)
                # status code
                if response.status_code == requests.codes.ok:
                    return response
                else:
                    response.raise_for_status()
            except Exception as err:
                self.logger.error(f"[{i + 1}/{self.retry}] {err}")
        return None

    def get(self, url: str, params: dict = None, **kwargs):
        """Sends a GET request.
        Args:
            :param url:    URL for the request.
            :param kwargs: Optional arguments.
                session:   (optional) Session object.
                headers:   (optional) Dictionary of HTTP Headers to send.
                cookies:   (optional) Dict or CookieJar object to send.
                proxy:     (optional) Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
                params:    (optional) Dictionary or bytes to be sent.
                timeout:   (optional) How long to wait for the server to send data before giving up, as a float, or a :ref:"(connect timeout, read timeout) <timeouts>" tuple.
                redirect:  (optional) Allow redirects.
        Returns:
            :return Success returns response, otherwise None.
        """
        kwargs.update({"params": params})
        return self.request(method="GET", url=url, **kwargs)

    def post(self, url: str, data: dict = None, json: dict = None, **kwargs):
        """Sends a POST request.
        Args:
            :param url:    URL for the request.
            :param kwargs: Optional arguments.
                session:   (optional) Session object.
                headers:   (optional) Dictionary of HTTP Headers to send.
                cookies:   (optional) Dict or CookieJar object to send.
                proxy:     (optional) Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
                data:      (optional) Dictionary, list of tuples, bytes, or file-like object to send.
                json:      (optional) Json to send in the body.
                timeout:   (optional) How long to wait for the server to send data before giving up, as a float, or a :ref:"(connect timeout, read timeout) <timeouts>" tuple.
        Returns:
            :return Success returns response, otherwise None.
        """
        kwargs.update({"data": data, "json": json})
        return self.request(method="POST", url=url, **kwargs)

    def put(self, url:str, data: dict = None, **kwargs):
        """Sends a PUT request.
        Args:
            :param url:    URL for the request.
            :param kwargs: Optional arguments.
                session:   (optional) Session object.
                headers:   (optional) Dictionary of HTTP Headers to send.
                cookies:   (optional) Dict or CookieJar object to send.
                proxy:     (optional) Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
                data:      (optional) Dictionary, list of tuples, bytes, or file-like object to send.
                timeout:   (optional) How long to wait for the server to send data before giving up, as a float, or a :ref:"(connect timeout, read timeout) <timeouts>" tuple.
        Returns:
            :return Success returns response, otherwise None.
        """
        kwargs.update({"data": data})
        return self.request(method="PUT", url=url, **kwargs)

    def patch(self, url: str, data: dict = None, **kwargs):
        """Sends a PATCH request.
        Args:
            :param url:    URL for the request.
            :param kwargs: Optional arguments.
                session:   (optional) Session object.
                headers:   (optional) Dictionary of HTTP Headers to send.
                cookies:   (optional) Dict or CookieJar object to send.
                proxy:     (optional) Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
                data:      (optional) Dictionary, list of tuples, bytes, or file-like object to send.
                timeout:   (optional) How long to wait for the server to send data before giving up, as a float, or a :ref:"(connect timeout, read timeout) <timeouts>" tuple.
        Returns:
            :return Success returns response, otherwise None.
        """
        kwargs.update({ "data": data })
        return self.request(method="PATCH", url=url, **kwargs)

    def delete(self, url: str, **kwargs):
        """Sends a DELETE request.
        Args:
            :param url:    URL for the request.
            :param kwargs: Optional arguments.
                session:   (optional) Session object.
                headers:   (optional) Dictionary of HTTP Headers to send.
                cookies:   (optional) Dict or CookieJar object to send.
                proxy:     (optional) Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
                timeout:   (optional) How long to wait for the server to send data before giving up, as a float, or a :ref:"(connect timeout, read timeout) <timeouts>" tuple.
        Returns:
            :return Success returns response, otherwise None.
        """
        return self.request(method="DELETE", url=url, **kwargs)

