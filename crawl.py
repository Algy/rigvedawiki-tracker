#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

from urllib2 import Request, urlopen, quote
from bs4 import BeautifulSoup, NavigableString
from pprint import pprint

class UnexpectedFormatError(Exception):
    pass

def is_whitespace(s):
    return s == " " or s == "\n" or s == "\t"

def spoofing_urlopen(url):
    headers = {
        "Accept": "text/html, application/xhtml+xml",
        "Accept-Language": "ko-KR",
        "User-Agent": "Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11",
        "Accept-Encoding": "deflate"
    }
    req = Request(url, headers=headers)
    return urlopen(req)

class BaseLogItem:
    def get_id(self):
        raise NotImplementedError

    def equal_log(self, other):
        raise NotImplementedError


class RigvedaLogItem(BaseLogItem):
    def __init__(self, cmd, article, author, time_magic, edit_num=None, comment=None, diff=None):
        self.cmd = cmd
        self.gathered_at = None
        self.article = article
        self.author = author
        self.time_magic = time_magic
        self.edit_num = edit_num
        self.comment = comment
        self.diff = diff
    
    def get_id(self):
        return self.article

    def equal_log(self, other):
        return self.get_id() == other.get_id() and \
               self.cmd == other.cmd and \
               self.edit_num == other.edit_num

    def show(self):
        print self.cmd, self.article, "By", self.author, "at", self.time_magic
        if self.comment:
            print "Comment>"
            print self.comment
        if self.diff:
            print "==="
            for x in self.diff:
                print "In", x["area"]
                content = x["content"]
                if len(content) > 40:
                    content = content[:40] + "..."
                print content
            print "<--->"

    def __repr__(self):
        return "RigvedaLogItem({cmd}, {article})"\
                .format(cmd=self.cmd, article=self.article.encode("utf-8"))


def cmd_from_img_src(img_src):
    if "diff" in img_src:
        return "modify"
    elif "attach" in img_src:
        return "attach"
    elif "deleted" in img_src:
        return "delete"
    else:
        return "unknown"


def parse_row(tr, log_html=None):
    cmd = unicode(cmd_from_img_src(tr.img["src"]))
    article = unicode(tr.select("td.title")[0].a.span.string)
    author = unicode(tr.select("td.author span.rc-editors span.editor")[0].string)
    try:
        edit_num = int(tr.select("td.editinfo span.num")[0].string)
    except IndexError:
        edit_num = None
    time_magic = unicode(tr.select("td.date")[0].string)

    if log_html:
        try:
            log_small = log_html.select("small[name=word-break]")[0]
            comment = "".join(filter(lambda x: isinstance(x, NavigableString), log_small.contents))
        except IndexError:
            comment = ""
    else:
        comment = None
    return RigvedaLogItem(cmd, article, author, time_magic, edit_num, comment)

def parse_tr_list(tr_list):
    tr_len = len(tr_list)
    idx = 0
    result = []
    while idx < tr_len:
        tr = tr_list[idx]

        try:
            tr_class = tr['class'][0]
        except KeyError:
            tr_class = None

        if tr_class is None or tr_class == 'alt':
            log_html = None
            if idx + 1 < tr_len:
                try:
                    next_tr_class = tr_list[idx + 1]["class"][0]
                    if next_tr_class == "log":
                        log_html = tr_list[idx + 1]
                except KeyError:
                    log_html = None
            result.append(parse_row(tr, log_html))

            cmd_img_src = tr.img["src"]
        idx += 1
    return result


def collect_items(src):
    '''
    string -> BaseLogitem iter 
    '''
    soup = BeautifulSoup(src)
    lst = soup.select("div.recentChanges")
    if len(lst) == 0:
        raise UnexpectedFormatError
    tr_list = lst[-1].select("tbody")[0].select("tr")
    return parse_tr_list(tr_list)


def compare_logs(old_lst, new_lst):
    old_idx = 0

    old_map = dict((e.get_id(), idx) for idx, e in enumerate(old_lst))
    for new_log in new_lst:
        if new_log.get_id() not in old_map:
            yield new_log
        else:
            # Invariant: old_lst[old_idx].get_id() in old_map 
            old_log = old_lst[old_idx]
            if new_log.equal_log(old_log):
                break
            else:
                old_map.pop(old_lst[old_map[new_log.get_id()]].get_id())
                while old_idx < len(old_lst) and \
                      old_lst[old_idx].get_id() not in old_map:
                    old_idx += 1
                yield new_log

def get_recentchanges():
    src = spoofing_urlopen("http://rigvedawiki.net/r1/wiki.php/RecentChanges").read()
    return collect_items(src)


def get_diff(article):
    def escape(s):
        def tr(c):
            if c == '\\':
                return '\\\\'
            else:
                return c
        return "".join(tr(c) for c in s)

    if isinstance(article, unicode):
        article = article.encode("utf-8")
    src = spoofing_urlopen(
            "http://rigvedawiki.net/r1/wiki.php/%s?action=diff"%quote(article)).read()
    soup = BeautifulSoup(src)
    t = soup.select("div.fancyDiff")
    if len(t) == 0:
        raise UnexpectedFormatError
    fancyDiff = t[0]

    result = []
    temp = u""
    area = None

    for child in fancyDiff.contents:
        if isinstance(child, NavigableString):
            temp += escape(unicode(child).strip())
            continue
        elif child.name == "br":
            temp += u"\n"
            continue
        elif child.name == "h2":
            continue

        try:
            child_classes = child["class"]
            child_class = child_classes[0]
        except IndexError:
            child_class = None
        except KeyError:
            child_class = None

        if child_class == "diff-sep":
            if area is not None:
                result.append({"area": area, "content": temp})
            area = unicode(child.string)
            temp = u""
        elif child_class == "diff-added":
            content = child
            if child.select("div.diff-added"):
                content = child.select("div.diff-added")[0]
            temp += u"\\+"
            for x in content:
                if isinstance(x, NavigableString):
                    temp += escape(unicode(x).strip())
                elif x.name == "br":
                    temp += u"\n"
                elif x.name == "ins":
                    temp += u"\\>" + escape(unicode(x.string)) + u"\\<"

            temp += u"+\\"
            temp += u"\n"
        elif child_class == "diff-removed":
            content = child
            if child.select("div.diff-removed"):
                content = child.select("div.diff-removed")[0]
            temp += u"\\-"
            for x in content:
                if isinstance(x, NavigableString):
                    temp += escape(unicode(x).strip())
                elif x.name == "br":
                    temp += u"\n"
                elif x.name == "del":
                    temp += u"\\>" + escape(unicode(x.string)) + u"<\\"
            temp += u"-\\"
            temp += u"\n"
        else:
            content = child
            temp += escape(content.string)
    if area is not None:
        result.append({"area": area, "content": temp})
    return result
    
def get_epoch():
    return time.time()


def crawl_once(old, store_fun=None, publish_fun=None):
    new = get_recentchanges()
    if old is None:
        return new
    fresh_logs = list(compare_logs(old, new))

    for log in fresh_logs[::-1]:
        log.diff = get_diff(log.article)
        # log.diff = "no diff"
        log.gathered_at = get_epoch()
    if store_fun:
        store_fun(fresh_logs)
    if publish_fun:
        publish_fun(fresh_logs)
    return new


def _test_have_a_while():
    from time import sleep
    old = get_recentchanges()

    def publish_to_screen(logs):
        for log in logs:
            log.show()
    while True:
        sleep(3)
        old = crawl_once(old, publish_to_screen, None)

if __name__ == "__main__":
    _test_have_a_while()
