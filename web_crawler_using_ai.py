#!/usr/bin/env python
"""\
Web crawler using ai: given root url, crawl web pages below within same domain and return result in markdown format.
"""
__author__ = "Ram P"
__email__ = "rambpol@yahoo.com"
__version__ = "0.1.0"
# __maintainer__ = __author__
# __status__ = "Development"
# __copyright__  = "Copyright 2023, " + __author__

import os
import sys
import argparse
from urllib.parse import urlparse
from tldextract import extract as tld_extract
from pydantic import BaseModel, HttpUrl, ValidationError
import uuid
from crawl4ai.content_filter_strategy import BM25ContentFilter, PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
# from crawl4ai.rate_limit_strategy import RateLimitConfig
import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlResult

import logging
LOGGER_NAME =  os.path.splitext(os.path.basename(__file__))[0]
logger = logging.getLogger(name=LOGGER_NAME) # root logger by default, pass LOGGER_NAME for script specific log file
logging.raiseExceptions = False

WEBSITE_TO_CRAWL_DEFAULT = 'https://crawl4ai.com'
WEBCRAWL_MAX_DEPTH_DEFAULT = 3
CRAWL4AI_BROWSER_DATA_DIR = "./cral4ai_browser_data"  # to persist cookies, etc.

import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode #, RateLimitConfig

async def main(args):
    # Configure the browser
    browser_cfg = BrowserConfig(
        browser_type="chromium",  # or "firefox" or "webkit"
        headless=True,
        viewport_width=1280,
        viewport_height=720,
        # proxy="http://user:pass@myproxy:8080",
        use_persistent_context=True,  # to persist cookies, etc.
        user_data_dir=CRAWL4AI_BROWSER_DATA_DIR,  # to persist cookies, etc.
        ignore_https_errors=True,   # dont care if invalid certificates when crawling web pages
        java_script_enabled=True,  # Set to False to disable javascript
        cookies=[],  # list of cookies to be used in the browser: each a dict like {"name": "session", "value": "...", "url": "..."}
        headers={},  # Set headers to be used in the browser
        text_mode=False, # Set to True to disable images, css, fonts, etc.  (want images)
    )

    # Configure the run
    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        session_id="crawl4ai_session_" + get_root_domain(args.ROOT_URL),  # + str(uuid.uuid1()),  # unique session id based on network address and time of the machine it is running on
        css_selector=None, # "main.article",
        excluded_selector="script, style, nav, footer",
        excluded_tags=["script", "style", "nav", "footer"],
        keep_data_attributes=True, # want to keep data-* attributes
        # wait_for="css:.main-content",  # condition before content extraction: wait for a CSS ("css:selector") like "css:.article-loaded", or JS ("js:() => bool") 
        wait_for_images=True,
        delay_before_return_html=0.5,  # 0 seconds
        check_robots_txt=True, # respect robots.txt rules
        wait_until="networkidle",  # "networkidle0" or "networkidle2" or "load" or "domcontentloaded"
        page_timeout=60000,  # milliseconds
        semaphore_count=5,  # number of pages to crawl concurrently
        # max_depth=args.MAX_DEPTH,  # maximum depth of pages to crawl
        # word_count_threshold=15,
        screenshot=True,
        # enable_rate_limiting=True,
        # rate_limit_config=RateLimitConfig(
            # base_delay=(1.0, 3.0),
            # max_delay=60.0,
            # max_retries=3,
            # rate_limit_codes=[429, 503],
        # ),
        # memory_threshold_percent=70.0,
        # check_interval=1.0,
        # max_session_permit=20,
        # display_mode="DETAILED",
        # stream=True,
        js_code = None,  # JavaScript to run after load. E.g. "document.querySelector('button')?.click();" or "window.scrollTo(0, document.body.scrollHeight);"
        js_only = False,  # Set to True to run only JavaScript code
        ignore_body_visibility=False,  # Set to True to ignore body visibility
        scan_full_page=False,  # Set to False to scan only the visible part of the page
        scroll_delay=0.5,  # 0.5 seconds between scrolls when full page is not visible
        process_iframes=True,  # Set to False to ignore iframes
        remove_overlay_elements=True,  # Set to False to keep overlay elements
        simulate_user=True,  # Set to False to disable user simulation (to avoid bot detection)
        override_navigator=True,  # Set to False to use the default navigator object (Override navigator properties in JS for stealth.)
        magic=True,  # Set to False to disable magic mode: Automatic handling of popups/consent banners. Experimental.
        image_score_threshold=0.5,  # Image score threshold for image extraction
        exclude_external_images=True,  # Set to False to keep external images
        # exclude_social_media_domains=[],  # A default list can be extended. Any link to these domains is removed from final output.
        exclude_external_links=True,  # Set to False to keep external links
        exclude_social_media_links=True,  # Set to False to keep social media links
        exclude_domains=[],  # A default list can be extended. Any link to these domains is removed from final output.
        extraction_strategy=None,  # "READABILITY" or "GOOSE" or "NEWSPAPER" or "NONE"
        # extraction_strategy_config=None,  # {"readability": {"min_text_length": 100}}
        # extraction_strategy_config={"goose": {"use_meta_language": False}},
        # extraction_strategy_config={"newspaper": {"use_meta_language": False}},
        # markdown_generator="MARKDOWNIT",  # "MARKDOWNIT" or "MISTUNE" or "MARKDOWN"
        markdown_generator=DefaultMarkdownGenerator(),
        # markdown_generator_config={"mistune": {"escape": False}},
        # markdown_generator_config={"markdown": {"escape": False}},
        content_filter = BM25ContentFilter(
            user_query=args.USER_QUERY,  # User query to adjust BM25 scores
            # Adjust for stricter or looser results
            bm25_threshold=1.2  
        ) if args.USER_QUERY else None,
        # PruningContentFilter(
        #     min_text_length=100,    # Minimum text length in a paragraph
        #     min_word_count=5,       # Minimum word count in a paragraph (after removing stop words)
        #     min_sentence_count=1,   # Minimum sentence count in a paragraph (after removing stop words)
        #     min_paragraph_count=1,  # Minimum paragraph count in the content
        #     min_content_length=1000,  # Minimum content length
        #     min_content_word_count=100,  # Minimum content word count
        #     min_content_sentence_count=5,  # Minimum content sentence count
        #     min_content_paragraph_count=5,  # Minimum content paragraph count
        # )
        verbose=True,
        log_console=False, # Set to True to log javascript console output
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        # Process results as they complete
        results = await crawler.arun_many(
            urls=[args.ROOT_URL],
            max_depth=args.MAX_DEPTH,
            config=run_cfg
        )
        for result in results:
            print(f"Completed url: {result.url}")
            if result.success:
                # Process each result immediately
                # process_result(result)
                print(f"cleaned_html length for url {result.url}: ", len(result.cleaned_html))
                print(f"Markdown content of url {result.url}:\n", result.markdown)
            else:
                print(f"Crawl failed for url {result.url}:", result.error_message)

    return 0

class UrlValidator(BaseModel):
    url: HttpUrl

def validate(url: str):
    try:
        UrlValidator(url=url)
    except ValidationError as e:
        logger.fatal(f"Exception validating url <{url}>: {e}")
        raise e
    logger.info(f"validated url: <{url}>")


def get_root_domain(root_url):
    validate(root_url)
    root_url_parsed = urlparse(root_url)
    if not root_url_parsed.scheme or not root_url_parsed.netloc or not root_url_parsed.hostname:
        logger.exception(f'Invalid root_url=<{root_url}>: empty scheme or domain/server or hostname')
        raise ValueError(f'Invalid root_url=<{root_url}>: empty scheme or domain/server or hostname')
    if root_url_parsed.scheme == root_url_parsed.hostname:
        logger.exception(f'Invalid root_url=<{root_url}>: scheme and hostname are same')
        raise ValueError(f'Invalid root_url=<{root_url}>: scheme and hostname are same')
    
    root_domain = root_url_parsed.netloc
    tld_extract_result = tld_extract(root_url)
    root_domain = tld_extract_result.domain + '.' + tld_extract_result.suffix
    logger.info(f'root_domain for url=<{root_url}> = <{root_domain}>')
    return root_domain

def get_args():
    """ Get command line arguments """
    parser = argparse.ArgumentParser()
    # parser.add_argument('-o', '--OUTPUT_DIR', help="output files directory", default=OUTPUT_DIR_DEFAULT)
    parser.add_argument('-u', '--ROOT_URL', help="root url of webpage or website to be crawled", default = WEBSITE_TO_CRAWL_DEFAULT)
    # parser.add_argument('-t', '--ROOT_URL_TLD_CRAWL_FLAG', help="flag to indicate to crawl all of the pages matching top level domain of url", required=False)
    parser.add_argument('-d', '--MAX_DEPTH', help="depth of web pages to crawl", default=WEBCRAWL_MAX_DEPTH_DEFAULT, required=False)
    parser.add_argument('-q', '--USER_QUERY', help="user query to only download web content relevant to the query", required=False)
    # parser.add_argument('-m', '--SAVE_MEDIA_FILES_FLAG', help="flag to save media files (images, audio and video) found on webpages", 
    #                     action='store_true')
    # parser.add_argument('-s', '--SAVE_CRAWL_TO_FILE_FLAG', help="flag to indicate to save list of crawled pages to a file", 
    #                     default=save_crawl_to_file_DEFAULT, required=False)
    # parser.add_argument('-f', '--FORCE_CRAWL_FLAG', help="flag to indicate to force crawl of previously crawled pages", 
    #                     action='store_true')
    # parser.add_argument('-c', '--CONTENT_TYPES', help="regular expression of content types to save when crawling (only matching content types will be saved)", default='', required=False)
    l_args = parser.parse_args()
    # logger.info(vars(l_args))
    # logger.info('\n')
    return l_args
    
# main script run
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(name)s: [%(levelname)s] %(message)s", # format="%(name)s: %(asctime)s: [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(LOGGER_NAME + '.log')])
    logger.info(f'*************** Running python script: {sys.argv} ***************')
    logger.info(f'In working directory: {os.getcwd()}')
    logger.debug(f'Path for loading python modules: {sys.path}')
    
    from dotenv import load_dotenv, find_dotenv
    ENV_FILENAME = '.env'
    SECRETS_FILENAME = '.env.secrets'
    if not load_dotenv(find_dotenv(ENV_FILENAME)):
        logger.warning(f'Failed to load enironment variables from {ENV_FILENAME}')
    else:
        logger.info(f'Loaded enironment variables from {ENV_FILENAME}')
    if not load_dotenv(find_dotenv(SECRETS_FILENAME)):
        logger.warning(f'Failed to load enironment variables from {SECRETS_FILENAME}')
    else:
        logger.info(f'Loaded enironment variables from {SECRETS_FILENAME}')
    
    m_args = get_args()
    #pd.set_option('display.max_rows', 500)
    #pd.set_option('display.max_columns', 500)
    #pd.set_option('display.width', 1000)
    ret = asyncio.run(main(m_args))
    logger.info(f'********** Finished running python script: {sys.argv}: return: {ret} **********')
    sys.exit(0)  # zero exit code to mark Success.
