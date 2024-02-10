# Description and Purpose
Crawl and save contents of web pages starting from a root url
## LICENSE INFO
```
__license__ = "MIT"
```
## Setup, install and usage
**Command-line arguments**
```
    parser.add_argument('-o', '--OUTPUT_DIR', help="output files directory", default=OUTPUT_DIR_DEFAULT)
    parser.add_argument('-u', '--ROOT_URL', help="root url of webpage or website to be crawled", required=True)
    parser.add_argument('-t', '--ROOT_URL_TLD_CRAWL_FLAG', help="flag to indicate to crawl all of the pages matching top level domain of url ", required=False)
    parser.add_argument('-d', '--MAX_DEPTH', help="depth of web pages to crawl", default=MAX_DEPTH_DEFAULT, required=False)
    parser.add_argument('-s', '--SAVE_CRAWLE_TO_FILE_FLAG', help="flag to indicate to save crawl details (pages crawled) to an excel file", default=SAVE_CRAWL_TO_FILE_DEFAULT, required=False)
    parser.add_argument('-c', '--CONTENT_TYPES', help="regular expression of content types to save when crawling (only matching content types will be saved)", default='*', required=False)

```
 **Notes**: 
 * -t argument indicates all pages below top level domain are to be crawled (for example, if root url is abc.xyz.com, pages below pqr.xyz.com will be crawled as well because they match top level domain xyz.com). If -t is not given, only specified root url and below will be crawled.
 * -c argument allows saving only pdf and json content by specifying '-c pdf\|json' in the command line   
### Pre-requisites
* python version 3.10
### pip installs
* pip install -r requirements.txt
### 
## Requirements
* Ability to save the webpages as .html and pdf as .pdf files etc.
* Ability to crawl to certain depth from the root url
* Ability to crawl certain types of content only
* Ability to associate saved file to the corresponding web page url
    * Robustness against url path characters not allowed in filenames
* Ability to trace the saved file conent back to the path of the web page starting from root url  
* Ability to rerun when crashed without having to recrawl pages already crawled

## Design notes and diagrams
## Dependencies
* API keys 
    * None

## Quality considerations: Security, robustness, performance etc.
## TODO
    * Fix the web page file saving to .html in a way that when the file is opened in browser it looks exactly like the webpage (right now looks like the script and css file links are missing when saved)

    * Create GUI and single-file executable without python installation for non-technical users and create webapp using streamlit or gradio