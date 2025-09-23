import azure.functions as func
import logging
import requests
import traceback
import validators
import json
import uuid
from datetime import datetime
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
import os
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchField,
    SearchFieldDataType,
    SimpleField,
    SearchableField,
    ComplexField
)
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

# Load values from .env file into environment
load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Configuration - These should now come from .env or Azure Function App Settings
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
SEARCH_SERVICE_ENDPOINT = os.getenv("SEARCH_SERVICE_ENDPOINT")
SEARCH_API_KEY = os.getenv("SEARCH_API_KEY")
SEARCH_INDEX_NAME = os.getenv("SEARCH_INDEX_NAME")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")


@app.route(route="search_site", methods=["POST"])
def search_site(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Checks for a URL JSON property in the HTTP Request body.
    url = req.params.get('url')
    max_depth = req.params.get('max_depth', 1)
    
    if not url:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            url = req_body.get('url')
            max_depth = req_body.get('max_depth', 1)

    if url:
        if validators.url(url):
            try:
                max_depth = int(max_depth)
                response = orchestrator_function(url, max_depth)
                return func.HttpResponse(json.dumps(response, indent=2),
                                     status_code=200,
                                     mimetype="application/json")
            except Exception as e:
                logging.error(f"Error processing request: {str(e)}")
                return func.HttpResponse(
                    f"Error processing request: {str(e)}",
                    status_code=500
                )
        else:
            return func.HttpResponse(
                "The URL was invalid.",
                status_code=400
            )
    else:
        return func.HttpResponse(
            "No URL was passed. Please input a URL.",
            status_code=400
        )


def orchestrator_function(start_url, max_depth=1):
    """
    Orchestrates the web crawling, blob storage, and AI search indexing process.
    """
    try:
        # Initialize Azure services
        blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        search_client = initialize_search_index()
        
        # Crawl the website
        crawled_data = crawl_website_recursive(start_url, max_depth)
        
        # Store links in Blob Storage
        blob_info = store_links_in_blob(blob_service, crawled_data)
        
        # Index content in AI Search
        indexed_count = index_content_in_search(search_client, crawled_data)
        
        return {
            "status": "success",
            "crawled_pages": len(crawled_data),
            "blob_storage": blob_info,
            "search_index": {
                "indexed_documents": indexed_count,
                "index_name": SEARCH_INDEX_NAME
            },
            "pages": [
                {
                    "url": page["url"],
                    "title": page["title"],
                    "links_found": len(page["links"])
                }
                for page in crawled_data
            ]
        }
        
    except Exception as error:
        logging.error(f"Error in orchestrator function: {str(error)}")
        logging.error(traceback.format_exc())
        raise error


def crawl_website_recursive(start_url, max_depth, visited=None, current_depth=0):
    """
    Recursively crawl website up to specified depth.
    """
    if visited is None:
        visited = set()
    
    if current_depth > max_depth or start_url in visited:
        return []
    
    visited.add(start_url)
    crawled_data = []
    
    try:
        # Crawl current page
        page_data = crawl_site(start_url)
        page_info = {
            "url": start_url,
            "title": get_page_title(page_data),
            "meta_description": get_meta_tag(page_data),
            "links": get_all_urls(page_data, start_url),
            "content": get_page_content(page_data),
            "crawl_timestamp": datetime.utcnow().isoformat(),
            "depth": current_depth
        }
        crawled_data.append(page_info)
        
        # If we haven't reached max depth, crawl linked pages
        if current_depth < max_depth:
            for link in page_info["links"][:10]:  # Limit to first 10 links to avoid excessive crawling
                if link not in visited and is_same_domain(start_url, link):
                    try:
                        sub_pages = crawl_website_recursive(link, max_depth, visited, current_depth + 1)
                        crawled_data.extend(sub_pages)
                    except Exception as e:
                        logging.warning(f"Failed to crawl {link}: {str(e)}")
                        
    except Exception as error:
        logging.error(f"Error crawling {start_url}: {str(error)}")
        
    return crawled_data


def crawl_site(url):
    """Submits the HTTP request to the user-inputted URL."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, allow_redirects=True, timeout=30, headers=headers)
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml")


def get_page_title(data):
    """Extracts the page title."""
    try:
        title_tag = data.find('title')
        return title_tag.string.strip() if title_tag and title_tag.string else "No title found"
    except Exception as error:
        logging.error(f"Error retrieving the site title: {str(error)}")
        return "No title found"


def get_all_urls(data, base_url):
    """Gets all of the URLs from the webpage."""
    try:
        urls = []
        url_elements = data.select("a[href]")
        
        for url_element in url_elements:
            href = url_element.get('href', '').strip()
            if href:
                # Convert relative URLs to absolute
                absolute_url = urljoin(base_url, href)
                if validators.url(absolute_url):
                    urls.append(absolute_url)
        
        return list(set(urls))  # Remove duplicates
    
    except Exception as error:
        logging.error(f"Error retrieving the URLs in the site: {str(error)}")
        return []


def get_meta_tag(data):
    """Extracts the meta description tag from the URL."""
    try:
        meta_tag = data.find("meta", attrs={'name': 'description'})
        if meta_tag and meta_tag.get("content"):
            return meta_tag["content"].strip()
        
        # Try og:description as fallback
        og_desc = data.find("meta", attrs={'property': 'og:description'})
        if og_desc and og_desc.get("content"):
            return og_desc["content"].strip()
            
        return "No description found"
    except Exception as error:
        logging.error(f"Error retrieving meta description: {str(error)}")
        return "No description found"


def get_page_content(data):
    """Extract main content from the page."""
    try:
        # Remove script and style elements
        for script in data(["script", "style", "nav", "header", "footer"]):
            script.decompose()
        
        # Try to find main content areas
        main_content = data.find('main') or data.find('article') or data.find('div', class_='content')
        
        if main_content:
            text = main_content.get_text()
        else:
            text = data.get_text()
        
        # Clean up the text
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        # Limit content length for indexing
        return text[:5000] if len(text) > 5000 else text
        
    except Exception as error:
        logging.error(f"Error extracting page content: {str(error)}")
        return ""


def is_same_domain(url1, url2):
    """Check if two URLs belong to the same domain."""
    try:
        domain1 = urlparse(url1).netloc
        domain2 = urlparse(url2).netloc
        return domain1 == domain2
    except:
        return False


def store_links_in_blob(blob_service, crawled_data):
    """Store the crawled links and metadata in Azure Blob Storage."""
    try:
        container_client = blob_service.get_container_client(BLOB_CONTAINER_NAME)
        
        # Create container if it doesn't exist
        try:
            container_client.create_container()
        except Exception:
            pass  # Container already exists
        
        # Create a unique filename
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        blob_name = f"crawl_results_{timestamp}_{str(uuid.uuid4())[:8]}.json"
        
        # Prepare data for storage
        blob_data = {
            "crawl_timestamp": datetime.utcnow().isoformat(),
            "total_pages": len(crawled_data),
            "pages": crawled_data
        }
        
        # Upload to blob storage
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json.dumps(blob_data, indent=2), overwrite=True)
        
        return {
            "blob_name": blob_name,
            "container": BLOB_CONTAINER_NAME,
            "size_bytes": len(json.dumps(blob_data))
        }
        
    except Exception as error:
        logging.error(f"Error storing data in blob storage: {str(error)}")
        raise error


def initialize_search_index():
    """Initialize or create the Azure AI Search index."""
    try:
        # Create search index client
        index_client = SearchIndexClient(
            endpoint=SEARCH_SERVICE_ENDPOINT,
            credential=AzureKeyCredential(SEARCH_API_KEY)
        )
        
        # Check if index already exists
        try:
            existing_index = index_client.get_index(SEARCH_INDEX_NAME)
            logging.info(f"Using existing search index: {SEARCH_INDEX_NAME}")
            
            # Return search client for documents
            return SearchClient(
                endpoint=SEARCH_SERVICE_ENDPOINT,
                index_name=SEARCH_INDEX_NAME,
                credential=AzureKeyCredential(SEARCH_API_KEY)
            )
            
        except ResourceNotFoundError:
            # Index doesn't exist, create it
            logging.info(f"Creating new search index: {SEARCH_INDEX_NAME}")
            
            # Define the search index schema
            fields = [
                SimpleField(name="id", type=SearchFieldDataType.String, key=True),
                SearchableField(name="url", type=SearchFieldDataType.String),
                SearchableField(name="title", type=SearchFieldDataType.String),
                SearchableField(name="content", type=SearchFieldDataType.String),
                SearchableField(name="meta_description", type=SearchFieldDataType.String),
                SimpleField(name="crawl_timestamp", type=SearchFieldDataType.DateTimeOffset),
                SimpleField(name="depth", type=SearchFieldDataType.Int32),
                SimpleField(name="links_count", type=SearchFieldDataType.Int32)
            ]
            
            # Create the index
            index = SearchIndex(name=SEARCH_INDEX_NAME, fields=fields)
            index_client.create_index(index)
            
            # Return search client for documents
            return SearchClient(
                endpoint=SEARCH_SERVICE_ENDPOINT,
                index_name=SEARCH_INDEX_NAME,
                credential=AzureKeyCredential(SEARCH_API_KEY)
            )
        
    except Exception as error:
        logging.error(f"Error initializing search index: {str(error)}")
        raise error


def index_content_in_search(search_client, crawled_data):
    """Index the crawled content in Azure AI Search."""
    try:
        documents = []
        
        for page in crawled_data:
            # Create a deterministic ID based on URL to avoid duplicates
            page_id = str(uuid.uuid5(uuid.NAMESPACE_URL, page["url"]))
            
            document = {
                "id": page_id,
                "url": page["url"],
                "title": page["title"],
                "content": page["content"],
                "meta_description": page["meta_description"],
                "crawl_timestamp": page["crawl_timestamp"],
                "depth": page["depth"],
                "links_count": len(page["links"])
            }
            documents.append(document)
        
        # Upload documents to search index
        if documents:
            # Use merge_or_upload to handle potential duplicates
            result = search_client.merge_or_upload_documents(documents)
            successful_count = sum(1 for r in result if r.succeeded)
            
            if successful_count < len(documents):
                logging.warning(f"Only {successful_count}/{len(documents)} documents indexed successfully")
                # Log failed documents for debugging
                for i, r in enumerate(result):
                    if not r.succeeded:
                        logging.error(f"Failed to index document {i}: {r.error_message if hasattr(r, 'error_message') else 'Unknown error'}")
            
            return successful_count
        
        return 0
        
    except Exception as error:
        logging.error(f"Error indexing content in search: {str(error)}")
        raise error