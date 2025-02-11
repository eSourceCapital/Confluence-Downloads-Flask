from flask import Flask, request, jsonify
from requests.auth import HTTPBasicAuth
import requests
import re
import os
import time
import io
import time
from datetime import datetime

app = Flask(__name__)

## CONFLUENCE API

def get_confluence_space_id_by_key(domain: str, email: str, api_token: str, space_key: str) -> dict:
    """
    Fetches space ID details from the Confluence API.

    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        space_key (str): The key of the space to fetch details for (e.g. 'OR' your-domain.atlassian.atlassian.net/wiki/spaces/OR/).

    Returns:
        str: The ID of the space provided, otherwise None
    """

    import base64
    auth_string = f"{email}:{api_token}"
    encoded_auth_string = base64.b64encode(auth_string.encode()).decode()
    
    url = f"https://{domain}/wiki/rest/api/space/{space_key}"
    headers = {
        "Authorization": f"Basic {encoded_auth_string}",
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    data = handle_json_errors(response)
    if 'error' in data:
        return None
    space_id = data.get("id")
    if space_id is None:
        print(f"Error: 'id' field missing in response when fetching space {space_key}")
        return None
    return space_id

def get_confluence_homepage_id_by_space_id(domain: str, email: str, api_token: str, space_id: str):
    """
    Fetches a space's homepage ID from the Confluence API.
    Refer to: https://developer.atlassian.com/cloud/confluence/rest/v2/api-group-page/#api-spaces-id-pages-get
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        space_id (str): The ID of the space to fetch details for.

    Returns:
        s: ID of the homepage, None otherwise
    """
    url = f"https://{domain}/wiki/api/v2/spaces/{space_id}/pages"
    auth = HTTPBasicAuth(email, api_token)
    headers = {
      "Accept": "application/json"
    }
    response = requests.get(url, headers=headers, auth=auth)
    data = handle_json_errors(response)
    if 'error' in data:
        return None
    pages = data.get("results")
    if pages is None:
        print(f"Error: 'results' field missing when fetching homepage ID")
        return None

    for page in pages:
        if page['parentType'] is None:
            return page['id']
    return None

def get_confluence_children_by_parent_page_id_recursive(domain: str, email: str, api_token: str, page_id: str):
    """
    Fetches all child pages of a given Confluence page recursively, handling pagination.
    
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        page_id (str): The ID of the page to fetch children from.

    Returns:
        dict: A dictionary of all child page IDs and titles.
    """
    base_url = f"https://{domain}/wiki/api/v2/pages/{page_id}/children"
    auth = HTTPBasicAuth(email, api_token)
    headers = {"Accept": "application/json"}
    params = {"limit": "250"}

    pages_ids_dict = {}

    url = base_url  # Start with the initial URL
    while url:
        response = requests.get(url, headers=headers, auth=auth, params=params)
        data = handle_json_errors(response)
        if 'error' in data:
            return {}
        if 'results' not in data:
            print(f"Error: 'results' field missing when fetching children")
            return {}

        # Store page IDs and titles
        for child in data["results"]:
            pages_ids_dict[child["id"]] = child["title"]

        # Check for pagination in the Link header
        link_header = response.headers.get("Link")
        if link_header:
            links = {rel.split(";")[1].strip(): rel.split(";")[0].strip("<>") for rel in link_header.split(",")}
            next_url = links.get('rel="next"')
            url = f"https://{domain}{next_url}" if next_url else None  # Only update if there's another page
        else:
            url = None  # No more pages

    # Recursively fetch children of each retrieved page
    for child_id in list(pages_ids_dict.keys()):
        pages_ids_dict.update(get_confluence_children_by_parent_page_id_recursive(domain, email, api_token, child_id))

    return pages_ids_dict

def get_confluence_pages_by_space_id_limit(domain: str, email: str, api_token: str, space_id: str, limit=250):
    """
    Fetches all Confluence pages from a Space ID, limit <= 250, without pagination.
    Refer to: https://developer.atlassian.com/cloud/confluence/rest/v2/api-group-page/#api-spaces-id-pages-get
    
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        space_id (str): The ID of the space to fetch details for.

    Returns:
        list: A list of all pages retrieved from Confluence.
    """
    limit = abs(limit) if limit <= 250 else 250
    url = f"https://{domain}/wiki/api/v2/spaces/{space_id}/pages"
    auth = HTTPBasicAuth(email, api_token)
    headers = {"Accept": "application/json"}
    params = {"limit": limit}  # Fetch the max number per request
    response = requests.get(url, headers=headers, auth=auth, params=params)
    data = handle_json_errors(response)
    if 'error' in data:
        return []
    if 'results' not in data:
        print(f"Error: 'results' field missing when fetching children")
        return []
    return data["results"]

def get_confluence_pages_by_space_id(domain: str, email: str, api_token: str, space_id: str):
    """
    Fetches all Confluence pages from a Space ID, handling pagination.
    Refer to: https://developer.atlassian.com/cloud/confluence/rest/v2/api-group-page/#api-spaces-id-pages-get
    
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        space_id (str): The ID of the space to fetch details for.

    Returns:
        list: A list of all pages retrieved from Confluence.
    """
    url = f"https://{domain}/wiki/api/v2/spaces/{space_id}/pages"
    auth = HTTPBasicAuth(email, api_token)
    headers = {"Accept": "application/json"}
    params = {"limit": "250"}  # Fetch the max number per request
    all_pages = []

    while url:
        response = requests.get(url, headers=headers, auth=auth, params=params)
        data = handle_json_errors(response)
        if 'error' in data:
            return []
        if 'results' not in data:
            print(f"Error: 'results' field missing when fetching children")
            return []

        all_pages.extend(data["results"])  # Collect page data

        # Handle pagination via "Link" header
        link_header = response.headers.get("Link")
        if link_header:
            links = {rel.split(";")[1].strip(): rel.split(";")[0].strip("<>") for rel in link_header.split(",")}
            next_url = links.get('rel="next"')
            url = f"https://{domain}{next_url}" if next_url else None
        else:
            url = None  # No more pages

    return all_pages
  
def get_pdf_export_confluence_url(domain, email, api_token, page_id):
    """
    Refer to: https://confluence.atlassian.com/confkb/rest-api-to-export-and-download-a-page-in-pdf-format-1388160685.html
    """
    # Construct the export URL
    url = f"https://{domain}/wiki/spaces/flyingpdf/pdfpageexport.action?pageId={page_id}&unmatched-route=true"
    auth = HTTPBasicAuth(email, api_token)
    headers = {
        "X-Atlassian-Token": "no-check",
    }
    response = requests.get(url, headers=headers, auth=auth, allow_redirects=True)
    if response.status_code != 200:
        return {"error": f"Request failed with status {response.status_code}", "details": response.text}

    task_cloud_ids = extract_task_and_cloud_id_from_html(response.text)
    if task_cloud_ids:
      download_url = f"https://{domain}/wiki/services/api/v1/download/pdf?taskId={task_cloud_ids['taskId']}&cloudId={task_cloud_ids['cloudId']}"
      download_response = requests.get(download_url, auth=HTTPBasicAuth(email, api_token))
      presigned_url = download_response.text
      return presigned_url
  
def get_confluence_page_title_by_id(domain: str, email: str, api_token: str, page_id: str):
    """
    Fetches page title from the Confluence API.
    Refer to: https://developer.atlassian.com/cloud/confluence/rest/v2/api-group-page/#api-pages-id-get
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        page_id (str): The ID of the page to fetch details for.

    Returns:
        title: page title, None otherwise
    """
    url = f"https://{domain}/wiki/api/v2/pages/{page_id}"
    auth = HTTPBasicAuth(email, api_token)
    headers = {
      "Accept": "application/json"
    }
    response = requests.request("GET", url, headers=headers, auth=auth)
    data = handle_json_errors(response)
    if 'error' in data:
        return None
    if 'title' not in data:
        print(f"Error: 'results' field missing when fetching children")
        return None
    return data['title']
  
def get_confluence_page_content_by_id(domain: str, email: str, api_token: str, page_id: str):
    """
    Fetches page's content from the Confluence API.
    Refer to: https://developer.atlassian.com/cloud/confluence/rest/v2/api-group-page/#api-pages-id-get
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        page_id (str): The ID of the page to fetch content from.

    Returns:
        A string with content of the page
    """
    url = f"https://{domain}/wiki/rest/api/content/{page_id}?expand=body.export_view"
    auth = HTTPBasicAuth(email, api_token)
    headers = {
      "Accept": "application/json"
    }
    response = requests.request("GET", url, headers=headers, auth=auth)
    data = handle_json_errors(response)
    if 'error' in data:
        return None
    try:
        page_content = data['body']['export_view']['value']
    except KeyError as e:
        print(f"Error accessing page content for page {page_id}: {e}")
        return None
    return page_content

def is_empty_confluence_page(domain: str, email: str, api_token: str, page_id: str):
    """
    Fetches page's content from the Confluence API and checks if it is empty.
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        page_id (str): The ID of the page to fetch content from.

    Returns:
        A  boolean value
    """
    page_content = get_confluence_page_content_by_id(domain, email, api_token, page_id)
    return page_content.strip() in ["<p />", ""]

## HELPER FUNCTIONS

def handle_json_errors(response):
    if response.status_code == 200:
        try:
            data = response.json()
            return data
        except ValueError:
            print(f"API response is not JSON formatted: {response.text}")
            return {"error": "Response is not JSON formatted", "details": response.text}
    else:
        print(f"API request failed: {response.status_code} - {response.text}")
        return {"error": f"Request failed with status {response.status_code}", "details": response.text}

def extract_task_and_cloud_id_from_html(html_string):
    """
    Extracts taskId and cloudId from the meta tags in the HTML string using regular expressions.

    Args:
        html_string: The HTML string to parse.

    Returns:
        A dictionary containing taskId and cloudId, or None if not found.
    """
    # Regular expressions to match the meta tags
    task_id_match = re.search(r'<meta\s+name="ajs-taskId"\s+content="([^"]+)"', html_string)
    cloud_id_match = re.search(r'<meta\s+name="ajs-cloud-id"\s+content="([^"]+)"', html_string)

    task_id = task_id_match.group(1) if task_id_match else None
    cloud_id = cloud_id_match.group(1) if cloud_id_match else None

    if task_id and cloud_id:
        return {'taskId': task_id, 'cloudId': cloud_id}
    else:
        print("taskId or cloudId not found in the HTML")
        return None
      
def download_pdf_from_presigned_url(url, output_path):
    """
    Authenticates with a server to retrieve a pre-signed URL and downloads a file.

    Args:
        url (str): URL for download request
        output_path (str): Path, including filename, where PDF should be downloaded
        
    Return:
        Status code. 200 is succesful
    """
    #Get path and filename
    directory = os.path.dirname(output_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    #Make sure filename ends in .pdf
    filename = os.path.basename(output_path)
    if not filename.lower().endswith('.pdf'):
        filename += '.pdf'
    output_path = f"{directory}/{filename}"
        
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"File downloaded successfully and saved as {filename}")  
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")
    
    return {"statusCode": response.status_code}    

def download_pdf_from_presigned_url_to_gcs_bucket(url, filename, gcs_bucket_name):
    """
    Downloads a PDF from a pre-signed URL directly to a Google Cloud Storage bucket.

    Args:
        url (str): URL for download request
        filename (str): Name of file in the GCS bucket
        gcs_bucket_name (str): Google Cloud Storage bucket to upload the file to
        
    Returns:
        Status code of the download request. 200 is successful.
    """
    
    # Make sure filename is properly formatted and ends in .pdf
    filename = convert_title_to_filename(filename)
    if not filename.lower().endswith('.pdf'):
        filename += '.pdf'
        
    # Perform the request to get the file content
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        # Initialize the Google Cloud Storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(filename)
        
        # Use an in-memory BytesIO buffer to hold the file content temporarily
        with io.BytesIO() as file_buffer:
            for chunk in response.iter_content(chunk_size=8192):
                file_buffer.write(chunk)
            
            # Reset buffer position to the beginning
            file_buffer.seek(0)
            
            # Upload directly from the buffer
            blob.upload_from_file(file_buffer, content_type='application/pdf')
        
        print(f"File downloaded successfully and saved to GCS bucket {gcs_bucket_name} as {filename}")
    
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")
    
    return {"statusCode": response.status_code}
      
def convert_title_to_filename(title, max_length=100):
    """
    Converts a title string to a safe filename format by replacing spaces with underscores
    and removing non-word characters, then truncating to a max length.

    Args:
        title (str): The title to be converted.

    Returns:
        str: The converted filename with spaces replaced by underscores and non-word characters removed.
    """
    # Replace spaces with underscores and remove non-word characters
    result = re.sub(r'\W+', '', title.strip().replace(' ', '_'))
    
    # Truncate to max_length while ensuring the file extension is preserved if present
    return result[:max_length].rstrip('_')

def add_value_to_dict(dictionary, key, value):
    """Adds a value to a dictionary at a given key.

    Args:
        dictionary: The dictionary to modify.
        key: The key to use.
        value: The value to add.
    """

    if key not in dictionary:
        dictionary[key] = [value]
    else:
        dictionary[key].append(value)
        
## FINAL OUTPUT FUNCTIONS

def export_pdf_confluence_page_by_id(
    domain, 
    email, 
    api_token, 
    page_id, 
    page_title=None, 
    output_path=None, 
    gcs_bucket_name=None,
    wait_time=15):
    
    """
    Exports a page as a PDF from the Confluence API.
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        page_id (str): The ID of the page to fetch details for.
        page_title (str): The title of the page to fetch details for. Optional.
        output_path (str): Path where file will be downloaded to. Optional. 
                           Default is 'confluence_downloads/'
        gcs_bucket (str): Google Cloud Storage bucket to upload the file to. Optional.

    Returns:
        str: Status of the downloaded page: 'EMPTY_PAGE', 'DOWNLOAD_SUCCESFUL', 'DOWNLOAD_FAILED'
    """
    
    #Get page title if not provided
    if not page_title:
        page_title = get_confluence_page_title_by_id(domain, email, api_token, page_id)
    
    #File page title, formatted and ending in confluencePageId=page_id   
    file_page_title = f"{convert_title_to_filename(page_title)}_confluencePageId={page_id}"
    
    #Check if it is an empty page
    if is_empty_confluence_page(domain, email, api_token, page_id):
        print(f"{file_page_title} is an empty page.")
        return 'EMPTY_PAGE'
    
    #Wait time cannot be 0
    if not wait_time or wait_time == 0:
        wait_time = 15

    #Try 3 times
    for attempt in range(3):
        #Generate the presigned download URL
        url = get_pdf_export_confluence_url(domain, email, api_token, page_id)
        
        #To avoid file not found error, wait a bit before downloading from the URL
        time.sleep(wait_time)
        
        #Download the file, and store the status code
        
        #If there is a bucket specified, download to bucket
        if gcs_bucket_name:    
            download_url = download_pdf_from_presigned_url_to_gcs_bucket(url=url, filename=file_page_title, gcs_bucket_name=gcs_bucket_name)
            status_code = download_url['statusCode']
            
        #If not, download to output_path
        else: 
            #If no output_path, then set to a value
            if not output_path:
                output_path = 'confluence_downloads/'
            #Make sure output_path ends in /
            output_path = output_path + "/" if not output_path.endswith("/") else output_path
        
            download_url = download_pdf_from_presigned_url(url=url, output_path=f"{output_path}{file_page_title}")  
            status_code = download_url['statusCode']
        
        if status_code == 200:
            return 'DOWNLOAD_SUCCESFUL'
        else:
            wait_time += 10 #Increase wait between url and download
            print(f"Attempt {attempt + 1} failed with status code {status_code}. Retrying in 10 seconds...")
            time.sleep(10)
            
    return 'DOWNLOAD_FAILED'

def export_pdf_confluence_space_by_key(domain, email, api_token, space_key, output_path=None, gcs_bucket_name=None, wait_time=15):
    """
    Exports all pages in a space as a PDF from the Confluence API.
    
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        space_key (str): The key of the space to fetch details for (e.g. 'OR' your-domain.atlassian.atlassian.net/wiki/spaces/OR/).
        output_path (str): Path where file will be downloaded to. Optional.

    Returns:
        dict: pages_status. Keys = Page IDs, and Values = Status of the downloaded page: 'EMPTY_PAGE', 'DOWNLOAD_SUCCESFUL', 'DOWNLOAD_FAILED'
    """
    import time

    # Save timestamp
    start = time.time()
    print(f"Start time: {datetime.now()}")

    #Get space id
    space_id = get_confluence_space_id_by_key(domain, email, api_token, space_key)
    if not space_id:
        return
    print(f"Space ID: {space_id}")

    #Get homepage id
    homepage_id = get_confluence_homepage_id_by_space_id(domain, email, api_token, space_id)
    if not homepage_id:
        return
    print(f"Homepage ID: {homepage_id}")

    #Get all pages
    #pages_ids_dict = get_confluence_children_by_parent_page_id_recursive(domain, email, api_token, homepage_id)  
    all_pages = get_confluence_pages_by_space_id(domain, email, api_token, space_id)
    #all_pages = get_confluence_pages_by_space_id_limit(domain, email, api_token, space_id, limit=250)
    pages_ids_dict = {}
    for page in all_pages:
        pages_ids_dict[page['id']] = page['title']
    print(f"Page IDs and titles. Length {len(pages_ids_dict)}, Dictionary: {pages_ids_dict}")
    
    #Store status of pages
    pages_status = {}
    
    #Download pages
    count = 0
    for page_id, page_title in pages_ids_dict.items():
        count += 1
        if count % 25 == 0: #Multiples of 25
            print(f"Document #{count}: {datetime.now()}")
        page_status = export_pdf_confluence_page_by_id(
            domain=domain,
            email=email,
            api_token=api_token,
            page_id=page_id,
            page_title=page_title,
            output_path=output_path,
            gcs_bucket_name=gcs_bucket_name,
            wait_time=wait_time
        )
        add_value_to_dict(dictionary=pages_status, key=page_status, value=page_id)
        
    print(pages_status)
    
    # Save timestamp
    end = time.time()
    print(f"End time: {datetime.now()}, End - start: {end - start}")
    return pages_status
    
@app.route('/export_pdf_space', methods=['POST'])
def export_pdf_space():
    """
    Downloads all pages in a space from the Confluence API.
    """
    print("Received request to export Confluence space as PDFs.")
    
    data = request.get_json()
    print(f"Data received: {data}")
    
    domain = data.get('domain')
    email = data.get('email')
    api_token = data.get('api_token')
    space_key = data.get('space_key')
    output_path = data.get('output_path')
    gcs_bucket_name = data.get('gcs_bucket_name')
    wait_time = data.get('wait_time')

    if not all([domain, email, api_token, space_key]):
        return jsonify({"error": "Missing required parameters"}), 400

    pages_status = export_pdf_confluence_space_by_key(
        domain=domain,
        email=email,
        api_token=api_token,
        space_key=space_key,
        output_path=output_path,
        gcs_bucket_name=gcs_bucket_name,
        wait_time=wait_time
    )
    return jsonify({"results": pages_status})

@app.route('/export_pdf_page', methods=['POST'])
def export_pdf_page():
    """
    Downloads a page in a space from the Confluence API.
    """
    print("Received request to export Confluence page as PDF.")
    
    data = request.get_json()
    print(f"Data received: {data}")
    
    domain = data.get('domain')
    email = data.get('email')
    api_token = data.get('api_token')
    page_id = data.get('page_id')
    page_title = data.get('page_title')
    output_path = data.get('output_path')
    gcs_bucket_name = data.get('gcs_bucket_name')
    wait_time = int(data.get('wait_time'))

    if not all([domain, email, api_token, page_id]):
        return jsonify({"error": "Missing required parameters"}), 400

    pages_status = export_pdf_confluence_page_by_id(
        domain=domain,
        email=email,
        api_token=api_token,
        page_id=page_id,
        page_title=page_title,
        output_path=output_path,
        gcs_bucket_name=gcs_bucket_name,
        wait_time=wait_time
    )
    return jsonify({"results": pages_status})

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 1111))
    app.run(debug=True, host="0.0.0.0", port=port)
    
# curl -v -X POST http://0.0.0.0:1111/export_pdf_space -H "Content-Type: application/json" -d '{"domain":"", "email":"", "api_token":"", "space_key":"", "output_path":"pdf_downloads/"}'