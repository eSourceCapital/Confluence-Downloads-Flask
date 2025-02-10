from flask import Flask, request, jsonify
from requests.auth import HTTPBasicAuth
import requests
import re
import os
import time

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
        str: The ID of the space provided
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
    key_json = handle_json_errors(response)
    return key_json['id']

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
        s: ID of the homepage
    """
    url = f"https://{domain}/wiki/api/v2/spaces/{space_id}/pages"
    auth = HTTPBasicAuth(email, api_token)
    headers = {
      "Accept": "application/json"
    }
    response = requests.request("GET", url, headers=headers, auth=auth)
    handle_json_errors(response)
    pages = response.json()['results']
    for page in pages:
        if page['parentType'] is None:
            return page['id']
    return None
  
def get_confluence_children_by_parent_page_id_recursive(domain: str, email: str, api_token: str, page_id: str):
    """
    Fetches page's content from the Confluence API.
    Refer to: https://developer.atlassian.com/cloud/confluence/rest/v2/api-group-page/#api-pages-id-get
    Args:
        domain (str): The Confluence instance domain (e.g., 'your-domain.atlassian.net').
        email (str): The email address of the Confluence user.
        api_token (str): The API token for authentication.
        page_id (str): The ID of the page to fetch content from.

    Returns:
        dict: All page ids and titles
    """
    url = f"https://{domain}/wiki/api/v2/pages/{page_id}/children"
    auth = HTTPBasicAuth(email, api_token)
    headers = {
      "Accept": "application/json"
    }

    response = requests.request("GET", url, headers=headers, auth=auth)
    response.raise_for_status()
    children = response.json()
    if not children or not children['results']:
        return {}

    pages_ids_dict = {}
    for child in children['results']:
        pages_ids_dict[child['id']] = child['title']
        pages_ids_dict.update(get_confluence_children_by_parent_page_id_recursive(domain, email, api_token, child['id']))

    return pages_ids_dict
  
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
        title: page title
    """
    url = f"https://{domain}/wiki/api/v2/pages/{page_id}"
    auth = HTTPBasicAuth(email, api_token)
    headers = {
      "Accept": "application/json"
    }
    response = requests.request("GET", url, headers=headers, auth=auth)
    response = handle_json_errors(response)
    return response['title']
  
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
    handle_json_errors(response)
    page_content = response.json()['body']['export_view']['value']
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
    return (page_content == "<p />" or page_content == "")

## HELPER FUNCTIONS

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
      
def convert_title_to_filename(title):
    """
    Converts a title string to a safe filename format by replacing spaces with underscores
    and removing non-word characters.

    Args:
        title (str): The title to be converted.

    Returns:
        str: The converted filename with spaces replaced by underscores and non-word characters removed.
    """
    return re.sub(r'\W+', '', title.strip().replace(' ', '_'))
  
def handle_json_errors(response):
    """
    Handles JSON parsing for an HTTP response, returning JSON data if successful or error details if not.

    Args:
        response (requests.Response): The HTTP response object to parse.

    Returns:
        dict: A dictionary containing the parsed JSON data if successful,
        or an error message with response details if the JSON parsing fails or if the response status is not 200.
    """
    if response.status_code == 200:
        try:
            data = response.json()
            return data
        except ValueError:
            return {"error": "Response is not JSON formatted", "details": response.text}
    else:
        return {"error": f"Request failed with status {response.status_code}", "details": response.text}
    
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
    #Get space id
    space_id = get_confluence_space_id_by_key(domain, email, api_token, space_key)
    print(f"Space ID: {space_id}")

    #Get homepage id
    homepage_id = get_confluence_homepage_id_by_space_id(domain, email, api_token, space_id)
    print(f"Homepage ID: {homepage_id}")

    #Get all children from the homepage
    pages_ids_dict = get_confluence_children_by_parent_page_id_recursive(domain, email, api_token, homepage_id)
    print(f"Page IDs and titles: {pages_ids_dict}")
    
    #Store status of pages
    pages_status = {}
    
    #Download pages
    for page_id, page_title in pages_ids_dict.items():
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

