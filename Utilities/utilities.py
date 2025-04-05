import logging
import requests

logger = logging.getLogger(__name__)

def send_http_request(url, method, payload=None, headers=None, params=None):
    """
    Send an HTTP request and return the JSON response.
    """
    try:
        logger.info("Sending HTTP request: %s %s", method, url)
        response = requests.request(method, url, data=payload, headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors
        logger.info("HTTP request successful: %s", response.status_code)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("HTTP request error: %s", e)
        return None
