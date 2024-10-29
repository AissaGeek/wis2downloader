from prometheus_client import Counter, Gauge


QUEUE_SIZE = Gauge(
    'queue_size', 'Current size of the job queue')
TOPIC_STATUS = Gauge(
    'topic_subscription_status', 'Subscription status of a given topic',
    ['topic'])
DOWNLOADED_BYTES = Counter(
    'downloaded_bytes', 'Total number of downloaded bytes',
    ['target', 'topic', 'centre_id', 'file_type'])
DOWNLOADED_FILES = Counter(
    'downloaded_files', 'Total number of downloaded files',
    ['target', 'topic', 'centre_id', 'file_type'])
FAILED_DOWNLOADS = Counter(
    'failed_downloads', 'Total number of failed downloads to either fs o s3',
    ['target', 'topic', 'centre_id'])
FAILED_DOWNLOADS_BROKER = Counter(
    'failed_downloads_broker', 'Total number of failed downloads from broker ',
    ['topic', 'centre_id'])
