# This script will contain the methods for extracting the data from various sources such as CSV files, an API and an S3 Bucket.

class DataExtractor:
    def __init__(self, source):
        self.source = source

    