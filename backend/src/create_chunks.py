from langchain_text_splitters import TokenTextSplitter
from langchain.docstore.document import Document
from langchain_neo4j import Neo4jGraph
import logging
from src.document_sources.youtube import get_chunks_with_timestamps, get_calculated_timestamps
import re
from math import ceil
import os

logging.basicConfig(format="%(asctime)s - %(message)s", level="INFO")


class CreateChunksofDocument:
    def __init__(self, pages: list[Document], graph: Neo4jGraph):
        self.pages = pages
        self.graph = graph

    def split_file_into_chunks(self):
        """
        Split a list of documents(file pages) into chunks of fixed size.

        Args:
            pages: A list of pages to split. Each page is a list of text strings.

        Returns:
            A list of chunks each of which is a langchain Document.
        """
        logging.info("Split file into smaller chunks")
        chunks_to_be_processed = int(os.getenv('CHUNKS_TO_BE_PROCESSED', 50))
        total_content = ''.join(page.page_content for page in self.pages)
        total_tokens = len(total_content.split())  

        chunk_size = ceil(total_tokens / chunks_to_be_processed)
        logging.info(f"Dynamic chunk size set to: {chunk_size} tokens (to fit within {chunks_to_be_processed} chunks)")
        text_splitter = TokenTextSplitter(chunk_size=chunk_size, chunk_overlap=20)
        chunks = []
        if 'page' in self.pages[0].metadata:
            for i, document in enumerate(self.pages):
                page_number = i + 1
                for chunk in text_splitter.split_documents([document]):
                    chunks.append(Document(page_content=chunk.page_content, metadata={'page_number':page_number}))    
        
        elif 'length' in self.pages[0].metadata:
            if len(self.pages) == 1  or (len(self.pages) > 1 and self.pages[1].page_content.strip() == ''): 
                match = re.search(r'(?:v=)([0-9A-Za-z_-]{11})\s*',self.pages[0].metadata['source'])
                youtube_id=match.group(1)   
                chunks_without_time_range = text_splitter.split_documents([self.pages[0]])
                chunks = get_calculated_timestamps(chunks_without_time_range, youtube_id)

            else: 
                chunks_without_time_range = text_splitter.split_documents(self.pages)   
                chunks = get_chunks_with_timestamps(chunks_without_time_range)
        else:
            chunks = text_splitter.split_documents(self.pages)

        if len(chunks) > chunks_to_be_processed:
           logging.warning(f"More chunks created ({len(chunks)}) than limit ({chunks_to_be_processed}). Truncating.")
           chunks = chunks[:chunks_to_be_processed]
        return chunks
