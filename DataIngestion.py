import pandas as pd
from sqlalchemy import create_engine, text
import os
from typing import Optional
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CSVToPostgresPipeline:
    """Pipeline to read CSV files and upload them to PostgreSQL database"""
    
    def __init__(self, csv_folder_path: str = None, chunk_size: int = None):
        """
        Initialize the pipeline with configuration from environment variables
        
        Args:
            csv_folder_path: Optional override for CSV folder path
            chunk_size: Optional override for chunk size
        """
        # Load database credentials from environment
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')
        self.db_name = os.getenv('DB_NAME')
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        
        # Validate required environment variables
        required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}\n"
                f"Please create a .env file with these variables. "
                f"See .env.example for template."
            )
        
        # Build connection string securely
        self.db_connection_string = (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )
        
        # Pipeline configuration
        self.csv_folder_path = csv_folder_path or os.getenv('CSV_FOLDER_PATH')
        if not self.csv_folder_path:
            raise ValueError("CSV_FOLDER_PATH must be set in environment or passed as argument")
            
        self.chunk_size = chunk_size or int(os.getenv('CHUNK_SIZE', '100000'))
        self.engine = None
        self.large_files = os.getenv('LARGE_FILES', 'sales.csv').split(',')
        
        logger.info(f"Pipeline initialized for folder: {self.csv_folder_path}")
        logger.info(f"Database: {self.db_name} on {self.db_host}:{self.db_port}")
        
    def connect_to_database(self) -> bool:
        """Establish database connection"""
        try:
            self.engine = create_engine(self.db_connection_string)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean column names by removing spaces and extra whitespace"""
        df.columns = [c.strip().replace(' ', '') for c in df.columns]
        return df
    
    def get_table_name(self, filename: str) -> str:
        """Convert filename to valid table name"""
        return filename.replace('.csv', '').replace('-', '_').lower()
    
    def process_large_file(self, file_path: str, table_name: str) -> bool:
        """Process large CSV files in chunks"""
        logger.info(f"Using memory-safe chunking for large file")
        
        total_rows = 0
        chunk_number = 0
        
        try:
            for chunk in pd.read_csv(file_path, chunksize=self.chunk_size):
                chunk_number += 1
                
                # Clean columns
                chunk = self.clean_column_names(chunk)
                
                # Upload chunk (replace on first chunk, append after)
                mode = 'replace' if chunk_number == 1 else 'append'
                chunk.to_sql(table_name, self.engine, if_exists=mode, index=False)
                
                total_rows += len(chunk)
                logger.info(f"Chunk {chunk_number} uploaded. ({total_rows:,} rows total)")
            
            logger.info(f"Large file uploaded successfully: {total_rows:,} total rows")
            return True
            
        except Exception as e:
            logger.error(f"Error processing large file in chunks: {e}")
            return False
    
    def process_standard_file(self, file_path: str, table_name: str) -> bool:
        """Process standard CSV files"""
        try:
            # Read CSV
            df = pd.read_csv(file_path)
            
            # Clean column names
            df = self.clean_column_names(df)
            
            # Upload to SQL
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            
            logger.info(f"File ingested successfully into table '{table_name}' ({len(df):,} rows)")
            return True
            
        except Exception as e:
            logger.error(f"Error processing standard file: {e}")
            return False
    
    def process_single_file(self, filename: str) -> bool:
        """Process a single CSV file"""
        logger.info("=" * 50)
        logger.info(f"Processing '{filename}'...")
        
        full_file_path = os.path.join(self.csv_folder_path, filename)
        table_name = self.get_table_name(filename)
        
        # Choose processing method based on file
        if filename in self.large_files:
            return self.process_large_file(full_file_path, table_name)
        else:
            return self.process_standard_file(full_file_path, table_name)
    
    def get_csv_files(self) -> list:
        """Get list of CSV files in the folder"""
        try:
            all_files = os.listdir(self.csv_folder_path)
            csv_files = [f for f in all_files if f.endswith('.csv')]
            logger.info(f"Found {len(csv_files)} CSV file(s) in '{self.csv_folder_path}'")
            return csv_files
        except Exception as e:
            logger.error(f"Error reading folder: {e}")
            return []
    
    def run(self) -> dict:
        """
        Run the complete pipeline
        
        Returns:
            dict: Summary of pipeline execution with success/failure counts
        """
        logger.info("Starting CSV to PostgreSQL pipeline")
        logger.info(f"Searching for CSV files in '{self.csv_folder_path}'...")
        
        # Connect to database
        if not self.connect_to_database():
            return {'status': 'failed', 'reason': 'Database connection failed'}
        
        # Get CSV files
        csv_files = self.get_csv_files()
        if not csv_files:
            return {'status': 'failed', 'reason': 'No CSV files found'}
        
        # Process each file
        results = {
            'total_files': len(csv_files),
            'successful': 0,
            'failed': 0,
            'failed_files': []
        }
        
        for filename in csv_files:
            if self.process_single_file(filename):
                results['successful'] += 1
            else:
                results['failed'] += 1
                results['failed_files'].append(filename)
        
        # Summary
        logger.info("=" * 50)
        logger.info("Pipeline Execution Summary")
        logger.info(f"Total files: {results['total_files']}")
        logger.info(f"Successful: {results['successful']}")
        logger.info(f"Failed: {results['failed']}")
        if results['failed_files']:
            logger.info(f"Failed files: {', '.join(results['failed_files'])}")
        logger.info("Pipeline finished")
        
        results['status'] = 'completed'
        return results


# Main execution
if __name__ == "__main__":
    # All configuration comes from environment variables
    # No hardcoded credentials!
    
    try:
        pipeline = CSVToPostgresPipeline()
        results = pipeline.run()
        
        # Exit with appropriate code
        exit(0 if results['status'] == 'completed' and results['failed'] == 0 else 1)
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Please check your .env file and ensure all required variables are set.")
        exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        exit(1)