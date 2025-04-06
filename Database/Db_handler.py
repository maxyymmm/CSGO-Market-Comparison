import os
import datetime

import numpy as np
import pandas as pd
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

# Set up module logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s:%(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Base declarative class for SQLAlchemy models
Base = declarative_base()


# Define the Items table
class Item(Base):
    __tablename__ = 'items'
    item_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    price_records = relationship('PriceRecord', back_populates='item')


# Define the Sources table (extended with commission_rate)
class Source(Base):
    __tablename__ = 'sources'
    source_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    commission_rate = Column(Float, nullable=False, default=0.0)  # nowa kolumna prowizji
    price_records = relationship('PriceRecord', back_populates='source')


# Define the PriceRecords table
class PriceRecord(Base):
    __tablename__ = 'price_records'
    record_id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('items.item_id'), nullable=False)
    source_id = Column(Integer, ForeignKey('sources.source_id'), nullable=False)
    price = Column(Float, nullable=False)
    price_after_sell = Column(Float, nullable=True)
    retrieved_at = Column(DateTime, default=datetime.datetime.utcnow)
    item = relationship('Item', back_populates='price_records')
    source = relationship('Source', back_populates='price_records')


class DatabaseHandler:
    def __init__(self, db_url):
        """
        Initialize the database handler.
        db_url should be in the form:
        postgresql://username:password@localhost:5432/your_database
        """
        logger.info("Initializing database engine with URL: %s", db_url)
        self.engine = create_engine(db_url)
        logger.info("Creating tables if they do not exist...")
        Base.metadata.create_all(self.engine)  # Create tables if they do not exist
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        logger.info("DatabaseHandler initialized successfully.")

    def load_csv_data_from_folder(self, download_folder='Sites_download'):
        """
        Load CSV files from the specified folder into the database.
        CSV format expected: name@price@price_after_sell
        The file name (without extension) is treated as the source name.
        """
        logger.info("Loading CSV data from folder: %s", download_folder)
        files = [f for f in os.listdir(download_folder) if f.endswith('.csv')]
        if not files:
            logger.warning("No CSV files found in folder: %s", download_folder)
            return

        for filename in files:
            source_name = os.path.splitext(filename)[0]  # e.g. "csdeals"
            file_path = os.path.join(download_folder, filename)
            logger.info("Processing file: %s as source: %s", filename, source_name)

            try:
                df = pd.read_csv(file_path, delimiter='@')
            except Exception as e:
                logger.error("Failed to read CSV file %s: %s", filename, e)
                continue

            for index, row in df.iterrows():
                try:
                    item_name = row['name']
                    # price = float(row['price'])
                    if pd.isna(row['price']) or not np.isfinite(row['price']):
                        continue
                    price = float(row['price'])
                    price_after_sell = float(row['price_after_sell']) if 'price_after_sell' in row else None
                    retrieved_at = datetime.datetime.utcnow()

                    # Check if the item exists (comparison by name)
                    item = self.session.query(Item).filter_by(name=item_name).first()
                    if not item:
                        logger.info("Item '%s' not found in DB. Creating new record.", item_name)
                        item = Item(name=item_name)
                        self.session.add(item)
                        self.session.commit()  # Commit to get item_id

                    # Check if the source exists
                    source = self.session.query(Source).filter_by(name=source_name).first()
                    if not source:
                        logger.info(
                            "Source '%s' not found in DB. Creating new record with default commission_rate 0.0.",
                            source_name)
                        source = Source(name=source_name, commission_rate=0.0)
                        self.session.add(source)
                        self.session.commit()  # Commit to get source_id

                    # Create a new price record
                    price_record = PriceRecord(
                        item_id=item.item_id,
                        source_id=source.source_id,
                        price=price,
                        price_after_sell=price_after_sell,
                        retrieved_at=retrieved_at
                    )
                    self.session.add(price_record)
                except Exception as e:
                    logger.error("Error processing row %s in file %s: %s", index, filename, e)
            self.session.commit()
            logger.info("Data loaded from %s", filename)
