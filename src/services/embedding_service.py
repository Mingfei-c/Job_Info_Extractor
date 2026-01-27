"""
Embedding Service
Vector search implementation using LangChain + Gemini Embedding + pgvector
"""

import logging
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, ForeignKey, Integer, create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

from src.services.job_fetch import Job

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Gemini API Key
GOOGLE_API_KEY = os.getenv("GEMINI_API_KEY")

# Embedding dimension (Gemini text-embedding-004)
EMBEDDING_DIM = 768


# ==================== pgvector Support ====================

try:
    from pgvector.sqlalchemy import Vector

    PGVECTOR_AVAILABLE = True
except ImportError:
    PGVECTOR_AVAILABLE = False
    logger.warning("pgvector not installed, please run: pip install pgvector")


# ==================== Database Models ====================

# Note: pgvector extension must be enabled in PostgreSQL first
# CREATE EXTENSION IF NOT EXISTS vector;


class JobEmbedding(Base):
    """Job embedding table"""

    __tablename__ = "job_embeddings"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("adzuna_jobs.id"), unique=True, index=True)
    embedding = Column(Vector(EMBEDDING_DIM)) if PGVECTOR_AVAILABLE else None
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ==================== Embedding Service ====================


class EmbeddingService:
    """
    Embedding Service

    Features:
    1. Generate embeddings for job descriptions and store in pgvector
    2. Search matching jobs using vector similarity

    Usage:
        service = EmbeddingService()
        service.embed_all_jobs()  # Preprocessing
        jobs = service.search_similar_jobs("Python developer with 3 years experience")
    """

    EMBEDDING_MODEL = "models/text-embedding-004"

    def __init__(self):
        """Initialize service"""
        if not GOOGLE_API_KEY:
            raise ValueError("Please configure GOOGLE_API_KEY in .env")

        if not PGVECTOR_AVAILABLE:
            raise ImportError("pgvector required: pip install pgvector")

        # Initialize LangChain Embedding
        try:
            from langchain_google_genai import GoogleGenerativeAIEmbeddings

            self.embeddings = GoogleGenerativeAIEmbeddings(
                model=self.EMBEDDING_MODEL, google_api_key=GOOGLE_API_KEY
            )
        except ImportError as err:
            raise ImportError(
                "langchain-google-genai required: pip install langchain-google-genai"
            ) from err

        self.db = SessionLocal()

        # Ensure pgvector extension and tables exist
        self._ensure_pgvector_setup()

    def __del__(self):
        """Clean up resources"""
        if hasattr(self, "db"):
            self.db.close()

    def _ensure_pgvector_setup(self):
        """Ensure pgvector extension and tables are created"""
        try:
            # Create pgvector extension
            self.db.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            self.db.commit()
            logger.info("pgvector extension enabled")
        except Exception as e:
            logger.warning(f"Failed to create pgvector extension (may already exist): {e}")
            self.db.rollback()

        # Create job_embeddings table
        try:
            self.db.execute(
                text(f"""
                CREATE TABLE IF NOT EXISTS job_embeddings (
                    id SERIAL PRIMARY KEY,
                    job_id INTEGER UNIQUE REFERENCES adzuna_jobs(id) ON DELETE CASCADE,
                    embedding vector({EMBEDDING_DIM}),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            )
            self.db.commit()
            logger.info("job_embeddings table created")
        except Exception as e:
            logger.warning(f"Failed to create table (may already exist): {e}")
            self.db.rollback()

        # Create vector index (for faster search)
        try:
            self.db.execute(
                text("""
                CREATE INDEX IF NOT EXISTS job_embeddings_embedding_idx
                ON job_embeddings
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100)
            """)
            )
            self.db.commit()
            logger.info("Vector index created")
        except Exception as e:
            logger.warning(f"Failed to create index: {e}")
            self.db.rollback()

    def generate_embedding(self, text: str) -> list[float]:
        """
        Generate embedding for a single text

        Args:
            text: Text to embed

        Returns:
            768-dimensional vector
        """
        if not text or not text.strip():
            return [0.0] * EMBEDDING_DIM

        # Truncate text if too long (Gemini limit)
        max_chars = 10000
        if len(text) > max_chars:
            text = text[:max_chars]

        try:
            vector = self.embeddings.embed_query(text)
            return vector
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise

    def embed_all_jobs(self, batch_size: int = 50) -> dict:
        """
        Generate embeddings for all jobs that haven't been embedded

        Args:
            batch_size: Number of jobs to process per batch

        Returns:
            {"processed": N, "skipped": N, "failed": N}
        """
        logger.info("=" * 60)
        logger.info("Starting job description embedding")

        # Query already embedded job_ids
        existing_ids = set()
        result = self.db.execute(text("SELECT job_id FROM job_embeddings"))
        for row in result:
            existing_ids.add(row[0])

        logger.info(f"Already embedded: {len(existing_ids)} jobs")

        # Query all jobs
        all_jobs = self.db.query(Job).filter(Job.description.isnot(None)).all()
        pending_jobs = [j for j in all_jobs if j.id not in existing_ids]

        logger.info(f"Pending: {len(pending_jobs)} jobs")

        if not pending_jobs:
            return {"processed": 0, "skipped": len(existing_ids), "failed": 0}

        # Batch processing
        processed = 0
        failed = 0

        for i in range(0, len(pending_jobs), batch_size):
            batch = pending_jobs[i : i + batch_size]

            for job in batch:
                try:
                    # Build text for embedding
                    text_to_embed = self._build_job_text(job)

                    # Generate embedding
                    vector = self.generate_embedding(text_to_embed)

                    # Store in database
                    self.db.execute(
                        text("""
                            INSERT INTO job_embeddings (job_id, embedding, created_at)
                            VALUES (:job_id, :embedding, :created_at)
                            ON CONFLICT (job_id) DO UPDATE SET
                                embedding = :embedding,
                                created_at = :created_at
                        """),
                        {
                            "job_id": job.id,
                            "embedding": str(vector),
                            "created_at": datetime.now(timezone.utc),
                        },
                    )

                    processed += 1

                except Exception as e:
                    logger.error(f"Failed to process job_id={job.id}: {e}")
                    failed += 1

            # Commit batch
            self.db.commit()

            # Progress log
            logger.info(
                f"Progress: {processed + failed}/{len(pending_jobs)}, "
                f"Success: {processed}, Failed: {failed}"
            )

            # Rate limiting (avoid API limits)
            time.sleep(0.5)

        logger.info("=" * 60)
        logger.info(
            f"Embedding complete: Processed {processed}, Skipped {len(existing_ids)}, Failed {failed}"
        )

        return {"processed": processed, "skipped": len(existing_ids), "failed": failed}

    def _build_job_text(self, job: Job) -> str:
        """Build text for job embedding"""
        parts = []

        if job.title:
            parts.append(f"Title: {job.title}")

        if job.company_name:
            parts.append(f"Company: {job.company_name}")

        if job.category:
            parts.append(f"Category: {job.category}")

        if job.location:
            parts.append(f"Location: {job.location}")

        if job.description:
            parts.append(f"Description: {job.description}")

        return "\n".join(parts)

    def search_similar_jobs(self, query_text: str, top_k: int = 10) -> list[dict]:
        """
        Search matching jobs using vector similarity

        Args:
            query_text: Search text (e.g., resume content)
            top_k: Number of results to return

        Returns:
            List of matching jobs with similarity scores
        """
        logger.info(f"Vector search: Finding Top {top_k} matching jobs")

        # Generate query embedding
        query_vector = self.generate_embedding(query_text)

        # Use pgvector cosine distance search
        # Note: cosine distance = 1 - cosine similarity
        # Smaller distance = more similar
        # LEFT JOIN full_descriptions to get full description
        result = self.db.execute(
            text("""
                SELECT
                    j.id,
                    j.adzuna_id,
                    j.title,
                    j.company_name,
                    j.category,
                    j.location,
                    j.salary_min,
                    j.salary_max,
                    j.description,
                    j.redirect_url,
                    fd.full_description,
                    1 - (e.embedding <=> :query_vector) as similarity
                FROM adzuna_jobs j
                JOIN job_embeddings e ON j.id = e.job_id
                LEFT JOIN full_descriptions fd ON j.id = fd.job_id
                ORDER BY e.embedding <=> :query_vector
                LIMIT :top_k
            """),
            {"query_vector": str(query_vector), "top_k": top_k},
        )

        jobs = []
        for row in result:
            jobs.append(
                {
                    "id": row[0],
                    "adzuna_id": row[1],
                    "title": row[2],
                    "company_name": row[3],
                    "category": row[4],
                    "location": row[5],
                    "salary_min": row[6],
                    "salary_max": row[7],
                    "description": row[8],
                    "redirect_url": row[9],
                    "full_description": row[10],
                    "similarity": round(row[11], 4) if row[11] else 0,
                }
            )

        logger.info(f"Found {len(jobs)} matching jobs")

        return jobs

    def get_stats(self) -> dict:
        """Get embedding statistics"""
        total_jobs = self.db.query(Job).count()
        embedded_jobs = self.db.execute(text("SELECT COUNT(*) FROM job_embeddings")).scalar()

        return {
            "total_jobs": total_jobs,
            "embedded_jobs": embedded_jobs,
            "pending_jobs": total_jobs - embedded_jobs,
        }
