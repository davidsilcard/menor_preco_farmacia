import unittest
from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base, OperationJob, SearchJob
from src.services.operation_jobs import _claim_next_operation_job
from src.services.search_jobs import _claim_next_search_job


class AtomicClaimTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

    def tearDown(self):
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def test_claim_next_operation_job_marks_oldest_queued_as_processing(self):
        now = datetime.now(UTC).replace(tzinfo=None)
        with self.Session() as session:
            session.add_all(
                [
                    OperationJob(
                        job_type="a",
                        requested_by="ops_api",
                        status="queued",
                        payload_fingerprint="a",
                        created_at=now - timedelta(minutes=2),
                        updated_at=now - timedelta(minutes=2),
                    ),
                    OperationJob(
                        job_type="b",
                        requested_by="ops_api",
                        status="queued",
                        payload_fingerprint="b",
                        created_at=now - timedelta(minutes=1),
                        updated_at=now - timedelta(minutes=1),
                    ),
                ]
            )
            session.commit()

        with self.Session() as session:
            claimed = _claim_next_operation_job(session)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.job_type, "a")
            self.assertEqual(claimed.status, "processing")

        with self.Session() as session:
            jobs = session.query(OperationJob).order_by(OperationJob.created_at.asc(), OperationJob.id.asc()).all()
            self.assertEqual([job.status for job in jobs], ["processing", "queued"])

    def test_claim_next_search_job_marks_oldest_queued_as_processing(self):
        now = datetime.now(UTC).replace(tzinfo=None)
        with self.Session() as session:
            session.add_all(
                [
                    SearchJob(
                        query="produto a",
                        normalized_query="produto a",
                        cep="89254300",
                        status="queued",
                        requested_by_tool="search_products",
                        created_at=now - timedelta(minutes=2),
                        updated_at=now - timedelta(minutes=2),
                    ),
                    SearchJob(
                        query="produto b",
                        normalized_query="produto b",
                        cep="89254300",
                        status="queued",
                        requested_by_tool="search_products",
                        created_at=now - timedelta(minutes=1),
                        updated_at=now - timedelta(minutes=1),
                    ),
                ]
            )
            session.commit()

        with self.Session() as session:
            claimed = _claim_next_search_job(session)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.query, "produto a")
            self.assertEqual(claimed.status, "processing")

        with self.Session() as session:
            jobs = session.query(SearchJob).order_by(SearchJob.created_at.asc(), SearchJob.id.asc()).all()
            self.assertEqual([job.status for job in jobs], ["processing", "queued"])


if __name__ == "__main__":
    unittest.main()
