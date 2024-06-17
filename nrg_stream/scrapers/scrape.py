"""
"""
import sys
import os

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream/scrapers"))

from utils import Utils
from price_scraper import PriceScraper
from load_scraper import LoadScraper
from wind_scraper import WindScraper
from model_run_scraper import ModelRunScraper
from scraper_job import ScraperJob


class Scraper:
    """ """

    def __init__(self):
        """ """
        self.u = Utils()
        self.job_ids = []
        self.errors = []
        self.cnt = 0

    def _get_jobs(self):
        """ """
        query = "select id from scraper_jobs where status = 'created' order by created_at asc"
        records = self.u.execute_query(query, dict_cursor="True", fetch="all")
        self.job_ids = [x["id"] for x in records]

    def run(self, limit=0):
        """ """
        self._get_jobs()
        if limit > 0:
            self.job_ids = self.job_ids[:limit]
        for job_id in self.job_ids:
            scraper_job = ScraperJob(id=job_id)
            scraper_job.status = "running"
            try:
                self._run_scraper(scraper_job)
            except Exception as e:
                self.errors.append({"stream_id": scraper_job.stream_id, "error": e})
                scraper_job.status = "failed"
            self.cnt += 1
            if self.cnt % 10 == 0:
                print(f"completed {self.cnt} jobs")

    def _run_scraper(self, scraper_job):
        """ """
        if scraper_job.scraper == "price":
            scraper = PriceScraper(
                stream_id=scraper_job.stream_id,
                s_date=scraper_job.s_date,
                e_date=scraper_job.e_date,
            )
            scraper.run()
            scraper_job.status = "complete"
        elif scraper_job.scraper == "load":
            scraper = LoadScraper(
                stream_id=scraper_job.stream_id,
                s_date=scraper_job.s_date,
                e_date=scraper_job.e_date,
            )
            scraper.run()
            scraper_job.status = "complete"
        elif scraper_job.scraper == "wind":
            scraper = WindScraper(
                stream_id=scraper_job.stream_id,
                s_date=scraper_job.s_date,
                e_date=scraper_job.e_date,
            )
            scraper.run()
            scraper_job.status = "complete"
        elif scraper_job.scraper == "nrg_ercot_fcst":
            scraper = ModelRunScraper(
                stream_id=scraper_job.stream_id,
                s_date=scraper_job.s_date,
                e_date=scraper_job.e_date,
            )
            scraper.run()
            scraper_job.status = "complete"
        else:
            raise ValueError(f"invalid scraper {scraper_job.scraper}")

    def get_scraper_job_info(self):
        """ """
        query = (
            "select scraper, status, count(*) as cnt from scraper_jobs group by 1, 2"
        )
        records = self.u.execute_query(query, dict_cursor="True", fetch="all")
        return records


# stream_first_date = np.datetime64(ps._localize_db_dt(ps.stream_record.first_date))
# stream_last_date = np.datetime64(ps._localize_db_dt(ps.stream_record.last_date))

# from model_run_scraper import ModelRunScraper

# for stream_id in stream_ids:
#     mrs = ModelRunScraper(stream_id=stream_id, s_date="01-02-2023", e_date="01-01-2024")
#     mrs.run()
