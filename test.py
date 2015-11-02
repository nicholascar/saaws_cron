import datetime
from datetime import timedelta
import cron

cron.job_check_values(datetime.datetime.now() - timedelta(hours=24), 'days')