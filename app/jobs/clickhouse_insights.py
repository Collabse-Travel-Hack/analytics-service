from hatchet import Job
from insights_processing import aggregate_data

class AggregateInsightsJob(Job):
    def execute(self):
        insights = aggregate_data()
        #store_insights(insights)