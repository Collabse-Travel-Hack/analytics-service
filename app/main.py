from hatchet import Hatchet
from app.jobs import AggregateInsightsJob

if __name__ == "__main__":
    app = Hatchet()
    
    # Schedule the job to run at the desired interval   
    AggregateInsightsJob().schedule(interval="daily")
    
    app.run()