from fastapi import FastAPI, status, Response
from reveal.report_builder import BuildReport
from reveal import property_match, pulse, logging, propertyfinder, pulse_buildings
from reveal import job 
from reveal import link_ads, database_util
from reveal.job import JobExecution
from fastapi import BackgroundTasks 
import traceback


app = FastAPI()
allowedCommunities = ["Dubai Marina", "Jumeirah Lake Towers", "Al Furjan", "Jumeirah Village Circle", "Jumeirah Beach Residence"]
database_util.init_database()

@app.post("/be/report/{community}", status_code=201)
def read_root( community: str, response: Response):
    if community not in allowedCommunities:
        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return f"community: '{community}' not allowed available communities {allowedCommunities}"
    BuildReport().clean_report(community)
    BuildReport().build_community_report(community)
    return "done"

@app.delete("/be/report/{community}", status_code=201)
def clean_report(community:str , response: Response):
    if community not in allowedCommunities:
        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return 
    BuildReport().clean_report(community) 
    return "all report cleared"

@app.get("/be/link/{community}/stats", status_code=200)
def link_stats(community, response: Response):
    if community not in allowedCommunities:
        response.status_code = 403
        return f"allowed communities {allowedCommunities}"
    return link_ads.link_stats(community) 



@app.post("/be/link/{community}", status_code=201)
def link(community, response: Response):
    '''
     link communities,using standard fuzzy logic
    '''
    if community not in allowedCommunities:
        response.status_code = 403 
        return f"allowed communities {allowedCommunities}"
    property_match.match(community)
    return "done"

@app.delete("/be/link/{community}", status_code=201)
def link_community(community, response: Response):
    if community not in allowedCommunities:
        response.status_code = 403 
        return f"allowed communities {allowedCommunities}"
    property_match.remove_link(community)
    return "done"

    
@app.post("/be/link/towers/{community}", status_code=201)
def unlink_community(community, response: Response):
    '''
     remove  
    '''
    if community not in allowedCommunities:
        response.status_code = 403 
        return f"allowed communities {allowedCommunities}"
    property_match.match(community)
    return "done"
    
@app.post("/be/pulse", status_code=201)
def download_and_process_pulse_data(backgound_tasks: BackgroundTasks):
    '''
        download the pulse sales transaction file and store them
    '''
    job_execution = job.start('pulse-sync')
    backgound_tasks.add_task(_download_and_processPulse_data, job_execution)
    return job_execution.id

@app.post("/be/pulse/buildings", status_code=201)
def load_buildings(response: Response):
    try:
        pulse_buildings.load()
    except Exception:
        response.status_code= 500
        logging.err(f"error loading file {traceback.format_exc()}")

def _download_and_processPulse_data(job_execution: JobExecution):
    try:
        job.progress(job_execution, "start download pulse transaction file")
        logging.info("start pulse data download")
        filename = pulse.download_transaction()
        job.progress(job_execution, "pulse file download completed")
        logging.info("download completed!")
        if filename is not None:
            logging.info("pulse data downloaded ${filename} start processing")
            new_items = pulse.load(filename)
            job_execution.success(f"processed {new_items} records")
        else:
            job_execution.error("download file error")
    except Exception :
        job_execution.error(f"exception {Exception}!")
    finally:
        job.complete(job_execution)

@app.post("/be/propertyfinder/", status_code=201)
def download_and_process_propertyfinder_ads( backgound_tasks: BackgroundTasks, pages: int = 300  ):
    '''
        download, parse and process propertyfinder ads
    '''

    job_execution = job.start("get propertyfinder ads")
    backgound_tasks.add_task(propertyfinder.get_ads,pages, job_execution)

    return job_execution.id

@app.get("/be/job-execution/{job_id}", status_code=201)
def get_status_by_job_id(job_id: int, response: Response):
    job_status = job.get_status(job_id)
    if job_status is None:
        response.status_code = 403 
        return "not found"
    return job_status[0]
