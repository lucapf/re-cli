from fastapi import FastAPI, status, Response
from reveal.report_builder import BuildReport
from reveal import property_match, pulse, logging, propertyfinder
from reveal import job
from reveal.job import JobExecution
from fastapi import BackgroundTasks 

app = FastAPI()
allowedCommunities = ["Dubai Marina", "Jumeirah Lake Towers", "Al Furjan", "Jumeirah Village Circle"]
@app.post("/be/report/{community}", status_code=201)
def read_root( community: str, response: Response):
    if community not in allowedCommunities:
        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return f"allowed communities {allowedCommunities}"
    BuildReport().build_community_report(community)
    return "done"

@app.delete("/be/report", status_code=201)
def clean_report( ):
   BuildReport().clean_report() 
   return "all report cleared"

@app.post("/be/link/{community}", status_code=201)
def link(community, response: Response):
    if community not in allowedCommunities:
        response.status_code = 403 
        return f"allowed communities {allowedCommunities}"
    property_match.match(community)
    return "done"

@app.delete("/be/link/{community}", status_code=201)
def link_community(community, response: Response):
    '''
     link communities,using standard fuzzy logic 
    '''
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
    return f"background task ({job_execution.id}) queued"


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
    job_name = "process propertyfinder ads"
    job_execution = job.start(job_name)
    backgound_tasks.add_task(propertyfinder.get_ads,pages, job_execution)
    return f"job {job_name} - {job_execution.id} queued"







