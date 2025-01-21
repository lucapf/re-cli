from fastapi import FastAPI, status, Response
from reveal.report_builder import BuildReport
from reveal import property_match, pulse, logging, propertyfinder

app = FastAPI()
allowedCommunities = ["Dubai Marina", "Jumeirah Lake Towers", "Al Furjan", "Jumeirah Village Circle"]
@app.post("/report/{community}", status_code=201)
def read_root( community: str, response: Response):
    if community not in allowedCommunities:
        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return f"allowed communities {allowedCommunities}"
    BuildReport().build_community_report(community)
    return "done"

@app.delete("/report", status_code=201)
def clean_report( ):
   BuildReport().clean_report() 
   return "all report cleared"


@app.delete("/link/towers/{community}", status_code=201)
def link_community(community, response: Response):
    '''
     link communities,using standard fuzzy logic 
    '''
    if community not in allowedCommunities:
        response.status_code = 403 
        return f"allowed communities {allowedCommunities}"
    property_match.remove_link(community)
    return "done"

    
@app.post("/link/towers/{community}", status_code=201)
def unlink_community(community, response: Response):
    '''
     remove  
    '''
    if community not in allowedCommunities:
        response.status_code = 403 
        return f"allowed communities {allowedCommunities}"
    property_match.match(community)
    return "done"
    
@app.post("/pulse/", status_code=201)
def download_and_process_pulse_data(response:Response):
    '''
        download the pulse sales transaction file and store them
    '''
    logging.info("start pulse data download")
    filename = pulse.download_transaction()
    logging.info("download completed!")
    if filename is not None:
        logging.info("pulse data downloaded ${filename} start processing")
        new_items = pulse.load(filename)
        return f"loaded ${new_items}"
    else:
        response.status_code = 500
        return "download file error"

@app.post("/propertyfinder/", status_code=201)
def download_and_process_propertyfinder_ads(response: Response):
    '''
        download, parse and process propertyfinder ads
    '''
    propertyfinder.get_ads(300)




