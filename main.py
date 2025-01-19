from fastapi import FastAPI, status, Response, Request
from fastapi import status
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from reveal.report_builder import BuildReport

app = FastAPI()
allowedCommunities = ["Dubai Marina", "Jumeirah Lake Towers"]
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



