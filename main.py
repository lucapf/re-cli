from fastapi import FastAPI, status, Response, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

app = FastAPI()
app.mount("/static", StaticFiles(directory= "static"), name="static")
templates = Jinja2Templates(directory="templates")


