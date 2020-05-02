from utils import fullpath 
from fastapi import FastAPI 
import uvicorn 
from models import (
  urisaki, 
  gzaikozan, 
)

app = FastAPI()

app.include_router(urisaki.router, prefix='/urisaki')
app.include_router(gzaikozan.router, prefix='/gzaikozan')

if __name__ == '__main__': 
  uvicorn.run(app)