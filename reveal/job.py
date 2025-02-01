from reveal import database_util, logging

class JobExecution(object):
    def __init__(self, job_id: int):
        self.id: int = job_id
        self.is_error: bool = False
        self.message: str = '' 

    def error(self, message: str):
        self.is_error = True
        self.message = message

    def success(self, message: str):
        self.is_error = False 
        self.message = message

    def is_success(self):
        return not self.is_error 

    def get_status(self):
      return "completed" if self.is_success() else "failed"

def start(name:str)->JobExecution:
    job_id = database_util.fetchone("select nextval('reveal_sequence') as newval")[0]
    query="insert into job_execution (id, name, started_at, status) values(%s, %s, now(), 'created')"
    database_util.execute_insert_statement(query, tuple([job_id, name]))
    logging.info(f"created job with id; {job_id}")
    return JobExecution(job_id)

def progress(job_execution: JobExecution, message: str):
    query="""update job_execution 
            set log = concat(log , '\r\n',(now() - started_at)::varchar , ' - ' ,%s::varchar) 
            where id= %s"""

    database_util.execute_insert_statement(query, tuple([message, job_execution.id]))

def complete(job_execution: JobExecution):
    query="""update job_execution 
                set status=%s, 
                    log = concat(log , '\r\n',(now() - started_at)::varchar , ' - ' ,%s::varchar) 
                where id= %s"""
    database_util.execute_insert_statement(query, 
           tuple([job_execution.get_status(),
                  job_execution.message,
                  job_execution.id
                  ]))

def get_status(job_id: int):
    query='select status from job_execution where id = %s'
    return database_util.fetchone(query, tuple([job_id]))
