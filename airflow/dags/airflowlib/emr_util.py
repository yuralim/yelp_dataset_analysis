import logging, requests, json, time
from jinja2 import Template

def create_spark_session(master_dns):
    """Creates an interactive scala spark session"""
    host = 'http://' + master_dns + ':8998'
    data = {'kind': 'pyspark'}
    headers = {'Content-Type': 'application/json'}
    request_url = host + '/sessions'

    print("create_spark_session: " + request_url)
    response = requests.post(request_url, data=json.dumps(data), headers=headers)
    print(response.json())
    
    return response.headers

def wait_for_idle_session(master_dns, response_headers):
    """Wait for the session to be idle or ready for job submission"""
    status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location']
    
    while status != 'idle':
        time.sleep(3)
        
        print("wait_for_idle_session: " + session_url)
        status_response = requests.get(session_url)
        status = status_response.json()['state']
        print('Session status: ' + status)
    
    return session_url

def kill_spark_session(session_url):
    """Close the current spark session"""
    requests.delete(session_url, headers={'Content-Type': 'application/json'})

def submit_statement(session_url, statement_path, input_file_name, temp_file_path):
    """Submits the python code as a simple JSON command to the Livy server"""
    statements_url = session_url + '/statements'
    
    with open(statement_path, 'r') as f:
        code = f.read()

    data = {
        'code': Template(code).render(input_file_name=input_file_name, temp_file_path=temp_file_path)
    }
    
    response = requests.post(statements_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    print(response.json())
    
    return response

def track_statement_progress(master_dns, response_headers):
    """Function to help track the progress of the scala code submitted to Apache Livy"""
    statement_status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location'].split('/statements', 1)[0]
    
    # Poll the status of the submitted scala code
    while statement_status != 'available':
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
        statement_url = host + response_headers['location']
        statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
        statement_status = statement_response.json()['state']
        print('Statement status: ' + statement_status)

        #logging the logs
        lines = requests.get(session_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
            print(line)

        if 'progress' in statement_response.json():
            print('Progress: ' + str(statement_response.json()['progress']))

        time.sleep(10)
    
    final_statement_status = statement_response.json()['output']['status']
    if final_statement_status == 'error':
        print('Statement exception: ' + statement_response.json()['output']['evalue'])
        for trace in statement_response.json()['output']['traceback']:
            print(trace)
        raise ValueError('Final Statement Status: ' + final_statement_status)
    
    print('Final Statement Status: ' + final_statement_status)