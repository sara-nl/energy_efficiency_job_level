from configparser import ConfigParser
import pandas as pd
import sqlite3
import subprocess
import re
import pandas as pd
import json



def parse_config(filename):
    """Parse the configuration file to get Prometheus connection details."""
    parser = ConfigParser()
    parser.read(filename)
    return parser



def auth_tokens(prom_setting, prom_pass):
    

    # Parse the main configuration (e.g., host and port)
    config = parse_config(prom_setting)
    # Parse the credentials configuration (e.g., username and password)
    config_pw = parse_config(prom_pass)

    # Extract Prometheus connection details from the config
    prometheus_config = config['Prometheus']
    prometheus_config_pw = config_pw['Prometheus']

    host = prometheus_config['host']
    port = prometheus_config['port']
    username = prometheus_config_pw['username']
    password = prometheus_config_pw['password']

    return host, port, username, password



def chunking_time(start_time, end_time, step, query_length):
    '''
    Prom. only returns 11000 sample of a time series, so for a given start and
    end time we need to ensure in each query we are asking Prom valid length.
    this function chunk the given length. 
    '''
    time_index = pd.date_range(start=start_time, end=end_time,freq=step, unit='s')
    chunk_time = time_index[::query_length]
    # Ensure the last timestamp is included
    if time_index[-1] not in chunk_time:
        chunk_time = chunk_time.append(pd.DatetimeIndex([time_index[-1]]))
    return chunk_time



def get_list(file_path):
    names = []
    with open(file_path, "r") as file:
        for line in file:
            # Strip leading/trailing whitespace and check if line is a comment
            line = line.strip()
            if not line.startswith("#") and line:  # If the line is not a comment and is not empty
                names.append(line)

    return names



def node_type(node_name):
    maping = {'int': 'interactive',
              'tcn': 'thin',
              'fcn': 'fat',
              'hcn': 'high_memory',
              'gcn': 'gpu',
              'srv': 'service'
              }
    return maping.get(node_name, 'other')






# Function to dynamically build the insert or update statement
def insert_or_update(node, timestamp, data_dict):
    # Create a list of columns to insert/update dynamically
    columns_to_update = ", ".join([f"{key} = COALESCE(EXCLUDED.{key}, metrics.{key})" for key in data_dict.keys()])
    
    # Insert the keys and values dynamically
    columns_part = ", ".join(['node', 'timestamp'] + list(data_dict.keys()))
    placeholders = ", ".join(['?', '?'] + ['?' for _ in data_dict.keys()])
    
    # Prepare the SQL statement with dynamic columns
    query = f"""
        INSERT INTO metrics ({columns_part})
        VALUES ({placeholders})
        ON CONFLICT(node, timestamp)
        DO UPDATE SET {columns_to_update};
    """
    
    return query





def dropping_duplicated_rows(db_name, table_name):


    # Connect to the SQLite database
    con = sqlite3.connect(db_name)

    # Create a cursor object to interact with the database
    cur = con.cursor()


    new_table_name = f"{table_name}_unique"
    cur.execute(f"""
        CREATE TABLE {new_table_name} AS 
        SELECT DISTINCT * 
        FROM {table_name};
    """)

    # # Step 2: Drop the original table
    # cur.execute("DROP TABLE your_table;")

    # # Step 3: Rename the new table to the original table's name
    # cur.execute("ALTER TABLE your_table_unique RENAME TO your_table;")

    # Commit the changes
    con.commit()

    # Close the connection
    con.close()
    return new_table_name




def merge_df_list(df_list, on='timestamp', how='outer'):
            # Start with the first DataFrame
    df_con = df_list[0]

    # Loop through the rest of the DataFrames and merge them
    for df in df_list[1:]:
        df_con = pd.merge(df_con, df, on= on, how=how)  # Use 'outer' to keep all values
        
    return df_con
        
        
        
        











def format_node_names(node_name: str) -> str: 
    """This function cleans the node names that we have
    obtained from slurm data base. 
    for example [tcn97, tcn99-tcn101] ==> tcn97, tcn99, tcn100, tcn101

    Args:
        node_name (str): _it is a string that we want to process_

    Returns:
        _string_: 
    """
    # Check if the node name contains brackets (indicating range or list format)
    if '[' in node_name:
        # Extract prefix and the numbers part
        prefix, nums = re.match(r"(\w+)\[(.+)\]", node_name).groups()
        
        # Split on comma and process each item
        formatted_nodes = []
        for part in nums.split(','):
            part = part.strip()
            if '-' in part:  # It's a range
                start, end = map(int, part.split('-'))
                formatted_nodes.extend([f"{prefix}{i}" for i in range(start, end + 1)])
            else:  # It's a single number
                formatted_nodes.append(f"{prefix}{part}")
        
        return f"{','.join(formatted_nodes)}"
    else:
        # No brackets, treat as a single node name
        return f"{node_name}"
    
    
    
    
       

def get_slurm_data(job_ids: list[int]) -> object:
    """a function for getting data from slurm

    Args:
        job_ids (list[int]): list of job ids

    Returns:
        object: a pandas dataframe
    """

    slurm_job_data = {}

    for job_id in job_ids:
        # Run the 'sacct' command with job ID and format options
        command = ['sacct', '-j', str(job_id),'--parsable',
        '--format=Submit,Eligible,Start,End,Elapsed,JobID,JobName,State,AllocCPUs,TotalCPU,NodeList']
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.stderr:
            print("Standard Error:\n", result.stderr)
        else:
            slurm_job_data[job_id] = result.stdout
        

    df = pd.DataFrame(pd.Series(slurm_job_data))
    df.index.name = 'job_id'
    df.reset_index(inplace=True)
    df.rename(columns= {0: 'feature'}, inplace=True)
    return df



def preprocess_slurm(df: object) -> object:
    """processing the slurm data

    Args:
        df (object): a data frame with job id and the feature

    Returns:
        object: a dataframe with extracted features as its columns
    """

    df['feature'] = df['feature'].str.split('\n')
    df['length_of_feature'] = [len(l) for l in df['feature'].tolist()]
    # df['length_of_feature'].value_counts()[0:10]
    
    
    
 
    lower_bound = 0
    upper_bound = len(df)
    data_processed = []

    for n in range(lower_bound, upper_bound):

        len_feature = df.iloc[n, :]['length_of_feature']
        if len_feature > 2:
            job_id =int( df.iloc[n, :]['job_id'])
            query_name = df.iloc[n, :]['feature'][0]
            signal = df.iloc[n, :]['feature'][1:-1]
            
            
            data = {'job_id': [job_id] * len(signal),
                    'query_name': [query_name] * len(signal),
                    'signal': signal}

            data_processed.append(pd.DataFrame(data))

    df = pd.concat(data_processed, axis=0)
    df['query_name'] = df['query_name'].str.split('|')
    df['signal'] = df['signal'].str.split('|')
    # get the length of signal name column
    df['length_of_query'] = [len(l) for l in df['query_name'].tolist()]
    df['length_of_signal'] = [len(l) for l in df['signal'].tolist()]
    
    


    signal_names = df['query_name'].iloc[0][0:-1]
        # for the 13 signals
    for i, signal_name in enumerate(signal_names):
        df[signal_name] = df['signal'].apply(lambda x:x[i])
        
        
        
    df['formatted_node_names'] = df['NodeList'].apply(format_node_names)
    df.drop(['query_name','signal', 'length_of_query',
                 'length_of_signal', 'JobName'], axis=1, inplace=True)

    df.rename(columns={"JobID":"Slurm_job_id"}, inplace=True)

 
    df.sort_values(by='job_id', inplace=True)


    return df



def extract_job_data(file_path, key_list, pref_vars_key_list, check_key_list):
    with open(file_path, mode="r", encoding="utf-8") as f:
        ben_data = json.load(f)
    try:
        testcases = ben_data['runs'][0]['testcases']
    except:
        print("data is corrupt")
        return [{}]
    
    all_job_data = []
    for testcase in testcases:
        job_data = {}
        
        
        for key in key_list:
            job_data[key] = testcase.get(key, 'no_data')
                
                
                
        pref_var_data = testcase.get('perfvars', {})
        
        if isinstance(pref_var_data, list):
            pref_var_data = pref_var_data[0]
            
        elif pref_var_data==None:
            pref_var_data = {}
    
    
        for key in pref_vars_key_list:
            
            job_data["pref_" + key] = pref_var_data.get(key, 'no_data')
         
        
        check_var_data = testcase.get('check_vars', {})  
        for key in check_key_list:
            job_data[key] = check_var_data.get(key, 'no_data')

            
            
    
        
      
      
      
        all_job_data.append(job_data)
        
        
    return all_job_data