


with DAG(
    'test_dag',
    default_args = default_args,
    description = 'Test DAG',
    schedule_interval = "@daily",
    catchup=False, # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html?highlight=catchup#catchup
    max_active_runs=1, # Only allow one run of this DAG to be running at any given time
    tags=['test']) as test_dag:
    
    @task (dag=test_dag)
    def get_users_and_plan_processing(**kwargs):
        
        execution_dtm = datetime.datetime.now()   
        execution_date = execution_dtm.strftime("%Y_%m_%d_%H_%M")
        filename = 'users_' + execution_date + '.p'
    
        # user_data.to_pickle(filename)
        
        ti = kwargs['ti']
        ti.xcom_push(key='filename', value=filename)
         
        return [1,2,3] # list(range(number_of_chunks))
    
    @task_group(
            group_id="process_chunk_taskgroup",
            dag=test_dag)
    def process_chunk_taskgroup(chunk_no):
        
        @task (dag=test_dag, max_active_tis_per_dag=1, provide_context=True)
        def chunk_processing(chunk_no, **kwargs):
            import time

            print (f'Processing chunk {chunk_no}...')
            ti = kwargs['ti']
            filename = ti.xcom_pull(key='filename', task_ids='get_users_and_plan_processing')
            print (f'Users data is in {filename}')
            time.sleep(2)
            return 'name'+str(chunk_no)
        
        @task (dag=test_dag, max_active_tis_per_dag=1)
        def chunk_uploading(filename):
            import time
            
            print (f'Uploading {filename}...')

            print (filename)
            time.sleep(2)
            return 
        
        return chunk_uploading(chunk_processing(chunk_no))
        
    process_chunk_taskgroup_object = process_chunk_taskgroup.expand(chunk_no = get_users_and_plan_processing())
    
    @task (dag=test_dag)
    def remove_temp_file(**kwargs):
        import os
        #get file name via XCom
        ti = kwargs['ti']
        filename = ti.xcom_pull(key='filename', task_ids='get_users_and_plan_processing')
        print (f'Deleting file: {filename}')
        
        os.remove(filename)
    
    process_chunk_taskgroup_object >> remove_temp_file()