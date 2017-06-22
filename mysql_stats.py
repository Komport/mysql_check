from  DBConn import DBConn as db

def extract_data(IP,Port,ID,local_con):
    db_con = db(IP,Port)
    db_con.open_connection()
    global_qwr = "SHOW GLOBAL STATUS"
    process_qwr = "SHOW PROCESSLIST"
    prev_data_qwr = "SELECT * from server_stats_tmp where server_id = "+str(ID)
    global_status = db_con.get_data(global_qwr)
    process_list = db_con.get_data(process_qwr)
    prev_data = local_con.get_data(prev_data_qwr,1)
    all_metrics = ["Uptime"
               ,"Open_tables"
               ,"Aborted_connects"
               ,"Threads_connected"
               ,"Threads_running"
               ,"Max_used_connections"
               ,"Questions"
               ,"Queries"
               ,"Slow_queries"
               ,"Select_full_join"
               ,"Created_tmp_disk_tables"
               ,"Handler_read_first"
               ,"Handler_read_key"
               ,"Handler_read_next"
               ,"Handler_read_prev"
               ,"Handler_read_rnd"
               ,"Handler_read_rnd_next"
               ,"Innodb_buffer_pool_wait_free"
               ,"Innodb_row_lock_waits"
               ,"Com_select"
               ,"Com_insert"
               ,"Com_delete"
               ,"Com_update"]
    inc_metrics = ["Open_tables"
               ,"Aborted_connects"
               ,"Questions"
               ,"Queries"
               ,"Slow_queries"
               ,"Select_full_join"
               ,"Created_tmp_disk_tables"
               ,"Handler_read_first"
               ,"Handler_read_key"
               ,"Handler_read_next"
               ,"Handler_read_prev"
               ,"Handler_read_rnd"
               ,"Handler_read_rnd_next"
               ,"Innodb_buffer_pool_wait_free"
               ,"Innodb_row_lock_waits"
               ,"Com_select"
               ,"Com_insert"
               ,"Com_delete"
               ,"Com_update"]
    raw_data = {}
    raw_data['Server_id'] = ID
    sum = 0
    for line in global_status:
            if line[0] in all_metrics:
                raw_data[line[0]] = line[1]
                
    for line in process_list:
        if line[4] == "Query" and line[5] >= 0:
            sum = sum+1
    raw_data['Long_running_transactions'] = sum
    raw_data['timestamp'] = """NOW()"""
    new_data = raw_data
    
    prev_data = prev_data[0]
    
    
    
    for line in new_data:
        
        if line in inc_metrics:
            
            if int(new_data[line]) > int(prev_data[line]):
                print (line)
                new_data[line] = int(new_data[line])-int(prev_data[line])
            elif int(new_data[line]) == int(prev_data[line]):
                new_data[line] = 0
            
                
        
    return(raw_data,new_data)

#Insert data
def insert_data(data,local_con,raw = True):
    ins_data=(data['Server_id'],data['Uptime'],data['Threads_connected'],data['Threads_running'],data['Max_used_connections']
              ,data['Aborted_connects'],data['Questions'],data['Queries'],data['Select_full_join'],data['Created_tmp_disk_tables']
              ,data['Handler_read_first'],data['Handler_read_key'],data['Handler_read_next'],data['Handler_read_prev']
              ,data['Handler_read_rnd'],data['Handler_read_rnd_next'],data['Innodb_row_lock_waits'],data['Innodb_buffer_pool_wait_free']
              ,data['Open_tables'],data['Long_running_transactions'],data['Slow_queries'],data['Com_select'],data['Com_insert'],data['Com_update']
              ,data['Com_delete'])
    if raw:
        tbl_name = "server_stats_tmp"
    else:
        tbl_name = "server_stats"
    query = """INSERT INTO """ + tbl_name+""" (server_id
,uptime
,threads_connected
,threads_running
,max_used_connections
,aborted_connects
,questions
,queries
,select_full_join
,created_tmp_disk_tables
,handler_read_first
,handler_read_key
,handler_read_next
,handler_read_prev
,handler_read_rnd
,handler_read_rnd_next
,innodb_row_lock_waits
,innodb_buffer_pool_wait_free
,open_tables
,long_running_transactions
,slow_queries
,com_select
,com_insert
,com_update
,com_delete
,timestamp)
values (%s,%s,%s,%s,%s,%s
,%s,%s,%s,%s,%s,%s,%s,%s
,%s,%s,%s,%s,%s,%s,%s,%s
,%s,%s,%s,NOW())"""
    
    local_con.insert_data(query,ins_data)


##Main Loop
def main_loop():
    local_con = db("localhost",13306)
    local_con.open_connection(2)
    table_nm = "server_stats_tmp"
    #local_con.truncate_table(table_nm)
    query = "SELECT ID, IP, Port FROM srv_list"
    result = local_con.get_data(query)
    
    new_data = {}
    raw_dict = {}
    raw_data = {}
    for (ID, IP, Port) in result:
          
      try:
        (raw_data,new_data) = extract_data(IP,Port,ID,local_con)
        insert_data(new_data,local_con,False)
        raw_dict[str(ID)] = raw_data
        
      except Exception as e:
        print (e)
        print ("Error here")
    local_con.truncate_table(table_nm)
    #print(raw_dict)
    for line in raw_dict:
        insert_data(raw_dict[line],local_con)
main_loop()
