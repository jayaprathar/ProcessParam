
from sqlalchemy import create_engine,text
import psutil
import shutil
import sqlalchemy
from datetime import datetime
import os
from datetime import datetime,timedelta,date
import sys
import time
from dateutil.relativedelta import relativedelta

live = 'live'

def createFolder(directory,file_name,data):

    date_time=datetime.now()
    curtime1=date_time.strftime("%d/%m/%Y %H:%M:%S")
    curtime2=date_time.strftime("%d-%m-%Y")
    directory = directory + str(curtime2) + '/'

    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        f= open(directory+str(file_name)+".txt","a+")      
        f.write(curtime1 +" "+ str(data) +"\r\n")
        f.close()

        # deleting old log files
        old_date = (datetime.now() + timedelta(days=-5)).date()
        file_list = os.listdir('Log')
        for file in file_list:
            try:
                file_date = datetime.strptime(file, '%d-%m-%Y').date()
            except:
                shutil.rmtree('Log/'+file)
            if file_date <= old_date:
                shutil.rmtree('Log/'+file)

    except OSError:
        print ('Error: Creating directory. ' +  directory)


def rnc_loss_update(): 

    loss_rpl_sel_query = ''' select * from digital_factory_ent_v1.loss_replace_dtl where pp_status = 'yes' '''
    rpl_rec= db_connection.execute(text(loss_rpl_sel_query)).mappings().all()

    for rec in rpl_rec:

        try:

            id  = rec['id']
            rpl_mill_date = rec['mill_date']
            rpl_mill_shift = rec['mill_shift']
            machine_id = rec['machine_id']
            month_year  = str(mill_month[rpl_mill_date.month])+str(rpl_mill_date.year)
            current_stop_begin_time = rec['current_stop_begin_time']
            current_stop_duration = rec['current_stop_duration']
            duration = timedelta(seconds=current_stop_duration)
            current_stop_end_time = current_stop_begin_time + duration

            if (mill_date != rpl_mill_date and mill_shift != rpl_mill_shift) :

                tbl_name_sel_query = f'''select table_name_comp from process_param.master_equipment_lookup where equipment_id = '{machine_id}' '''
                table_name= db_connection.execute(text(tbl_name_sel_query)).mappings().all()
                createFolder('Log/','rnc_update',f" Table_Name : {table_name} .")

                if len(table_name) >0:
                    tbl_name = table_name[0]['table_name_comp']

                    t_name = tbl_name.replace('mon_yr',month_year)
                    
                    sub_loss_sel_query = f''' 
                                    SELECT
                                            ifnull(mlc.sub_loss_id,0) AS sub
                                    FROM 
                                            digital_factory_ent_v1_completed.loss_{month_year} l

                                    INNER JOIN 
                                            digital_factory_ent_v1.master_loss_code mlc ON mlc.loss_id = l.current_stop_code

                                    WHERE 
                                            
                                            l.machine_id = '{machine_id}' 
                                            AND l.current_stop_code <> 0 
                                            AND l.current_stop_begin_time = '{current_stop_begin_time}' 
                                            and l.current_stop_duration = '{current_stop_duration}' '''

                    
                    createFolder('Log/','rnc_update',f" SubLoss Sel Query : {sub_loss_sel_query} .") 
                    
                    sub_loss_recs = db_connection.execute(text(sub_loss_sel_query)).mappings().all()

                    sub_loss_id = sub_loss_recs[0]['sub']

                    pp_sub_upt_query = f'''UPDATE 
                                                process_param_completed.{t_name} pp 
                                            SET 
                                                sub_loss_id = '{sub_loss_id}',
                                                sync_status = 'rnc_yes'
                                            WHERE
                                                pp.equipment_id = '{machine_id}' 
                                            AND pp._TIMESTAMP >= '{current_stop_begin_time}'
                                            AND pp._TIMESTAMP <= '{current_stop_end_time}'
                                            
                                            '''
                    
                    createFolder('Log/','rnc_update',f" SubLoss Upt Query : {pp_sub_upt_query} .") 
                    upt_query = db_connection.execute(text(pp_sub_upt_query))
                    ro_count = upt_query.rowcount
                    db_connection.commit()
                    createFolder('Log/','rnc_update',f" SubLoss Upt Query RowCount: {ro_count} .") 

                rpl_update_query = f'''update digital_factory_ent_v1.loss_replace_dtl set pp_status = 'no' where id = '{id}'  '''
                db_connection.execute(text(rpl_update_query))
                db_connection.commit()
                createFolder('Log/','rnc_update',f" Rnc_rpl Upt Query : {rpl_update_query} .")
        
        except Exception as e:
                # createFolder('Log/',live,f"Cant connect mysql server -->> {e}")  
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                createFolder('Log/','rnc_update_error',f"Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")               
           
            

def get_send_data(table_name1,next_month_year_str): 

    try:
    
        union_all = ''

        if next_month_year_str != '':
            
            union_all = f''' UNION ALL SELECT product_start_time,product_end_time,product_unique_id,machine_id,is_show_hrwise,product_ref_id FROM
                        digital_factory_ent_v1_completed.routecard_{next_month_year_str} WHERE is_show_hrwise = 'no' '''
        
        
        createFolder('Log/',live,f" inside function ,Table name : {table_name1} .") 
    
        query = f''' SELECT pp.sync_status,pp.id,pp._TIMESTAMP,l.bt,l.et,l.current_stop_code ,ifnull(mlc.sub_loss_id,0) sub_loss_id ,ifnull(cr.product_ref_id,'') as product_ref_id,
                    ifnull(mp.route_card_no,'') as route_card,ifnull(mp.item_code,'') as item_code
	
                    FROM 
                    	(SELECT id,_NAME,_NUMERICID,_VALUE,_TIMESTAMP,_QUALITY,sync_status, equipment_id FROM {table_name1} ) pp 

                    	left JOIN 

                    	(SELECT 
                    		lo.machine_id,
                    		lo.product_ref_id,
                    		lo.current_stop_code,
                    		lo.current_stop_begin_time bt,
                    		DATE_ADD(lo.current_stop_begin_time, INTERVAL lo.current_stop_duration SECOND) et

                    FROM
                    		digital_factory_ent_v1_completed.loss_{month_year} lo where lo.current_stop_code <> 0

                    		) AS l
                    	ON l.machine_id = pp.equipment_id AND pp._TIMESTAMP >= l.bt AND pp._TIMESTAMP <= l.et 

                    	LEFT JOIN digital_factory_ent_v1.master_loss_code mlc ON mlc.loss_id = l.current_stop_code

                       LEFT JOIN (SELECT product_start_time,product_end_time,product_unique_id,machine_id,is_show_hrwise,product_ref_id FROM digital_factory_ent_v1_completed.routecard_{month_year} WHERE is_show_hrwise = 'no' {union_all} )  cr 
                       ON cr.machine_id = pp.equipment_id AND pp._TIMESTAMP >= cr.product_start_time AND pp._TIMESTAMP <= cr.product_end_time
                    
                        LEFT JOIN digital_factory_ent_v1.master_product mp ON mp.product_unique_id = cr.product_unique_id
                        where pp.sync_status = 'yes'

                         ''' 

        createFolder('Log/',live,f"SELECT QUERY : {query}") 
        sync_status_update = 'pp_yes'
        all_records= db_connection.execute(text(query)).mappings().all()
        
        
        createFolder('Log/',live,f"number changed_recs in table {table_name1} : {len(all_records)} .")
        update_param(all_records,sync_status_update,table_name1)


        pre_3_days_item_code_upt_query = f'''  SELECT pp.sync_status,pp.id,pp._TIMESTAMP,ifnull(cr.product_ref_id,'') as product_ref_id,
                    ifnull(mp.route_card_no,'') as route_card,ifnull(mp.item_code,'') as item_code,pp.transaction_id
	
                    FROM 
                    	(SELECT id,_NAME,_NUMERICID,_VALUE,_TIMESTAMP,_QUALITY,sync_status, equipment_id,transaction_id FROM {table_name1} ) pp                     	

                       LEFT JOIN (SELECT product_start_time,product_end_time,product_unique_id,machine_id,is_show_hrwise,product_ref_id FROM digital_factory_ent_v1_completed.routecard_{month_year} WHERE is_show_hrwise = 'no' {union_all} )  cr 
                       ON cr.machine_id = pp.equipment_id AND pp._TIMESTAMP >= cr.product_start_time AND pp._TIMESTAMP <= cr.product_end_time
                    
                        LEFT JOIN digital_factory_ent_v1. mp ON mp.product_unique_id = cr.product_unique_id
                    WHERE  DATE(PP._TIMESTAMP) >= DATE_SUB(DATE(NOW()), INTERVAL 3 DAY) 
                    AND   DATE(PP._TIMESTAMP) <= DATE(NOW()) AND pp.transaction_id = ''
                          '''
        
        sync_status_update = 'pp_yes1'

        createFolder('Log/',live,f"item_code SELECT QUERY : {pre_3_days_item_code_upt_query}") 
        
        all_records1= db_connection.execute(text(pre_3_days_item_code_upt_query)).mappings().all()
        
        
        createFolder('Log/',live,f"number changed_recs in table {table_name1} : {len(all_records1)} .")
        update_param(all_records1,sync_status_update,table_name1)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        createFolder('Log/',live,f"Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")                                       

def update_param(all_records,sync_status_update,table_name1):

    try:  
        batch_size = 1000

        # Calculate the total number of batches
        total_batches = (len(all_records) + batch_size - 1) // batch_size

        # Process records in batches using a for loop
        for batch_index in range(total_batches):
            start_index = batch_index * batch_size
            end_index = (batch_index + 1) * batch_size
            current_batch = all_records[start_index:end_index]

            # Perform operations on the current batch
            for record in current_batch:
                # Your processing logic here

                if sync_status_update == 'pp_yes':
                    update_query = f"""
                    UPDATE {table_name1} set item_code = '{record['item_code']}' ,sub_loss_id = '{record['sub_loss_id']}' , route_card_no = '{record['route_card']}',transaction_id = '{record['product_ref_id']}',
                    sync_status = '{sync_status_update}'

                        WHERE id = {record['id']} """
                    
                    createFolder('Log/',live,f"UPDATE QUERY : {update_query}") 
                    
                    query = text(update_query)
                    db_connection.execute(query)
                    db_connection.commit()
                    
                if sync_status_update == 'pp_yes1' and record['product_ref_id'] !='':
                    
                    update_query = f"""
                    UPDATE {table_name1} set item_code = '{record['item_code']}'  , route_card_no = '{record['route_card']}',transaction_id = '{record['product_ref_id']}',
                    sync_status = '{sync_status_update}'

                        WHERE id = {record['id']} """
                
                    createFolder('Log/',live,f"UPDATE QUERY : {update_query}") 
                    
                    query = text(update_query)
                    db_connection.execute(query)
                    db_connection.commit()
    
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        createFolder('Log/',live,f"Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")                                       

            
    

def checkIfProcessRunning(processName):
    process_count = 0
    for proc in psutil.process_iter():
        try:
            if processName.lower() in proc.name().lower():
                process_count = process_count + 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return process_count

if getattr(sys, 'frozen', False):
    File_Name = os.path.basename(sys.executable)
elif __file__:
    File_Name = os.path.basename(__file__)



isFile = os.path.isfile("parameters.txt")
if isFile:
    f = open("parameters.txt","r")
    port = f.read()
if port:
    port_num = int(port)
    # print(port_num)
else :
    print(f"The given port is : {port} " )

process_count = checkIfProcessRunning(File_Name)
 

if process_count > 2:
    createFolder('Log/',live,f"Process {File_Name} Already Running !! -- Exiting - .")
    sys.exit()

else:
                
    while True: 
            try:
                #:477f9aeeb4c9f39a11b2a1d8382e2b58
        
                def mysqlconnect(port_num):
                    # db1 = create_engine(f"mysql+pymysql://root@localhost:{port_num}/process_param_completed")

            
                    db1 = create_engine(f"mysql+pymysql://root:477f9aeeb4c9f39a11b2a1d8382e2b58@localhost:{port_num}/process_param_completed")
                    db1_connection = db1.connect()
                    return db1_connection
            
            except Exception as e:
                # createFolder('Log/',live,f"Cant connect mysql server -->> {e}")  
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                createFolder('Log/',live,f"Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")               
           
            
            try: 
                db_connection= mysqlconnect(port_num)
                query = f''' SELECT 

                                    CASE
                                        WHEN
                                            CONCAT(DATE_FORMAT(ms.shift1_start_time,'%Y-%m-%d'),' ',DATE_FORMAT(now(),'%H:%i:%s')) >= DATE_SUB(ms.shift1_start_time, INTERVAL 10 MINUTE) AND 
                                            CONCAT(DATE_FORMAT(ms.shift1_start_time ,'%Y-%m-%d'),' ',DATE_FORMAT(now(),'%H:%i:%s')) <= DATE_ADD(ms.shift1_start_time, INTERVAL 10 MINUTE) 
                                        THEN 'yes'

                                        WHEN
                                            CONCAT(DATE_FORMAT(ms.shift1_start_time,'%Y-%m-%d'),' ',DATE_FORMAT(now(),'%H:%i:%s')) >= DATE_SUB(ms.shift2_start_time, INTERVAL 10 MINUTE) AND 
                                            CONCAT(DATE_FORMAT(ms.shift1_start_time,'%Y-%m-%d'),' ',DATE_FORMAT(now(),'%H:%i:%s')) <= DATE_ADD(ms.shift2_start_time, INTERVAL 10 MINUTE) 
                                        THEN 'yes'

                                        WHEN
                                            CONCAT(DATE_FORMAT(ms.shift1_start_time,'%Y-%m-%d'),' ',DATE_FORMAT(now(),'%H:%i:%s')) >= DATE_SUB(ms.shift3_start_time, INTERVAL 10 MINUTE) AND 
                                            CONCAT(DATE_FORMAT(ms.shift1_start_time,'%Y-%m-%d'),' ',DATE_FORMAT(now(),'%H:%i:%s')) <= DATE_ADD(ms.shift3_start_time, INTERVAL 10 MINUTE) 
                                        THEN 'yes'

                                        ELSE 'no' END AS shift_check
                                    FROM digital_factory_ent_v1.master_shifts ms  '''
                
                data = db_connection.execute(text(query)).mappings().all()
                # data = cursor.fetchall()

                shift_check = ''

                is_shift_end =  False

                for i in data:
                    shift_check = i['shift_check']

                    if shift_check != 'no':
                        # connection.close() 
                        createFolder('Log/',live,f"shift check yes connection closed..!!! ")
                        time.sleep(1200)  
                        createFolder('Log/',live,f"after 20 Min connection opend...!! ")  
                        is_shift_end = True
                        break
                    
                if is_shift_end: 
                    continue

                createFolder('Log/',live," Startted ")

                data  = db_connection.execute(text('SELECT * from digital_factory_ent_v1.master_shifts')).mappings().all()
                # createFolder('Log/',live,"Database Connected") 
                comp_db = 'digital_factory_ent_v1_completed'
                cur_db = 'digital_factory_ent_v1'          
                # data = db_connection.fetchall()

                date_list = [] 

                for i in data:

                    mill_date = i['mill_date']
                    mill_date_str = mill_date.strftime("%Y:%m:%d %H:%M:%S")

                    mill_date_only = mill_date_str[0:10]

                    mill_shift = i['mill_shift']

                    if mill_date.day == 1 and mill_shift == '1':
                        mill_date =  mill_date - timedelta(days=1)

                    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}               
                    month_year = str(mill_month[mill_date.month])+str(mill_date.year) 
                    date_list.append(month_year)              

                date_list = set(date_list)
                date_list = list(date_list)

                try:
                    # createFolder('Log/',live," Execution Startted ")                 
                    query = f'''select db_name , table_name , where_condition  from digital_factory_ent_v1.changed_table where status = 'pp_yes' and db_name = 'process_param_completed'  '''
                    val = db_connection.execute(text(query)).mappings().all()                
                    # val = cursor.fetchall()       

                    for data in val :
                    
                        db_name = data['db_name']+'.'
                        t_name = data['table_name'] 
                        where_condition = data['where_condition']
                        
                       
                        table_name = db_name + t_name 

                        if table_name.startswith('process_param_completed') :

                            for month_year in date_list:
                                month_year_dt= datetime.strptime(month_year, "%m%Y")
                                next_month_year_dt= month_year_dt + relativedelta(months=1)
                                next_month_year_str = next_month_year_dt.strftime("%m%Y")

                                
                                tbl_name1 = table_name.replace('my',month_year)
                                                               

                                tquery=f"""SELECT table_name FROM information_schema.TABLES WHERE 
                                        table_schema="digital_factory_ent_v1_completed" 
                                        AND table_name= 'routecard_{next_month_year_str}' """ 
                                
                                
                                data = db_connection.execute(text(tquery)).mappings().all()

                                if len(data) == 0:
                                    next_month_year_str = ''

                                try:                                     
                                    get_send_data(tbl_name1,next_month_year_str)
                                    rnc_loss_update()

                                except Exception as e:
                                    exc_type, exc_obj, exc_tb = sys.exc_info()
                                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                    createFolder('Log/',live,f"Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")                                       
                
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    createFolder('Log/',live,f"Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")                                       

            except Exception as e:
                # createFolder('Log/',live,f"Cant connect mysql server -->> {e}")  
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                createFolder('Log/',live,f"Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")               
           
            try:
                db_connection.close()
                time.sleep(1800)

            except Exception as e:
                # createFolder('Log/',live,f"Cant connect mysql server -->> {e}")  
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                createFolder('Log/',live,f"Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")               
           
            







# print('completed')
# print(datetime.now())
# id_list = list(df['id'])
# print(len(id_list))
# print(df['id'][0])
# # print(df['item_code'][0],'ite')

# chunk_size = 100
# id_list = list(df['id'])
# #print(id_list)
# print('222222222')
# # if len(id_list) > 30000:
# #     print(22)
# #     batch_size =  300 # Adjust the batch size based on your needs
# id_chunks = [id_list[i:i+chunk_size] for i in range(0, len(id_list), chunk_size)]

# print(len(id_chunks))

#     # for start in range(0, len(id_list), batch_size):
#     #     batch_ids = id_list[start:start + batch_size]
#     #     # Construct the SQL update statement
#     #     s_update = f"UPDATE thiruthani_process_param_012024 SET sync_status = 'ins' WHERE id IN ({batch_ids});"
#     #     s_update = s_update.replace(", );", ");")
#     #     print(s_update)

# list_of_chunk_tuples =[]
# for chunk in id_chunks:

#     for i in range(0,len(chunk)):
#     # Construct the update statement with the chunk of IDs
#     # update_query = f"""
#     #         UPDATE loss_012023
#     #         SET sync_status = 'no'
#     #         WHERE id IN ({','.join('%s' for _ in chunk)});"""
    
#         print(i,'1')
#         update_query = f"""
#             UPDATE ambattur_process_param_450t1_022024 set item_code = '{df['item_code'][i]}'
                
#                 WHERE id = {chunk[i]} """
#         print(update_query)
#         query = text(update_query)
#         db2_connection.execute(query)
#         db2_connection.commit()

# # Write the DataFrame to the "nap" table in db2
# # df.to_sql('thiruthani_process_param_012024', db2, index=False, if_exists='append')


# # id_list = ', '.join([f"'{id_val}'" for id_val in df['id']])
# # # print(id_list)
# # print('222222222')
# # if len(id_list) > 30000:
# #     print(22)
# #     batch_size =  300 # Adjust the batch size based on your needs

# #     for start in range(0, len(id_list), batch_size):
# #         batch_ids = id_list[start:start + batch_size]
# #         # Construct the SQL update statement
# #         s_update = f"UPDATE thiruthani_process_param_012024 SET sync_status = 'ins' WHERE id IN ({batch_ids});"
# #         s_update = s_update.replace(", );", ");")
# #         print(s_update)
# #         query = text(s_update)

# #         # Assuming you want to execute an update query

# #         result = db2_connection.execute(query)
# #         db2_connection.commit()
# #         print(datetime.now())
# # else:
# #         print(1)
# #         s_update = f"UPDATE thiruthani_process_param_012024 SET sync_status = 'ins' WHERE id IN ({id_list});"


# #         query = text(s_update)

# #         # Assuming you want to execute an update query

# #         result = db2_connection.execute(query)
# #         db2_connection.commit()
# #         print(datetime.now())


master_product