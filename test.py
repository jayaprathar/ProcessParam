# print(datetime.now())
# # Create SQLAlchemy engine for the source database (db1)
# db1 = create_engine("mysql+pymysql://root:@localhost:3306/process_param_completed")
# db1_connection = db1.connect()

# # Create SQLAlchemy engine for the destination database (db2)
# db2 = sqlalchemy.create_engine("mysql+pymysql://root:@localhost:3306/process_param_completed")

# db2_connection = db2.connect()
# # Complete the SQL query to select all rows from the "customers1" table
# query = '''

# SELECT pp.id,pp._TIMESTAMP,l.bt,l.et,l.current_stop_code ,ifnull(mlc.sub_loss_id,0) sub_loss ,ifnull(mp.ROUTE_CARD_NO,0) as route,ifnull(mp.item_code,0) as item_code
	
# FROM 
# 	(SELECT id,_NAME,_NUMERICID,_VALUE,_TIMESTAMP,_QUALITY,sync_status, equipment_id FROM process_param_completed.ambattur_process_param_450t1_022024 ) pp 
# 	INNER JOIN 
# 	(SELECT 
# 		lo.machine_id,
# 		lo.product_ref_id,
# 		lo.current_stop_code,
# 		lo.current_stop_begin_time bt,
# 		DATE_ADD(lo.current_stop_begin_time, INTERVAL lo.current_stop_duration SECOND) et
		
# 	FROM
# 		digital_factory_ent_v1_completed.loss_022024 lo

# 		) AS l
# 	ON l.machine_id = pp.equipment_id AND pp._TIMESTAMP >= l.bt AND pp._TIMESTAMP < l.et
# 	LEFT JOIN digital_factory_ent_v1.master_loss_code mlc ON mlc.loss_id = l.current_stop_code

#         LEFT JOIN digital_factory_ent_v1_completed.routecard_022024 cr ON cr.product_ref_id = l.product_ref_id
#         LEFT JOIN digital_factory_ent_v1.master_product mp ON mp.product_unique_id = cr.product_unique_id
#           '''

# # Read data from db1 using Pandas
# # df = pd.read_sql(query, db1)
# all_records = db2_connection.execute(text(query)).mappings().all()

# print('completed')
# print(result)
# Assuming you have a NumPy array of records
# all_records = np.array(result)

# Define the batch size
# batch_size = 1000

# # Calculate the total number of batches
# total_batches = (len(all_records) + batch_size - 1) // batch_size

# # Process records in batches using a for loop
# for batch_index in range(total_batches):
#     start_index = batch_index * batch_size
#     end_index = (batch_index + 1) * batch_size
#     current_batch = all_records[start_index:end_index]

#     # Perform operations on the current batch
#     for record in current_batch:
#         # Your processing logic here
#         update_query = f"""
#             UPDATE ambattur_process_param_450t1_022024 set item_code = '{record['item_code']}' ,sub_loss_id = '{record['sub_loss']}' , route_card_no = '{record['route']}'
                
#                 WHERE id = {record['id']} """
#         # print(update_query)
#         query = text(update_query)
#         db2_connection.execute(query)
#         db2_connection.commit()

# print(datetime.now())