from ftplib import FTP_TLS
import os
import csv
import json
import boto3

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):

	### RETRIEVE FILE FROM TAW ###
	# Set up connection to FTP server
	ftps = FTP_TLS()
	ftps.connect('ftp.tapww.com', 22, timeout=120)
	ftps.set_debuglevel(1)
	ftps.set_pasv(True)
	ftps.login(os.environ['taw_user'], os.environ['taw_pass'])
	ftps.prot_p()

	# Create local file to store contents of Inventory file
	f = open('/tmp/inv.txt', 'wb')

	# Retrieve the Inventory file contents and store locally
	ftps.retrbinary('RETR Inventory.txt', f.write)

	# Close local file
	f.close()

	# Close the connection
	ftps.quit()
	### END RETRIEVE FILE FROM TAW ###


	### READ INVENTORY FILE ###

	item_list = []
	with open('/tmp/inv.txt', newline='') as items:
		reader = csv.reader(items, delimiter='\t')
		item_list = [[item[2][:3] + '-' + item[2][3:], item[12]] for item in reader if item[2][:3] in ['BIL', 'EXP', 'FOX', 'RAN']]

	print(f"Items found: {len(item_list)}")

	# Divide list into chunks for threading
	chunks = []
	for i in range(0, len(item_list), 200):
		chunks.append(item_list[i:i+200])

	### END READ INVENTORY FILE ###

	numChunks = len(chunks)

	print(f"Number of chunks: {numChunks}")

	### INVOKE LAMBDA ONCE FOR EACH CHUNK ###

	i = 1

	for eachChunk in chunks:
		print(f"Invoking lambda for chunk {i} of {numChunks}")
		lambda_client.invoke(FunctionName='updateInventory', InvocationType='Event', Payload=json.dumps({'chunk' : eachChunk, 'chunkNum' : i}))
		i = i + 1

	### END INVOKE LAMBDA ONCE FOR EACH CHUNK ###

	return {
		'statusCode': 202,
		'headers' : {
			'Access-Control-Allow-Origin' : '*'
		},
		'body': json.dumps('Function invoked successfully! Processing...')
	}
