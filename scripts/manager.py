#!/usr/biin/python
import sys
import subprocess as sp
import time
import shlex
import os
import signal
import shutil
import glob

storm_home = '/share/hadoop/jkarimov/workDir/systems/apache-storm-1.0.2/'
spark_home = '/share/hadoop/jkarimov/workDir/systems/spark-2.0.0-bin-hadoop2.7//'
flink_home = '/share/hadoop/jkarimov/workDir/systems/flink-1.1.2/'
project_dir = '/share/hadoop/jkarimov/workDir/StreamBenchmarks/'
conf_file = '/share/hadoop/jkarimov/workDir/StreamBenchmarks/conf/benchmarkConf.yaml'
zk_home = '/share/hadoop/jkarimov/workDir/systems/zookeeper-3.4.9/'
storm_home = '/share/hadoop/jkarimov/workDir/systems/apache-storm-1.0.2/'
slaves = '/share/hadoop/jkarimov/workDir/StreamBenchmarks/scripts/slaves'
master = '/share/hadoop/jkarimov/workDir/StreamBenchmarks/scripts/master'
spark_output_dir = ''
flink_output_dir = ''
storm_output_dir = ''
kafka_output_dir = ''
conf_file = '/share/hadoop/jkarimov/workDir/StreamBenchmarks/conf/benchmarkConf.yaml'
datagenerator_hosts = [ ]
datagenerator_port = 0

def start_flink():
	sp.call([flink_home + "bin/start-cluster.sh"])

def stop_flink():
	sp.call(flink_home + "bin/stop-cluster.sh")

def flink_benchmark():
	clear_dir(project_dir + "output/flink/")
	sp.call([flink_home + "bin/flink" , "run", project_dir + "flink-benchmarks/target/flink-benchmarks-0.1.0.jar", "--confPath",conf_file])	

def start_spark():
	sp.call([spark_home + "sbin/start-all.sh"])
def stop_spark():
	sp.call([ spark_home + "sbin/stop-all.sh" ])

def clear_dir(path):
	filelist = [ f for f in os.listdir(path) ]
	for f in filelist:
		try:
			shutil.rmtree(path+f )
		except:
			os.remove(path+f)

def spark_benchmark():
	clear_dir(project_dir + "output/spark/")
	sp.call([spark_home + 'bin/spark-submit' ,'--class' ,'spark.benchmark.SparkBenchmark', project_dir + 'spark-benchmarks/target/spark-benchmarks-0.1.0.jar' , conf_file])

def start_zookeeper():
	sp.call([zk_home + 'bin/zkServer.sh', 'start'])	
	time.sleep(2)
def stop_zookeeper():
        sp.call([zk_home + 'bin/zkServer.sh', 'stop'])

def start_storm():
	start_zookeeper()
	sp.Popen([storm_home + 'bin/storm' ,'nimbus' ], stdout=sp.PIPE,stderr=sp.STDOUT)
	start_supervisor_node = '\'' +   storm_home + 'bin/storm supervisor\'' 
#	print start_supervisor_node
	time.sleep(5)
	print('nimbus started')
	with open(slaves) as f:
		for host in f:
			#print host
			sp.Popen (["ssh", host.strip() ,  '\'' + start_supervisor_node + '\''  ],stdout=sp.PIPE,stderr=sp.STDOUT) 
			time.sleep(5)
        		print('supervisor ' + host + ' started')
	print('storm started')
		

def stop_process(name,host):
	host  = host.strip()
	sshProcess = sp.Popen(['ssh', host], stdin=sp.PIPE, stdout = sp.PIPE, universal_newlines=True,bufsize=0)
	sshProcess.stdin.write("ps -aux | grep " + name + "  \n")
	sshProcess.stdin.close()
	for line in sshProcess.stdout:
    		if line == "END\n":
        		break
		if (name in line):
			line_list = line.split(None)
			if(len(line_list) > 3 and line_list[1].isdigit()):
				pid = int(line_list[1])
    				print('process with id ' + `pid` + ' found')
				try:
					os.kill(pid, -9)
					print('killed')
					time.sleep(1)
				except :
					print "Oops! "

def stop_storm():
	stop_zookeeper()
	stop_process_all('storm')

def stop_process_all(name):
	print name
	with open(slaves) as f:
		for host in f:
			stop_process(name,host)
	with open(master) as f:
		for host in f:
			stop_process(name,host)

def storm_benchmark():
	clear_dir(project_dir + "output/trident/")
	sp.call([storm_home + "bin/storm", "jar", project_dir + 'storm-benchmarks/target/storm-benchmarks-0.1.0.jar' , 'trident.benchmark.TridentBenchmark', conf_file,'cluster','topologyName'])

def merge_output_files():
	print spark_output_dir
	sp.call(["cat",spark_output_dir + '*/*', '>','spark.csv'])

def concat_files_in_dir(input_dir, output_dir):
	read_files = glob.glob(input_dir )

	with open(output_dir, "wb") as outfile:
    		for f in read_files:
        		with open(f, "rb") as infile:
            			outfile.write(infile.read())


def parse_conf_file():
	with open(conf_file) as f:
		content = f.readlines()
	for index,line in enumerate(content):
		if 'flink.output' in line:
			global flink_output_dir
			flink_output_dir =  line.split(None)[1].replace("\"","")
		elif 'spark.output' in line:
			global spark_output_dir
			spark_output_dir =  line.split(None)[1].replace("\"","")
		elif 'trident.output' in line:
			global storm_output_dir
			storm_output_dir =  line.split(None)[1].replace("\"","")
		elif 'kafka.output' in line:
			global kafka_output_dir
			kafka_output_dir =  line.split(None)[1].replace("\"","")
		elif 'datasourcesocket.port' in line:
			global datagenerator_port
			datagenerator_port = line.split(None)[1].replace("\"","")
		elif 'datasourcesocket.hosts' in line:
			global datagenerator_hosts
			tempInd = index + 1
			while '-' in content[tempInd]:
				datagenerator_hosts.append( content[tempInd].split(None)[1].replace("\"",""))
				tempInd = tempInd + 1


def start_data_generators():
	data_generator_instance_script = '\'' + 'java -cp ' + project_dir + 'data-generator/target/data-generator-0.1.0.jar data.source.socket.DataGenerator ' + project_dir + 'conf/benchmarkConf.yaml' + '\''
	
	global datagenerator_hosts
	print (data_generator_instance_script)
	for host in datagenerator_hosts:
		print(host)
		sp.Popen (["ssh", host.strip() ,  '\'' + data_generator_instance_script + '\''  ],stdout=sp.PIPE,stderr=sp.STDOUT)
		time.sleep(2)
		print('datagenerator ' + host.strip() + ' started')
	print('data generators started')


def start_data_generator():
	sp.call(['java','-cp', project_dir + 'data-generator/target/data-generator-0.1.0.jar', 'data.source.socket.DataGenerator', project_dir + 'conf/benchmarkConf.yaml' ])



parse_conf_file()
print(datagenerator_hosts)
if(len(sys.argv[1:]) == 1):
	arg = sys.argv[1]
	if (arg == "start-flink"):
		start_flink()
	elif(arg == "stop-flink"):
		stop_flink()
	elif(arg == "flink-benchmark"):
		flink_benchmark()
	elif(arg == "start-spark" ):
		start_spark()
	elif(arg == "stop-spark"):
		stop_spark()
	elif(arg == "spark-benchmark"):
		spark_benchmark()
	elif(arg == "start-zookeeper"):
		start_zookeeper()
	elif(arg == "stop-zookeeper"):
		stop_zookeeper()
	elif(arg == "start-storm"):
		start_storm()
	elif(arg == "stop-storm"):
		stop_storm()
	elif(arg == "storm-benchmark"):
		storm_benchmark()
	elif(arg == "concat-spark"):
		concat_files_in_dir('/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/spark/*/*', '/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/results/tempfile.csv')
	elif(arg == "concat-trident"):
		 concat_files_in_dir('/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/trident/*','/share/hadoop/jkarimov/workDir/StreamBenchmarks/output/results/tridentFile.csv')
	elif(arg == "start-datagenerators"):
		start_data_generators()
	elif(arg == "start-datagenerator"):
		start_data_generator()
elif(len(sys.argv[1:]) == 2):
	arg1 = sys.argv[1]	
	arg2 = sys.argv[2]
	if(arg1 == "stop-process-all"):
		stop_process_all(arg2)	
