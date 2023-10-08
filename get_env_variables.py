import os
os.environ['env']='Dev'
env = os.environ['env']

appName = 'master data'

src_olap = os.getcwd()+'\source\olap'

src_oltp = os.getcwd()+'\source\oltp'

city_path = os.getcwd()+'\output\city'