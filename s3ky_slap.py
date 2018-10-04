import sys
from threading import Thread
import time
import traceback
import logging

from s3kv import S3KV

driver = S3KV('127.0.0.1', 2379)

trace = []
bucket_key = 'demo.cs795.ao'

trace_dir = 'cs795_traces'

def parse_trace(trace_file, val_len):
	trace_f_path = trace_dir + '/' + trace_file
	with open(trace_f_path) as f:
		lines = f.readlines()
		for line in lines:
			item = line.strip().split()
			op = item[0]
			key = item[2]
			val = ''
			if op == 'READ':
				val = ''
			elif op == 'INSERT' or op == 'UPDATE':
				val = line[-(val_len+3):-3]
			trace.append({'op':op, 'key':key, 'val':val})
			#print("op={0} key={1} val={2}".format(op, key, val))

def slap(tid, start, end):
	print("tid{0}: range:{1}-{2}: sz:{3}".format(tid, start, end, len(trace[start:end])))
	for item in trace[start : end]:
		op = item['op']
		key = item['key']
		if op == 'INSERT':
			val = item['val']
			print("{0}: key={1}, val={2}".format(op, key, val))
			driver.s3kv_put(bucket_key, key, val)
		elif op == 'READ':
			print("{0}: key={1}".format(op, key))
			driver.s3kv_get(bucket_key, key)
		elif op == 'UPDATE':
			val = item['val']
			print("{0}: key={1}, val={2}".format(op, key, val))
			driver.s3kv_put(bucket_key, key, val)
		else:
			print("Op unknown: {0}".format(op))

class slapWorker(Thread):
	def __init__(self, tid, start, end):
		Thread.__init__(self)
		self.tid = tid
		self.start = start
		self.end = end

	def run(self):
		#print("tid{0}: range:{1}-{2}: sz:{3}".format(self.tid, self.start, self.end, len(trace[start:end])))
		for item in trace[self.start : self.end]:
			op = item['op']
			key = item['key']
			if op == 'INSERT':
				val = item['val']
				print("{0}: key={1}, val={2}".format(op, key, val))
				driver.s3kv_put(bucket_key, key, val)
			elif op == 'READ':
				print("{0}: key={1}".format(op, key))
				driver.s3kv_get(bucket_key, key)
			elif op == 'UPDATE':
				val = item['val']
				print("{0}: key={1}, val={2}".format(op, key, val))
				driver.s3kv_put(bucket_key, key, val)
			else:
				print("Op unknown: {0}".format(op))

###
# sample implementation of a toy S3KV benchmark
###
num_threads = int(sys.argv[1])
trace_file = sys.argv[2]
val_len = int(sys.argv[3])

parse_trace(trace_file, val_len)

num_items = len(trace)
print("num_items={0}".format(num_items))

workers = []

for i in range(num_threads):
	start = i * (num_items/num_threads)
	end = start + num_items/num_threads
	if i == num_threads - 1:
		end = num_items
	t = Thread(target=slap, args=(i, start, end))
	workers.append(t)
print("Done with creating slapWorkers...")

for t in workers:
	t.start()
print("Done with starting slapWorkers...")

for t in workers:
	t.join()
print("Existing main thread...")
