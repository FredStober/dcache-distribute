def expath(fn):
	return os.path.realpath(os.path.expanduser(fn))

def progress(iter, step = 1000, enable = True, speed = True):
	if enable:
		t_start = time.time()
		try:
			idx = 0
			for idx, entry in enumerate(iter):
				if idx % step == 0:
					sys.stdout.write('\r%d' % idx)
					if speed:
						delta_t = float(time.time() - t_start)
						if delta_t > 2:
							sys.stdout.write(' %d/s' % (idx / delta_t))
					sys.stdout.flush()
				yield entry
			sys.stdout.write('\n')
			sys.stdout.flush()
		except:
			sys.stdout.write('\nError at %d\n' % idx)
			sys.stdout.flush()
			raise
	else:
		for entry in iter:
			yield entry


def get_cached(cache_file, fun, *args, **kwargs):
	import json
	if not os.path.exists(cache_file):
		result = fun(*args, **kwargs)
		json.dump(result, open(cache_file, 'w'))
	return json.load(open(cache_file))
