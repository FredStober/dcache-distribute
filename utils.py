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

# Get pool infos information
def get_pool_infos(dCacheWebHost):
	import xml.dom.minidom, webservice_api
	# Fixme: Use hostname cmsdcacheweb-kit.gridka.de instead of IP
	data = webservice_api.readURL('%s/info/pools' % dCacheWebHost)
	dom = xml.dom.minidom.parseString(data)
	dom_pools = dom.getElementsByTagName('dCache')[0].getElementsByTagName('pools')[0]
	for dom_pool in dom_pools.getElementsByTagName('pool'):
		result = {}
		result['name'] = str(dom_pool.attributes['name'].value)
		try:
			dom_poolgroups = dom_pool.getElementsByTagName('poolgroups')[0]
			for pg in dom_poolgroups.getElementsByTagName('poolgroupref'):
				result.setdefault('poolgroups', []).append(str(pg.attributes['name'].value))

			dom_space = dom_pool.getElementsByTagName('space')[0]
			for sm in dom_space.getElementsByTagName('metric'):
				result.setdefault('space', {})[str(sm.attributes['name'].value)] = float(sm.childNodes[0].data)
		except:
			print 'Unable to parse pool info', result['name']
		yield result

# Translate fn to dataset directory name - go to first non-numeric parent directory
def fn2ddn(fn):
	result = fn.rsplit('/', 1)[0]
	while True:
		try:
			tmp = result.rsplit('/', 1)
			int(tmp[1])
			result = tmp[0]
		except:
			break
	return result

# Filter chimera dump for moveable files
def filterMoveable(chimera_iter):
	for entry in chimera_iter:
		if dCacheInfo.dcache_id not in entry:
			continue
		if not entry.get(dCacheInfo.size, 0):
			continue
		if not entry.get(dCacheInfo.location, []):
			continue
		yield entry
